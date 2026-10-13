"""
End-to-end integration test for LedgerLake pipeline.

Runs the complete pipeline:
1. Produce events to Kafka
2. Bronze writer consumes and writes to MinIO
3. Spark jobs process Bronze -> Silver
4. dbt models build Gold layer
5. Validate Gold tables have expected data

Run with: python -m pytest ledgerlake/tests/integration/test_e2e_pipeline.py -v -s
"""

import os
import sys
import time
import json
import subprocess
from pathlib import Path
from decimal import Decimal

import pytest
import boto3
from botocore.exceptions import ClientError

# Add project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "ingestion"))
sys.path.insert(0, str(PROJECT_ROOT / "spark_jobs"))
sys.path.insert(0, str(PROJECT_ROOT / "dbt_project"))

# Test configuration
TEST_DATE = "2026-01-15"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ROOT_USER", "ledgerlake")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "changeme")
BRONZE_BUCKET = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
SILVER_BUCKET = os.getenv("MINIO_BUCKET_SILVER", "silver")


@pytest.fixture(scope="session")
def minio_client():
    """MinIO client for verification."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )


@pytest.fixture(scope="session")
def test_env():
    """Environment variables for test run."""
    env = os.environ.copy()
    env.update({
        "KAFKA_BOOTSTRAP_SERVERS_HOST": KAFKA_BOOTSTRAP,
        "MINIO_ENDPOINT_HOST": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ACCESS,
        "MINIO_ROOT_PASSWORD": MINIO_SECRET,
        "MINIO_BUCKET_BRONZE": BRONZE_BUCKET,
        "MINIO_BUCKET_SILVER": SILVER_BUCKET,
        "EVENT_COUNT": "100",
        "CUSTOMER_POPULATION": "50",
        "EVENT_SEED": "20260115",
        "BASE_EVENT_TIME": f"{TEST_DATE}T00:00:00Z",
        "EVENT_SCENARIO": "valid",
        "BRONZE_WRITER_MAX_MESSAGES": "500",
        "BRONZE_WRITER_MAX_SECONDS": "30",
        "DUCKDB_PATH": "/tmp/ledgerlake_test.duckdb",
    })
    return env


def run_command(cmd, env, cwd=None, timeout=300):
    """Run a command and return result."""
    print(f"Running: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    result = subprocess.run(
        cmd if isinstance(cmd, list) else cmd.split(),
        env=env,
        cwd=cwd or PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        print(f"STDOUT:\n{result.stdout}")
        print(f"STDERR:\n{result.stderr}")
    return result


class TestE2EPipeline:
    """End-to-end pipeline tests."""

    def test_01_produce_events(self, test_env):
        """Step 1: Produce events to Kafka."""
        producers = [
            ("producers.transaction_producer", "TRANSACTION_PRODUCER_COUNT"),
            ("producers.customer_producer", "CUSTOMER_PRODUCER_COUNT"),
            ("producers.loan_producer", "LOAN_PRODUCER_COUNT"),
        ]
        
        for module_name, count_env in producers:
            env = test_env.copy()
            env[count_env] = "50"  # Small batch for testing
            
            result = run_command([
                sys.executable, "-m", module_name,
                "--count", "50",
                "--seed", "20260115",
                "--customers", "50",
                "--base-time", f"{TEST_DATE}T00:00:00Z",
                "--scenario", "valid",
            ], env=env)
            
            assert result.returncode == 0, f"Producer {module_name} failed"
            assert "Published" in result.stdout
            print(f"✓ {module_name}: {result.stdout.strip()}")

    def test_02_bronze_writer(self, test_env):
        """Step 2: Bronze writer consumes Kafka and writes to MinIO."""
        from bronze_writer import run_batch
        
        count = run_batch(max_messages=500, max_seconds=30)
        assert count > 0, "No messages processed by bronze writer"
        print(f"✓ Bronze writer processed {count} messages")

    def test_03_bronze_files_exist(self, minio_client):
        """Step 3: Verify Bronze files exist in MinIO."""
        year, month, day = TEST_DATE.split("-")
        topics = ["transactions", "customers", "loans"]
        
        for topic in topics:
            prefix = f"{topic}/year={year}/month={month}/day={day}/"
            response = minio_client.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
            assert "Contents" in response, f"No Bronze files for {topic} on {TEST_DATE}"
            print(f"✓ Bronze/{prefix}: {len(response['Contents'])} file(s)")

    def test_04_spark_silver_transactions(self, test_env):
        """Step 4: Spark job - transactions Bronze -> Silver."""
        import bronze_to_silver_transactions as job
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("test_silver_transactions") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.12:3.2.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        try:
            metrics = job.rebuild_transactions(
                spark,
                f"s3a://{BRONZE_BUCKET}/transactions/*/*/*/*.json",
                f"s3a://{SILVER_BUCKET}/transactions/",
                f"s3a://{SILVER_BUCKET}-rejected/transactions/",
            )
            assert metrics["accepted"] > 0, "No accepted transactions"
            assert metrics["accepted"] == metrics["input"] - metrics["exact_duplicates"] - metrics["rejected"]
            print(f"✓ Silver transactions: {metrics}")
        finally:
            spark.stop()

    def test_05_spark_silver_customers(self, test_env):
        """Step 5: Spark job - customers Bronze -> Silver."""
        import bronze_to_silver_customers as job
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("test_silver_customers") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.12:3.2.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.session.TimeZone", "UTC") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        try:
            metrics = job.rebuild_customers(
                spark,
                f"s3a://{BRONZE_BUCKET}/customers/*/*/*/*.json",
                f"s3a://{SILVER_BUCKET}/customers/",
                f"s3a://{SILVER_BUCKET}-rejected/customers/",
            )
            assert metrics["accepted"] > 0, "No accepted customers"
            print(f"✓ Silver customers: {metrics}")
        finally:
            spark.stop()

    def test_06_spark_silver_loans(self, test_env):
        """Step 6: Spark job - loans Bronze -> Silver."""
        import bronze_to_silver_loans as job
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("test_silver_loans") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.12:3.2.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        try:
            metrics = job.rebuild_loans(
                spark,
                f"s3a://{BRONZE_BUCKET}/loans/*/*/*/*.json",
                f"s3a://{SILVER_BUCKET}/loans/",
                f"s3a://{SILVER_BUCKET}-rejected/loans/",
            )
            assert metrics["accepted"] > 0, "No accepted loans"
            print(f"✓ Silver loans: {metrics}")
        finally:
            spark.stop()

    def test_07_dbt_run_gold(self, test_env):
        """Step 7: dbt run builds Gold layer."""
        env = test_env.copy()
        env["DBT_PROFILES_DIR"] = str(PROJECT_ROOT / "dbt_project")
        
        result = run_command([
            "dbt", "run", "--profiles-dir", str(PROJECT_ROOT / "dbt_project")
        ], env=env, cwd=str(PROJECT_ROOT / "dbt_project"), timeout=180)
        
        assert result.returncode == 0, "dbt run failed"
        print("✓ dbt run completed")

    def test_08_dbt_test_gold(self, test_env):
        """Step 8: dbt test validates Gold layer."""
        env = test_env.copy()
        env["DBT_PROFILES_DIR"] = str(PROJECT_ROOT / "dbt_project")
        
        result = run_command([
            "dbt", "test", "--profiles-dir", str(PROJECT_ROOT / "dbt_project")
        ], env=env, cwd=str(PROJECT_ROOT / "dbt_project"), timeout=180)
        
        assert result.returncode == 0, "dbt test failed"
        print("✓ dbt test completed")

    def test_09_gold_tables_have_data(self, test_env):
        """Step 9: Verify Gold tables have expected data."""
        import duckdb
        
        db_path = test_env.get("DUCKDB_PATH", "/tmp/ledgerlake_test.duckdb")
        conn = duckdb.connect(db_path, read_only=True)
        
        tables = [
            "customer_360",
            "customer_monthly_spending", 
            "fraud_detection_metrics",
            "loan_portfolio_analysis",
        ]
        
        for table in tables:
            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            count = result[0]
            assert count > 0, f"Gold table {table} is empty"
            print(f"✓ Gold.{table}: {count} rows")
        
        # Verify customer_360 has expected columns
        cols = conn.execute("DESCRIBE customer_360").fetchall()
        col_names = [c[0] for c in cols]
        expected_cols = ["customer_id", "age", "country", "total_transactions", 
                        "average_transaction", "credit_score", "risk_level"]
        for ec in expected_cols:
            assert ec in col_names, f"Missing column {ec} in customer_360"
        print("✓ Gold table schemas validated")
        
        conn.close()

    def test_10_silver_files_exist(self, minio_client):
        """Step 10: Verify Silver files exist in MinIO."""
        year, month, day = TEST_DATE.split("-")
        topics = ["transactions", "customers", "loans"]
        
        for topic in topics:
            prefix = f"{topic}/year={year}/month={month}/day={day}/"
            response = minio_client.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
            assert "Contents" in response, f"No Silver files for {topic} on {TEST_DATE}"
            # Should be Delta Lake format
            print(f"✓ Silver/{prefix}: {len(response['Contents'])} file(s)")


if __name__ == "__main__":
    # Allow running directly
    pytest.main([__file__, "-v", "-s", "--tb=short"])