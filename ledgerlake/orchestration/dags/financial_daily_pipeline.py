"""
financial_daily_pipeline.py — DAG Airflow principal (Palier E.1)

DAG avec les tâches suivantes, dans l'ordre :
1. run_bronze_writer - consomme Kafka et écrit en Bronze (batch mode)
2. extract_and_validate_bronze - vérifie que les nouveaux fichiers Bronze du jour sont bien arrivés
3. quality_check_bronze - exécute le checkpoint Great Expectations "bronze_to_silver"
4. spark_bronze_to_silver_transactions/customers/loans - déclenche les 3 jobs Spark en parallèle
5. quality_check_silver - checkpoint Great Expectations "silver_to_gold"
6. dbt_run_gold - BashOperator qui lance `dbt run`
7. dbt_test_gold - BashOperator qui lance `dbt test`
8. notify_completion - log de succès/échec
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SPARK_JOBS_DIR = PROJECT_ROOT / "spark_jobs"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
DATA_QUALITY_DIR = PROJECT_ROOT / "data_quality" / "great_expectations"
INGESTION_DIR = PROJECT_ROOT / "ingestion"

# Add ingestion to path for bronze_writer import
sys.path.insert(0, str(INGESTION_DIR))

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="financial_daily_pipeline",
    description="Pipeline quotidien LedgerLake : Kafka → Bronze → Silver → Gold",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ledgerlake", "financial", "daily"],
) as dag:

    def run_bronze_writer(**context):
        """Run bronze writer in batch mode to consume Kafka and write to Bronze."""
        import os
        from bronze_writer import run_batch
        max_messages = int(os.getenv("BRONZE_WRITER_MAX_MESSAGES", "1000"))
        max_seconds = float(os.getenv("BRONZE_WRITER_MAX_SECONDS", "60"))
        count = run_batch(max_messages=max_messages, max_seconds=max_seconds)
        print(f"Bronze writer processed {count} messages")
        return count

    run_bronze_writer_task = PythonOperator(
        task_id="run_bronze_writer",
        python_callable=run_bronze_writer,
    )

    def check_bronze_files(**context):
        """Vérifie la présence de nouveaux fichiers Bronze pour la date d'exécution."""
        import boto3
        from botocore.exceptions import ClientError
        import os
        from dotenv import load_dotenv

        load_dotenv()
        execution_date = context["data_interval_start"].in_timezone("UTC")
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")

        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000"),
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "ledgerlake"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "changeme"),
            region_name="us-east-1",
        )

        bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
        topics = ["transactions", "customers", "loans"]
        found_any = False

        for topic in topics:
            prefix = f"{topic}/year={year}/month={month}/day={day}/"
            try:
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
                if "Contents" in response:
                    found_any = True
                    print(f"Found Bronze files for {topic}: {prefix}")
            except ClientError as e:
                print(f"Error checking {topic}: {e}")

        if not found_any:
            raise ValueError(f"No Bronze files found for {year}-{month}-{day}")
        return True

    extract_and_validate_bronze = PythonOperator(
        task_id="extract_and_validate_bronze",
        python_callable=check_bronze_files,
    )

    def run_quality_check(stage: str, entity: str, path: str, format: str):
        """Execute Great Expectations validation."""
        import subprocess
        import sys

        cmd = [
            sys.executable,
            str(DATA_QUALITY_DIR / "quality_checks.py"),
            "--stage", stage,
            "--entity", entity,
            "--path", path,
            "--format", format,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
        if result.returncode != 0:
            print(f"STDOUT:\n{result.stdout}")
            print(f"STDERR:\n{result.stderr}")
            raise RuntimeError(f"Great Expectations failed for {stage}/{entity}")
        print(result.stdout)

    def run_quality_checks_bronze(**context):
        """Run quality checks for all bronze entities."""
        entities = [
            ("transactions", "s3a://bronze/transactions/*/*/*/*.json", "json"),
            ("customers", "s3a://bronze/customers/*/*/*/*.json", "json"),
            ("loans", "s3a://bronze/loans/*/*/*/*.json", "json"),
        ]
        for entity, path, fmt in entities:
            run_quality_check("bronze_to_silver", entity, path, fmt)

    quality_check_bronze = PythonOperator(
        task_id="quality_check_bronze",
        python_callable=run_quality_checks_bronze,
    )

    with TaskGroup("spark_bronze_to_silver") as spark_bronze_to_silver:
        spark_bronze_to_silver_transactions = BashOperator(
            task_id="spark_bronze_to_silver_transactions",
            bash_command=f"cd {SPARK_JOBS_DIR} && python bronze_to_silver_transactions.py",
        )
        spark_bronze_to_silver_customers = BashOperator(
            task_id="spark_bronze_to_silver_customers",
            bash_command=f"cd {SPARK_JOBS_DIR} && python bronze_to_silver_customers.py",
        )
        spark_bronze_to_silver_loans = BashOperator(
            task_id="spark_bronze_to_silver_loans",
            bash_command=f"cd {SPARK_JOBS_DIR} && python bronze_to_silver_loans.py",
        )

    def run_quality_checks_silver(**context):
        """Run quality checks for all silver entities."""
        entities = [
            ("transactions", "s3a://silver/transactions/", "parquet"),
            ("customers", "s3a://silver/customers/", "parquet"),
            ("loans", "s3a://silver/loans/", "parquet"),
        ]
        for entity, path, fmt in entities:
            run_quality_check("silver_to_gold", entity, path, fmt)

    quality_check_silver = PythonOperator(
        task_id="quality_check_silver",
        python_callable=run_quality_checks_silver,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
    )

    def notify_completion(**context):
        """Log final status."""
        ti = context["ti"]
        dag_run = context["dag_run"]
        status = "SUCCESS" if dag_run.state == "success" else "FAILED"
        print(f"=== Pipeline {status} ===")
        print(f"DAG: {dag_run.dag_id}")
        print(f"Run ID: {dag_run.run_id}")
        print(f"Execution Date: {context['data_interval_start']}")
        if status == "FAILED":
            failed_tasks = [t for t in dag_run.get_task_instances() if t.state == "failed"]
            for t in failed_tasks:
                print(f"  Failed task: {t.task_id}")

    notify_completion = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
        trigger_rule="all_done",
    )

    run_bronze_writer_task >> extract_and_validate_bronze >> quality_check_bronze >> spark_bronze_to_silver >> quality_check_silver >> dbt_run_gold >> dbt_test_gold >> notify_completion
