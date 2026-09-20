"""Rebuild the complete Silver transaction dataset from immutable Bronze.

Identical replays collapse; conflicting versions of an ID fail before writing.
Full replacement makes successful sequential reruns idempotent. Plain Parquet
replacement is not atomic: serialize writers and rerun after a failed write.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, month, dayofmonth, trim
from pyspark.sql.types import DoubleType, TimestampType
from dotenv import load_dotenv

load_dotenv()

def get_spark_session():
    # Nécessite les packages hadoop-aws pour lire/écrire sur s3a://
    return SparkSession.builder \
        .appName("bronze_to_silver_transactions") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "ledgerlake")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "changeme")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

TRANSACTION_COLUMNS = [
    "transaction_id", "customer_id", "amount", "currency", "type",
    "merchant", "country", "timestamp",
]


def transform_transactions(df):
    """Reject ambiguous IDs instead of picking an arbitrary financial record."""
    missing = set(TRANSACTION_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing transaction columns: {sorted(missing)}")
    # Exclude ingestion partition columns: replay may arrive on a different day.
    records = df.select(*TRANSACTION_COLUMNS)
    if not records.filter(
        col("transaction_id").isNull() | (trim(col("transaction_id")) == "")
    ).isEmpty():
        raise ValueError("Missing or empty transaction_id in Bronze")
    unique_records = records.dropDuplicates(TRANSACTION_COLUMNS)
    conflicts = unique_records.groupBy("transaction_id").count().filter(col("count") > 1)
    if not conflicts.isEmpty():
        raise ValueError("Conflicting records for the same transaction_id; Silver unchanged")
    # Preserve existing acceptance rules. Expanded validation/quarantine is a
    # separate milestone. Check conflicts first, even if one version is invalid.
    df_typed = unique_records \
        .withColumn("amount", col("amount").cast(DoubleType())) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    df_typed = df_typed.filter(col("amount") > 0).filter(col("customer_id").isNotNull())

    # 4. Standardisation du pays
    df_std = df_typed.withColumn(
        "country",
        when(col("country").isin("MA", "Maroc"), "Morocco")
        .when(col("country").isin("FR"), "France")
        .when(col("country").isin("US"), "USA")
        .otherwise(col("country"))
    )
    
    # 5. Enrichissement (country_region & extract date for partitions)
    df_enriched = df_std.withColumn(
        "country_region",
        when(col("country") == "Morocco", "Africa")
        .when(col("country") == "France", "Europe")
        .when(col("country") == "USA", "North America")
        .otherwise("Unknown")
    ).withColumn("year", year("timestamp")) \
     .withColumn("month", month("timestamp")) \
     .withColumn("day", dayofmonth("timestamp"))

    return df_enriched


def rebuild_transactions(spark, bronze_path, silver_path):
    """Read ALL retained Bronze history, validate, then replace ALL Silver.

    Callers must supply the complete input, not a date slice. No reads from the
    output path are used, so historical duplicates are repaired on the first run.
    """
    if bronze_path.rstrip('/') == silver_path.rstrip('/'):
        raise ValueError("Bronze input and Silver output must be different")
    # FAILFAST prevents malformed JSON from silently disappearing during rebuild.
    raw = spark.read.option("mode", "FAILFAST").json(bronze_path)
    if raw.isEmpty():
        raise ValueError("Empty Bronze input; refusing to replace Silver")
    clean = transform_transactions(raw).persist()
    try:
        # Materialize before touching output so parsing/cast errors fail early.
        count = clean.count()
        if count == 0:
            raise ValueError("No accepted transactions; refusing to replace Silver")
        (clean.write.mode("overwrite")
            .option("partitionOverwriteMode", "static")
            .partitionBy("year", "month", "day").parquet(silver_path))
        return count
    finally:
        clean.unpersist()


def process_transactions():
    spark = get_spark_session()
    try:
        count = rebuild_transactions(
            spark,
            "s3a://bronze/transactions/*/*/*/*.json",
            "s3a://silver/transactions/",
        )
        print(f"Rebuilt Silver transactions: {count} rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_transactions()
