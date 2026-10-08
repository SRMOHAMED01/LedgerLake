"""Build accepted and quarantined Silver transactions from complete Bronze history."""

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofmonth,
    lit,
    month,
    trim,
    when,
    year,
)
from pyspark.sql.types import DecimalType, IntegerType, TimestampType


load_dotenv()

TRANSACTION_COLUMNS = [
    "event_id", "event_type", "schema_version", "event_timestamp", "producer",
    "transaction_id", "customer_id", "amount", "currency", "type",
    "merchant", "country", "timestamp",
]
ALLOWED_CURRENCIES = ("MAD", "EUR", "USD")
ALLOWED_TYPES = ("CARD_PAYMENT", "TRANSFER", "ATM_WITHDRAWAL", "LOAN_REPAYMENT")


def get_spark_session():
    return (
        SparkSession.builder
        .appName("bronze_to_silver_transactions")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "ledgerlake"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "changeme"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def _require_columns(df):
    missing = set(TRANSACTION_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing transaction columns: {sorted(missing)}")


def classify_transactions(df):
    """Return accepted rows, rejected rows, and duplicate counts.

    Validation order is stable so each record receives one primary reason.
    Exact replays collapse. Every distinct version of a conflicting transaction
    ID is quarantined as DUPLICATE_CONFLICT.
    """
    _require_columns(df)
    records = df.select(*TRANSACTION_COLUMNS)
    input_count = records.count()
    unique = records.dropDuplicates(TRANSACTION_COLUMNS).persist()
    unique_count = unique.count()
    exact_duplicates = input_count - unique_count

    typed = (
        unique
        .withColumn("amount_decimal", col("amount").cast(DecimalType(18, 2)))
        .withColumn("event_time", col("event_timestamp").cast(TimestampType()))
        .withColumn("transaction_time", col("timestamp").cast(TimestampType()))
        .withColumn("schema_version_int", col("schema_version").cast(IntegerType()))
    )
    conflict_ids = (
        typed.groupBy("transaction_id").count()
        .filter(col("transaction_id").isNotNull() & (col("count") > 1))
        .select("transaction_id")
        .withColumn("duplicate_conflict", lit(True))
    )
    checked = typed.join(conflict_ids, "transaction_id", "left").withColumn(
        "rejection_reason",
        when(col("event_id").isNull() | (trim(col("event_id")) == ""), "MISSING_EVENT_ID")
        .when(col("event_type").isNull() | (col("event_type") != "TRANSACTION_CREATED"), "INVALID_EVENT_TYPE")
        .when(col("producer").isNull() | (col("producer") != "transaction-producer"), "INVALID_PRODUCER")
        .when(col("schema_version_int").isNull() | (col("schema_version_int") != 1), "UNSUPPORTED_SCHEMA_VERSION")
        .when(col("event_time").isNull(), "INVALID_EVENT_TIMESTAMP")
        .when(col("transaction_id").isNull() | (trim(col("transaction_id")) == ""), "MISSING_TRANSACTION_ID")
        .when(col("customer_id").isNull() | (trim(col("customer_id")) == ""), "MISSING_CUSTOMER_ID")
        .when(col("amount_decimal").isNull(), "INVALID_AMOUNT_FORMAT")
        .when(~col("amount").cast("string").rlike(r"^-?\d+(\.\d{1,2})?$"), "INVALID_AMOUNT_PRECISION")
        .when(col("amount_decimal") <= 0, "NON_POSITIVE_AMOUNT")
        .when(col("currency").isNull() | ~col("currency").isin(*ALLOWED_CURRENCIES), "INVALID_CURRENCY")
        .when(col("type").isNull() | ~col("type").isin(*ALLOWED_TYPES), "INVALID_TRANSACTION_TYPE")
        .when(col("merchant").isNull() | (trim(col("merchant")) == ""), "MISSING_MERCHANT")
        .when(col("country").isNull() | (trim(col("country")) == ""), "MISSING_COUNTRY")
        .when(col("transaction_time").isNull(), "INVALID_TRANSACTION_TIMESTAMP")
        .when(col("duplicate_conflict") == True, "DUPLICATE_CONFLICT")
    )

    rejected = (
        checked.filter(col("rejection_reason").isNotNull())
        .drop("amount_decimal", "event_time", "transaction_time", "schema_version_int", "duplicate_conflict")
        .withColumn("rejected_at", current_timestamp())
    )
    accepted = (
        checked.filter(col("rejection_reason").isNull())
        .drop(
            "amount", "timestamp", "event_timestamp", "schema_version_int",
            "duplicate_conflict", "rejection_reason",
        )
        .withColumnRenamed("amount_decimal", "amount")
        .withColumnRenamed("event_time", "event_timestamp")
        .withColumnRenamed("transaction_time", "timestamp")
        .withColumn(
            "country",
            when(col("country").isin("MA", "Maroc"), "Morocco")
            .when(col("country") == "FR", "France")
            .when(col("country") == "US", "USA")
            .otherwise(col("country")),
        )
        .withColumn(
            "country_region",
            when(col("country") == "Morocco", "Africa")
            .when(col("country") == "France", "Europe")
            .when(col("country") == "USA", "North America")
            .otherwise("Unknown"),
        )
        .withColumn("year", year("timestamp"))
        .withColumn("month", month("timestamp"))
        .withColumn("day", dayofmonth("timestamp"))
    )
    return accepted, rejected, {"input": input_count, "exact_duplicates": exact_duplicates}, unique


def transform_transactions(df):
    """Compatibility helper returning only accepted records."""
    accepted, _, _, cached = classify_transactions(df)
    cached.unpersist()
    return accepted


def rebuild_transactions(spark, bronze_path, silver_path, rejected_path):
    paths = {path.rstrip("/") for path in (bronze_path, silver_path, rejected_path)}
    if len(paths) != 3:
        raise ValueError("Bronze, Silver, and quarantine paths must be different")
    raw = spark.read.option("mode", "FAILFAST").json(bronze_path)
    if raw.isEmpty():
        raise ValueError("Empty Bronze input; refusing to replace Silver")

    accepted, rejected, metrics, cached = classify_transactions(raw)
    accepted = accepted.persist()
    rejected = rejected.persist()
    try:
        metrics["accepted"] = accepted.count()
        metrics["rejected"] = rejected.count()
        if metrics["input"] != metrics["accepted"] + metrics["rejected"] + metrics["exact_duplicates"]:
            raise RuntimeError(f"Transaction reconciliation failed: {metrics}")
        # Preserve invalid evidence even when every row is rejected. The
        # accepted Silver dataset is replaced only after valid rows exist.
        rejected.write.mode("overwrite").parquet(rejected_path)
        if metrics["accepted"] == 0:
            raise ValueError(f"No accepted transactions; Silver unchanged: {metrics}")
        (
            accepted.write.mode("overwrite")
            .partitionBy("year", "month", "day")
            .format("delta")
            .save(silver_path)
        )
        rejected.write.mode("overwrite").format("delta").save(rejected_path)
        return metrics
    finally:
        accepted.unpersist()
        rejected.unpersist()
        cached.unpersist()


def process_transactions():
    spark = get_spark_session()
    try:
        metrics = rebuild_transactions(
            spark,
            "s3a://bronze/transactions/*/*/*/*.json",
            "s3a://silver/transactions/",
            "s3a://silver-rejected/transactions/",
        )
        print(f"Rebuilt Silver transactions: {metrics}")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_transactions()
