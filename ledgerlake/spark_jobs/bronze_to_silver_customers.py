"""Build accepted and quarantined Silver customer events from complete Bronze history."""

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
from pyspark.sql.types import IntegerType, TimestampType


load_dotenv()

CUSTOMER_COLUMNS = [
    "event_id", "event_type", "schema_version", "event_timestamp", "producer",
    "customer_id", "age", "country", "segment",
]
ALLOWED_EVENT_TYPES = ("ACCOUNT_CREATED", "PROFILE_UPDATED")


def get_spark_session():
    return (
        SparkSession.builder
        .appName("bronze_to_silver_customers")
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
    missing = set(CUSTOMER_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing customer columns: {sorted(missing)}")


def classify_customers(df):
    """Return accepted rows, rejected rows, and duplicate counts."""
    _require_columns(df)
    records = df.select(*CUSTOMER_COLUMNS)
    input_count = records.count()
    unique = records.dropDuplicates(CUSTOMER_COLUMNS).persist()
    unique_count = unique.count()
    exact_duplicates = input_count - unique_count

    typed = (
        unique
        .withColumn("age_int", col("age").cast(IntegerType()))
        .withColumn("event_time", col("event_timestamp").cast(TimestampType()))
        .withColumn("schema_version_int", col("schema_version").cast(IntegerType()))
    )
    
    conflict_ids = (
        typed.groupBy("event_id").count()
        .filter(col("event_id").isNotNull() & (col("count") > 1))
        .select("event_id")
        .withColumn("duplicate_conflict", lit(True))
    )
    
    checked = typed.join(conflict_ids, "event_id", "left").withColumn(
        "rejection_reason",
        when(col("event_id").isNull() | (trim(col("event_id")) == ""), "MISSING_EVENT_ID")
        .when(col("event_type").isNull() | ~col("event_type").isin(*ALLOWED_EVENT_TYPES), "INVALID_EVENT_TYPE")
        .when(col("producer").isNull() | (col("producer") != "customer-producer"), "INVALID_PRODUCER")
        .when(col("schema_version_int").isNull() | (col("schema_version_int") != 1), "UNSUPPORTED_SCHEMA_VERSION")
        .when(col("event_time").isNull(), "INVALID_EVENT_TIMESTAMP")
        .when(col("customer_id").isNull() | (trim(col("customer_id")) == ""), "MISSING_CUSTOMER_ID")
        .when(col("age_int").isNull() | (col("age_int") < 0), "INVALID_AGE")
        .when(col("country").isNull() | (trim(col("country")) == ""), "MISSING_COUNTRY")
        .when(col("duplicate_conflict") == True, "DUPLICATE_CONFLICT")
    )

    rejected = (
        checked.filter(col("rejection_reason").isNotNull())
        .drop("age_int", "event_time", "schema_version_int", "duplicate_conflict")
        .withColumn("rejected_at", current_timestamp())
    )
    
    accepted = (
        checked.filter(col("rejection_reason").isNull())
        .drop("age", "schema_version_int", "duplicate_conflict", "rejection_reason")
        .withColumnRenamed("age_int", "age")
        .withColumnRenamed("event_time", "event_timestamp")
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
        .withColumn(
            "customer_segment", 
            when(col("segment").isNull() | (trim(col("segment")) == ""), "UNCLASSIFIED")
            .otherwise(col("segment"))
        )
        .withColumn("year", year("event_timestamp"))
        .withColumn("month", month("event_timestamp"))
        .withColumn("day", dayofmonth("event_timestamp"))
    )
    return accepted, rejected, {"input": input_count, "exact_duplicates": exact_duplicates}, unique


def rebuild_customers(spark, bronze_path, silver_path, rejected_path):
    paths = {path.rstrip("/") for path in (bronze_path, silver_path, rejected_path)}
    if len(paths) != 3:
        raise ValueError("Bronze, Silver, and quarantine paths must be different")
    raw = spark.read.option("mode", "FAILFAST").json(bronze_path)
    if raw.isEmpty():
        raise ValueError("Empty Bronze input; refusing to replace Silver")

    accepted, rejected, metrics, cached = classify_customers(raw)
    accepted = accepted.persist()
    rejected = rejected.persist()
    try:
        metrics["accepted"] = accepted.count()
        metrics["rejected"] = rejected.count()
        if metrics["input"] != metrics["accepted"] + metrics["rejected"] + metrics["exact_duplicates"]:
            raise RuntimeError(f"Customer reconciliation failed: {metrics}")
        if metrics["accepted"] == 0:
            raise ValueError(f"No accepted customers; Silver unchanged: {metrics}")
        (
            accepted.write.mode("overwrite")
            .partitionBy("year", "month", "day")
            .format("delta").save(silver_path)
        )
        rejected.write.mode("overwrite").format("delta").save(rejected_path)
        return metrics
    finally:
        accepted.unpersist()
        rejected.unpersist()
        cached.unpersist()


def process_customers():
    spark = get_spark_session()
    try:
        metrics = rebuild_customers(
            spark,
            "s3a://bronze/customers/*/*/*/*.json",
            "s3a://silver/customers/",
            "s3a://silver-rejected/customers/",
        )
        print(f"Rebuilt Silver customers: {metrics}")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_customers()
