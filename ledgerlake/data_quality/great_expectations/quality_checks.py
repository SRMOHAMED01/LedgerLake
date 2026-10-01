"""Run Great Expectations checks against Bronze or Silver/Gold datasets.

Examples (from the repository root):
    python ledgerlake/data_quality/great_expectations/quality_checks.py \
        --stage bronze_to_silver --entity transactions \
        --path 's3a://bronze/transactions/*/*/*/*.json' --format json

    python ledgerlake/data_quality/great_expectations/quality_checks.py \
        --stage silver_to_gold --entity transactions \
        --path 's3a://silver/transactions/' --format parquet
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp


BRONZE_ENTITIES = ("transactions", "customers", "loans")
GOLD_ENTITIES = (
    "transactions",
    "customer_360",
    "loan_portfolio_analysis",
    "fraud_detection_metrics",
    "customer_monthly_spending",
)


def get_spark_session() -> SparkSession:
    """Create local Spark with the same MinIO/S3A settings as the ETL jobs."""
    load_dotenv()
    return (
        SparkSession.builder.appName("ledgerlake_data_quality")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "ledgerlake"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "changeme"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def _required_columns(stage: str, entity: str) -> tuple[str, ...]:
    """Return the minimum schema needed to evaluate an entity's expectations."""
    schemas = {
        ("bronze_to_silver", "transactions"): (
            "transaction_id", "customer_id", "amount", "currency", "timestamp",
        ),
        ("bronze_to_silver", "customers"): ("customer_id", "age", "event_timestamp"),
        ("bronze_to_silver", "loans"): (
            "loan_id", "customer_id", "amount", "status", "event_timestamp",
        ),
        ("silver_to_gold", "transactions"): ("transaction_id", "customer_id", "amount"),
        ("silver_to_gold", "customer_360"): ("customer_id", "risk_level", "credit_score"),
        ("silver_to_gold", "loan_portfolio_analysis"): (
            "loan_id", "loan_amount", "duration_months", "default_probability", "risk_category",
        ),
        ("silver_to_gold", "fraud_detection_metrics"): (
            "customer_id", "number_transactions", "suspicious_operations", "fraud_score",
        ),
        ("silver_to_gold", "customer_monthly_spending"): (
            "customer_id", "spending_month", "total_spending", "number_transactions",
        ),
    }
    try:
        return schemas[(stage, entity)]
    except KeyError as exc:
        raise ValueError(f"Unsupported quality stage/entity: {stage}/{entity}") from exc


def _expectations(gx: Any, stage: str, entity: str) -> list[Any]:
    """Build the Great Expectations for one dataset boundary."""
    expectations: list[Any] = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
    ]

    if stage == "bronze_to_silver":
        if entity == "transactions":
            expectations.extend(
                [
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id"),
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column="amount", min_value=0, strict_min=True,
                    ),
                    gx.expectations.ExpectColumnValuesToBeInSet(
                        column="currency", value_set=["MAD", "EUR", "USD"],
                    ),
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="timestamp"),
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column="timestamp",
                        max_value=datetime.now(timezone.utc).replace(tzinfo=None),
                    ),
                ]
            )
        elif entity == "customers":
            expectations.extend(
                [
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column="age", min_value=18, max_value=120,
                    ),
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="event_timestamp"),
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column="event_timestamp",
                        max_value=datetime.now(timezone.utc).replace(tzinfo=None),
                    ),
                ]
            )
        else:  # loans
            expectations.extend(
                [
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="loan_id"),
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column="amount", min_value=0, strict_min=True,
                    ),
                    gx.expectations.ExpectColumnValuesToBeInSet(
                        column="status",
                        value_set=["REQUESTED", "APPROVED", "REJECTED", "REPAID", "DEFAULTED"],
                    ),
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="event_timestamp"),
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column="event_timestamp",
                        max_value=datetime.now(timezone.utc).replace(tzinfo=None),
                    ),
                ]
            )
        return expectations

    if entity == "transactions":
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
                gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id"),
            ]
        )
    elif entity == "customer_360":
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
                gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id"),
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column="risk_level", value_set=["High Risk", "Medium Risk", "Low Risk"],
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="credit_score", min_value=300, max_value=850,
                ),
            ]
        )
    elif entity == "loan_portfolio_analysis":
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="loan_id"),
                gx.expectations.ExpectColumnValuesToBeUnique(column="loan_id"),
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column="risk_category", value_set=["Critical", "High", "Medium", "Low"],
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="default_probability", min_value=0, max_value=1,
                ),
            ]
        )
    elif entity == "fraud_detection_metrics":
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
                gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id"),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="fraud_score", min_value=0, max_value=100,
                ),
            ]
        )
    else:  # customer_monthly_spending
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="spending_month"),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="total_spending", min_value=0,
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="number_transactions", min_value=1,
                ),
            ]
        )
    return expectations


def validate_dataframe(dataframe: DataFrame, stage: str, entity: str) -> bool:
    """Validate a Spark DataFrame and return True only if every expectation passes."""
    try:
        import great_expectations as gx
    except ImportError as exc:
        raise RuntimeError(
            "Great Expectations is not installed. Install ledgerlake/data_quality/great_expectations/requirements.txt."
        ) from exc

    required = _required_columns(stage, entity)
    missing = sorted(set(required) - set(dataframe.columns))
    if missing:
        raise ValueError(f"{stage}/{entity} is missing required columns: {', '.join(missing)}")

    # GE compares temporal values as timestamps rather than raw ISO-formatted strings.
    if stage == "bronze_to_silver":
        timestamp_column = "timestamp" if entity == "transactions" else "event_timestamp"
        dataframe = dataframe.withColumn(timestamp_column, to_timestamp(col(timestamp_column)))

    context = gx.get_context()
    data_source = context.data_sources.add_spark(name="ledgerlake_spark_quality_source")
    data_asset = data_source.add_dataframe_asset(name=f"{stage}_{entity}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_dataset")

    suite_name = f"{stage}_{entity}_suite"
    suite = gx.ExpectationSuite(name=suite_name)
    expectations = _expectations(gx, stage, entity)
    for expectation in expectations:
        suite.add_expectation(expectation)
    suite = context.suites.add(suite)

    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=f"{stage}_{entity}_validation",
    )
    validation_definition = context.validation_definitions.add(validation_definition)
    validation_results = validation_definition.run(
        batch_parameters={"dataframe": dataframe},
    )

    success = bool(validation_results.success)
    for expectation, result in zip(expectations, validation_results.results):
        passed = bool(result.success)
        print(f"{'PASS' if passed else 'FAIL'}: {type(expectation).__name__}")
        if not passed:
            print(result)

    print(f"Great Expectations {stage}/{entity}: {'PASSED' if success else 'FAILED'}")
    return success


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate LedgerLake Bronze, Silver, or Gold data.")
    parser.add_argument("--stage", choices=("bronze_to_silver", "silver_to_gold"), required=True)
    parser.add_argument("--entity", required=True, help="Dataset name for the selected stage")
    parser.add_argument("--path", required=True, help="Local or s3a:// input path (wildcards supported)")
    parser.add_argument("--format", choices=("json", "parquet"), required=True)
    args = parser.parse_args()
    valid_entities = BRONZE_ENTITIES if args.stage == "bronze_to_silver" else GOLD_ENTITIES
    if args.entity not in valid_entities:
        parser.error(f"--entity for {args.stage} must be one of: {', '.join(valid_entities)}")
    return args


def main() -> int:
    args = _parse_args()
    spark = get_spark_session()
    try:
        reader = spark.read.option("mode", "FAILFAST")
        dataframe = reader.json(args.path) if args.format == "json" else reader.parquet(args.path)
        success = validate_dataframe(dataframe, args.stage, args.entity)
        return 0 if success else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
