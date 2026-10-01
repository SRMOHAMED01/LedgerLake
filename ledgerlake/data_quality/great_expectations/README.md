# Great Expectations quality gates

This directory contains a runnable validation CLI for LedgerLake datasets. It
uses Great Expectations Core with Spark DataFrames, so validation can run on
local files or MinIO paths without collecting the full dataset into pandas.

## Install

Use Python 3.10–3.13 for the Great Expectations release documented by its
current installation guide, and install Spark dependencies from this directory:

```bash
python -m pip install -r ledgerlake/data_quality/great_expectations/requirements.txt
```

The project also requires the Spark/MinIO environment used by the Spark jobs.
Set `MINIO_ENDPOINT_HOST`, `MINIO_ROOT_USER`, and `MINIO_ROOT_PASSWORD` as needed;
local defaults match the Spark jobs. For S3A access, Spark uses the Hadoop AWS
package configured in the session.

## Gate 1: Bronze before Spark (Bronze → Silver)

Run one validation per raw entity. A failed expectation exits with status 1,
allowing a shell pipeline or future Airflow task to stop before Spark runs.

```bash
python ledgerlake/data_quality/great_expectations/quality_checks.py \
	--stage bronze_to_silver --entity transactions \
	--path 's3a://bronze/transactions/*/*/*/*.json' --format json

python ledgerlake/data_quality/great_expectations/quality_checks.py \
	--stage bronze_to_silver --entity customers \
	--path 's3a://bronze/customers/*/*/*/*.json' --format json

python ledgerlake/data_quality/great_expectations/quality_checks.py \
	--stage bronze_to_silver --entity loans \
	--path 's3a://bronze/loans/*/*/*/*.json' --format json
```

Checks include non-empty input, required identifiers, positive amounts,
allowed currencies/statuses, valid customer ages, and timestamps that are not
in the future.

## Gate 2: Silver → Gold boundary

The `silver_to_gold` stage can validate Silver transactions before dbt and
materialized Gold outputs after dbt. For the pre-dbt Silver check:

```bash
python ledgerlake/data_quality/great_expectations/quality_checks.py \
	--stage silver_to_gold --entity transactions \
	--path 's3a://silver/transactions/' --format parquet
```

Gold models currently materialize as DuckDB tables, while this CLI accepts
Spark-readable JSON or Parquet. Export a Gold model to Parquet or point `--path`
at a Parquet copy before running its checks. dbt's own tests in
`ledgerlake/dbt_project/models/gold/schema.yml` remain the direct tests of the
dbt materializations.

```bash
python ledgerlake/data_quality/great_expectations/quality_checks.py \
	--stage silver_to_gold --entity customer_360 \
	--path 'data/gold/customer_360/' --format parquet

python ledgerlake/data_quality/great_expectations/quality_checks.py \
	--stage silver_to_gold --entity loan_portfolio_analysis \
	--path 'data/gold/loan_portfolio_analysis/' --format parquet
```

Supported Gold entities are `transactions`, `customer_360`,
`loan_portfolio_analysis`, `fraud_detection_metrics`, and
`customer_monthly_spending`. Checks cover non-empty data, identifiers and
uniqueness where applicable, allowed risk categories, score/probability bounds,
and non-negative spending/counts. The risk and fraud outputs are demonstration
heuristics; passing structural checks does not establish predictive quality.

## Code entry point

`quality_checks.py` accepts local paths and `s3a://` paths, supports Spark JSON
and Parquet readers, prints each expectation result, and returns a non-zero exit
code if any check fails. It does not modify source data. Validation results use
an ephemeral Great Expectations context; persistent result storage and Airflow
integration are future work.
