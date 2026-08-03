# data_quality/great_expectations/ — Validation qualité (Palier D)

À IMPLÉMENTER (great_expectations init dans ce dossier) : deux suites de
validation minimum.

## Suite "bronze_to_silver" (avant transformation Spark)
- expect_column_values_to_not_be_null : customer_id, transaction_id
- expect_column_values_to_be_between : amount (min=0, max=None)
- expect_column_values_to_be_in_set : currency (["MAD", "EUR", "USD"])
- expect_column_values_to_not_be_future_date : timestamp

## Suite "silver_to_gold" (avant dbt run)
- expect_column_values_to_be_unique : transaction_id (après dédup Spark)
- expect_column_values_to_be_in_set : risk_level (["low", "medium", "high"])
- expect_table_row_count_to_be_between : sanity check sur le volume attendu

Ces checkpoints sont appelés par la tâche `quality_check_silver` /
`quality_check_gold` du DAG Airflow (orchestration/dags/).
