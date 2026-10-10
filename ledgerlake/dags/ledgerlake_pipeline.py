from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ledgerlake_pipeline',
    default_args=default_args,
    description='LedgerLake End-to-End Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # 1. Ingestion: This simulates kicking off the bronze writer or producers
    # Normally producers are continuous, but we might just want to trigger a sync
    ingest_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='echo "Triggering Kafka Ingestion (Bronze Layer)..."'
    )

    # 2. Spark Jobs (Silver Layer)
    spark_transactions = BashOperator(
        task_id='spark_silver_transactions',
        bash_command='cd /opt/airflow/spark_jobs && python bronze_to_silver_transactions.py'
    )

    spark_customers = BashOperator(
        task_id='spark_silver_customers',
        bash_command='cd /opt/airflow/spark_jobs && python bronze_to_silver_customers.py'
    )

    spark_loans = BashOperator(
        task_id='spark_silver_loans',
        bash_command='cd /opt/airflow/spark_jobs && python bronze_to_silver_loans.py'
    )

    # 3. dbt (Gold Layer)
    dbt_run = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/dbt_project && dbt run'
    )

    dbt_test = BashOperator(
        task_id='dbt_test_gold',
        bash_command='cd /opt/airflow/dbt_project && dbt test'
    )

    # Define dependencies
    ingest_bronze >> [spark_transactions, spark_customers, spark_loans]
    [spark_transactions, spark_customers, spark_loans] >> dbt_run >> dbt_test
