"""
financial_backfill_pipeline.py — DAG pour backfill manuel (Palier E.2)

Ce DAG permet de relancer le pipeline pour des dates passées.
Déclenché manuellement via l'UI Airflow avec conf:
{
  "start_date": "2026-01-01",
  "end_date": "2026-01-31"
}
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SPARK_JOBS_DIR = PROJECT_ROOT / "spark_jobs"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
DATA_QUALITY_DIR = PROJECT_ROOT / "data_quality" / "great_expectations"
INGESTION_DIR = PROJECT_ROOT / "ingestion"

sys.path.insert(0, str(INGESTION_DIR))

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISSED for DAG: {dag.dag_id}")
    for task in task_list:
        print(f"  Task missed SLA: {task.task_id}")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "email_on_failure": False,
    "email_on_retry": False,
    "sla": timedelta(hours=4),
    "execution_timeout": timedelta(hours=5),
}

with DAG(
    dag_id="financial_backfill_pipeline",
    description="Backfill manuel LedgerLake pour dates historiques",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=3,
    max_active_tasks=8,
    tags=["ledgerlake", "financial", "backfill"],
    sla_miss_callback=sla_miss_callback,
    render_template_as_native_obj=True,
    params={
        "start_date": "2026-01-01",
        "end_date": "2026-01-01",
    },
) as dag:

    def generate_date_range(**context):
        """Generate list of dates to backfill from params."""
        params = context["params"]
        start = datetime.fromisoformat(params["start_date"])
        end = datetime.fromisoformat(params["end_date"])
        
        if start > end:
            raise ValueError("start_date must be <= end_date")
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        
        print(f"Backfill date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")
        return dates

    generate_dates = PythonOperator(
        task_id="generate_date_range",
        python_callable=generate_date_range,
    )

    def run_daily_pipeline_for_date(target_date: str, **context):
        """Run the full pipeline for a single date by triggering the daily DAG."""
        from airflow.api.client.local_client import Client
        
        client = Client(None, None)
        
        # Trigger the daily DAG for this specific date
        run_id = f"backfill__{target_date}"
        dag_run = client.trigger_dag(
            dag_id="financial_daily_pipeline",
            run_id=run_id,
            execution_date=target_date,
            conf={"backfill": True, "target_date": target_date},
            replace_microseconds=False,
        )
        print(f"Triggered financial_daily_pipeline for {target_date} (run_id: {run_id})")
        return run_id

    def run_backfill_loop(**context):
        """Loop through dates and trigger daily DAG for each."""
        dates = context["ti"].xcom_pull(task_ids="generate_date_range")
        run_ids = []
        
        for target_date in dates:
            run_id = run_daily_pipeline_for_date(target_date, context)
            run_ids.append(run_id)
        
        print(f"Triggered {len(run_ids)} backfill runs")
        return run_ids

    run_backfill = PythonOperator(
        task_id="run_backfill",
        python_callable=run_backfill_loop,
    )

    def monitor_backfill(**context):
        """Monitor triggered backfill runs."""
        run_ids = context["ti"].xcom_pull(task_ids="run_backfill")
        print(f"Monitoring {len(run_ids)} backfill runs...")
        # In practice, you'd poll for completion here
        # For now, just log the run IDs
        for run_id in run_ids:
            print(f"  {run_id}")
        return {"triggered": len(run_ids), "run_ids": run_ids}

    monitor = PythonOperator(
        task_id="monitor_backfill",
        python_callable=monitor_backfill,
    )

    generate_dates >> run_backfill >> monitor