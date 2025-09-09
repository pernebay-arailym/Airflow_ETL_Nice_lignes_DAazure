from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# import cleanup function from your DuckDB transform module
from transform_duckdb import cleanup_old_data

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id="cleanup_duckdb",
    default_args=default_args,
    description="Daily cleanup of old data in DuckDB",
    schedule_interval="@daily",  # runs once per day
    start_date=datetime(2025, 9, 2),
    catchup=False,
    tags=["duckdb", "maintenance"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="cleanup_old_data",
        python_callable=cleanup_old_data,
        op_kwargs={"days": 7},  # keep only the last 7 days of data
    )

    cleanup_task
