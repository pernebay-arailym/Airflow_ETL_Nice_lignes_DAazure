from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys  # to manipulate the Python path
from pathlib import Path

# Add project root to sys.path so we can import extract_rt
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

# Import your extraction functions
from dags.extract_rt import (
    extract_vehicle_positions,
    extract_trip_updates,
    count_late_trips,
)

# Default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="gtfs_realtime_etl",
    default_args=default_args,
    description="ETL pipeline for Lignes d'Azur GTFS-RT data",
    start_date=datetime(2025, 9, 2),
    schedule_interval=timedelta(minutes=5),  # adjust as needed
    catchup=False,
    tags=["gtfs", "realtime", "duckdb"],
) as dag:

    # Task 1: extract vehicle positions
    extract_vehicle_task = PythonOperator(
        task_id="extract_vehicle_positions",
        python_callable=extract_vehicle_positions,
    )

    # Task 2: extract trip updates
    extract_tripupdates_task = PythonOperator(
        task_id="extract_trip_updates",
        python_callable=extract_trip_updates,
    )

    late_count_task = PythonOperator(
        task_id="count_late_trips",
        python_callable=count_late_trips,
    )

    # Define task order
    [extract_vehicle_task, extract_tripupdates_task] >> late_count_task
