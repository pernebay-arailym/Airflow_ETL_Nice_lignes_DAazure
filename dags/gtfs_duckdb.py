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
    top_busiest_stops,
)

from transform_duckdb import (
    init_db,
    load_static_gtfs,
    load_rt_snapshots,
    build_core_marts,
    export_for_powerbi,
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

    top_stops_task = PythonOperator(
        task_id="top_busiest_stops",
        python_callable=top_busiest_stops,
    )

    init_db_task = PythonOperator(task_id="init_db", python_callable=init_db)
    load_static_task = PythonOperator(
        task_id="load_static_gtfs", python_callable=load_static_gtfs
    )
    load_rt_task = PythonOperator(
        task_id="load_rt_snapshots", python_callable=load_rt_snapshots
    )
    build_marts_task = PythonOperator(
        task_id="build_core_marts", python_callable=build_core_marts
    )
    export_task = PythonOperator(
        task_id="export_for_powerbi", python_callable=export_for_powerbi
    )

    # Define task order
    # Extract tasks
    extract_vehicle_task
    (
        extract_tripupdates_task >> late_count_task
    )  # late trips depend only on trip updates
    (
        extract_tripupdates_task >> top_stops_task
    )  # top busiest stops depend only on trip updates

    # Database init and static data
    [
        extract_vehicle_task,
        extract_tripupdates_task,
    ] >> init_db_task  # init DB after extractions
    init_db_task >> load_static_task  # load static GTFS after DB ready

    # Real-time + marts + export
    [
        load_static_task,
        extract_tripupdates_task,
    ] >> load_rt_task  # load RT data once static + trip updates available
    load_rt_task >> build_marts_task >> export_task  # build marts, then export
