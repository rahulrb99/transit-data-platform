from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="transit_static_ingestion",
    description="Download and load MBTA static GTFS data.",
    start_date=datetime(2026, 8, 29, tzinfo=UTC),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["gtfs", "mbta", "static"],
) as dag:
    download_gtfs = BashOperator(
        task_id="download_mbta_gtfs",
        bash_command="python -m ingestion.static.download_mbta_gtfs",
    )

    load_raw_tables = BashOperator(
        task_id="load_static_gtfs_raw",
        bash_command="python -m ingestion.static.load_static_gtfs",
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd dbt && dbt build",
    )

    download_gtfs >> load_raw_tables >> run_dbt
