from datetime import datetime, timedelta

from airflow.sdk import dag, task

RAW_DIR = "/opt/airflow/data/raw"


@dag(
    schedule="@monthly",
    start_date=datetime(2024, 2, 1),
    catchup=True,
    max_active_runs=2,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["earthquake", "backfill"],
)
def earthquake_monthly():

    @task
    def fetch(logical_date=None) -> str:
        from pipeline.fdsn import fetch_period

        if logical_date is None:
            raise ValueError("This DAG needs a logical date to know which month to load")

        # run on the 1st of a month → load the previous full month
        end = logical_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start = (end - timedelta(days=1)).replace(day=1)
        return fetch_period(start, end, RAW_DIR)

    @task
    def load(path: str) -> dict:
        from pipeline.load_snapshot import load as load_file
        return load_file(path)

    load(fetch())


earthquake_monthly()