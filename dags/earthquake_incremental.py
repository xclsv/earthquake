from datetime import datetime, timedelta

from airflow.sdk import dag, task

RAW_DIR = "/opt/airflow/data/raw"


@dag(
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["earthquake"],
)
def earthquake_incremental():

    @task
    def fetch() -> str:
        from pipeline.fetch import fetch_feed
        return fetch_feed("all_day", RAW_DIR)

    @task
    def load(path: str) -> dict:
        from pipeline.load_snapshot import load as load_file
        return load_file(path)

    load(fetch())


earthquake_incremental()