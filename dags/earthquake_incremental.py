"""Incremental load of recent earthquake events.

Every 15 minutes: fetch the all_day feed, store it in object storage
and upsert it into Postgres. Keeps the table close to real time.
"""

"""Инкрементальная загрузка недавних событий землетрясений. 

Каждые 15 минут: скачивает фид all_day, сохраняет его в объектное хранилище 
и вставляет/обновляет его в Postgres. Поддерживает таблицу почти в реальном времени.
"""

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
        return fetch_feed("all_day")

    @task
    def load(key: str) -> dict:
        from pipeline.load_snapshot import load_object
        return load_object(key)

    load(fetch())


earthquake_incremental()