"""Monthly reload of a full month from the FDSN Event API, plus backfill.

Each run loads the month before its logical date: the run for March 1
loads February. This picks up late revisions missed by the incremental
DAG. With catchup=True, Airflow also creates runs for every past month
since start_date, which backfills the history.
"""

"""Ежемесячная перезагрузка полного месяца из FDSN Event API, плюс бэкфилл.

Каждый запуск загружает месяц перед своей логической датой: запуск 1 марта
загружает февраль. Это позволяет учитывать последние изменения, которые были пропущены инкрементальным
DAG. С параметром catchup=True, Airflow также создает запуски для каждого прошлого месяца
с момента start_date, что позволяет заполнить историю.
"""

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
        return fetch_period(start, end)

    @task
    def load(key: str) -> dict:
        from pipeline.load_snapshot import load_object
        return load_object(key)

    load(fetch())


earthquake_monthly()