"""Test DAG to check that Airflow runs tasks. Triggered manually."""

"""Тестовый DAG для проверки того, что Airflow выполняет задачи. Запускается вручную."""

from datetime import datetime
from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["test"])
def hello():
    @task
    def say_hello():
        print("Hello from Airflow!")

    say_hello()


hello()