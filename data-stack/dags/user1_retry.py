'''Задача-врун: падает в половине случаев. Смотрим, как Airflow повторяет попытки.'''

import random
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

STUDENT = "user1"


@dag(dag_id=f"{STUDENT}_retry", schedule=None,
     start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=[STUDENT])
def retry():

    @task(retries=4, retry_delay=timedelta(seconds=10))   # упала — повторить ещё 4 раза
    def flaky() -> str:
        if random.random() < 0.5:
            raise RuntimeError("временный сбой: «база не ответила» (учебная ошибка)")
        return "ok"

    @task
    def report(status: str):
        print("итог:", status)

    report(flaky())


retry()
