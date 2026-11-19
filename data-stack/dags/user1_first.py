'''Первый DAG: extract → transform → load. Данные — Iris.'''

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

STUDENT = "user1"        # <-- ваш логин (в имени файла он тоже должен быть)


@dag(
    dag_id=f"{STUDENT}_first",                    # имя DAG-а в интерфейсе
    schedule=None,                                # расписание нет — запускаем сами, кнопкой
    start_date=pendulum.datetime(2026, 1, 1),     # с какой даты существует конвейер
    catchup=False,                                # не «догонять» пропущенные даты
    tags=[STUDENT],                               # по тегу удобно фильтровать свои DAG-и
    default_args={"owner": STUDENT, "retries": 1, "retry_delay": timedelta(seconds=15)},
)
def first():
    '''Три задачи по порядку.'''

    @task
    def extract() -> dict:
        '''Шаг 1: берём данные. Тяжёлые импорты — внутри задачи: файл DAG-а перечитывается часто.'''
        from sklearn.datasets import load_iris

        data = load_iris(as_frame=True).frame
        return {                                   # наружу — только маленький словарь
            "rows": len(data),
            "means": data.mean(numeric_only=True).round(2).to_dict(),
        }

    @task
    def transform(info: dict) -> dict:
        '''Шаг 2: из средних выбираем колонку с максимальным средним.'''
        means = info["means"]
        top = max(means, key=means.get)
        return {"rows": info["rows"], "top_feature": top, "top_mean": means[top]}

    @task
    def load(report: dict) -> str:
        '''Шаг 3: печатаем отчёт (в разделе 7 научимся сохранять в S3).'''
        print(f"Iris: {report['rows']} строк; максимальное среднее у «{report['top_feature']}» "
              f"= {report['top_mean']}")
        return "готово"

    load(transform(extract()))                     # вот и весь порядок: extract → transform → load


first()                                            # вызов в конце файла обязателен
