'''DAG и PostgreSQL: обучаем модель, метрику — в таблицу, читаем обратно.'''

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

STUDENT = "user1"


def pg():
    return PostgresHook(postgres_conn_id="postgres_main")


@dag(dag_id=f"{STUDENT}_pg", schedule=None,
     start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=[STUDENT])
def pg_demo():

    @task
    def init() -> str:
        '''Своя схема и таблица метрик (IF NOT EXISTS — можно запускать много раз).'''
        pg().run(f'CREATE SCHEMA IF NOT EXISTS "{STUDENT}"')
        pg().run(f'''CREATE TABLE IF NOT EXISTS "{STUDENT}".runs (
                          ds TEXT, task TEXT, metric DOUBLE PRECISION,
                          created_at TIMESTAMP DEFAULT now())''')
        return "ok"

    @task
    def train(ok: str, ds=None) -> float:
        from sklearn.datasets import load_iris
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import train_test_split
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler

        data = load_iris(as_frame=True).frame
        train_df, test_df = train_test_split(data, test_size=0.25, random_state=42,
                                             stratify=data["target"])
        model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=5000))
        model.fit(train_df.drop(columns="target"), train_df["target"])
        accuracy = model.score(test_df.drop(columns="target"), test_df["target"])

        pg().insert_rows(f'"{STUDENT}".runs', rows=[(ds, "train_logreg", round(accuracy, 4))])
        return round(accuracy, 4)

    @task
    def read_back(accuracy: float):
        rows = pg().get_records(
            f'SELECT ds, task, metric FROM "{STUDENT}".runs ORDER BY created_at DESC LIMIT 5')
        print("accuracy этой задачи:", accuracy)
        print("последние строки в базе:")
        for row in rows:
            print("  ", row)

    read_back(train(init()))


pg_demo()
