'''Параллельные задачи: prepare → три обучения → select_best. Данные — wine.'''

import pendulum
from airflow.decorators import dag, task

STUDENT = "user1"


@dag(dag_id=f"{STUDENT}_parallel", schedule=None,
     start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=[STUDENT])
def parallel():

    @task
    def prepare() -> dict:
        '''Готовит данные. Таблицу между задачами не передаём (XCom — только маленькое):
        каждая задача обучения повторит разбиение с тем же random_state и получит те же выборки.'''
        from sklearn.datasets import load_wine
        from sklearn.model_selection import train_test_split

        data = load_wine(as_frame=True).frame
        train, test = train_test_split(data, test_size=0.25, random_state=42, stratify=data["target"])
        return {"train_rows": len(train), "test_rows": len(test)}

    @task
    def train(model_name: str, sizes: dict) -> dict:
        '''Обучает одну модель. Три такие задачи идут параллельно.'''
        from sklearn.datasets import load_wine
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import train_test_split
        from sklearn.neighbors import KNeighborsClassifier
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler
        from sklearn.tree import DecisionTreeClassifier

        data = load_wine(as_frame=True).frame
        train, test = train_test_split(data, test_size=0.25, random_state=42, stratify=data["target"])

        models = {
            "logreg": make_pipeline(StandardScaler(), LogisticRegression(max_iter=5000)),
            "knn": make_pipeline(StandardScaler(), KNeighborsClassifier(n_neighbors=5)),
            "tree": DecisionTreeClassifier(max_depth=3, random_state=42),
        }
        model = models[model_name].fit(train.drop(columns="target"), train["target"])
        accuracy = model.score(test.drop(columns="target"), test["target"])
        return {"model": model_name, "accuracy": round(accuracy, 4)}

    @task
    def select_best(r1: dict, r2: dict, r3: dict) -> dict:
        results = [r1, r2, r3]
        best = max(results, key=lambda r: r["accuracy"])
        print("результаты:", [(r["model"], r["accuracy"]) for r in results])
        print("лучший:", best["model"])
        return best

    sizes = prepare()
    select_best(train("logreg", sizes), train("knn", sizes), train("tree", sizes))


parallel()
