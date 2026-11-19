'''Полный конвейер: данные → S3 → две модели параллельно (обе в MLflow) → лучшая в реестр → прогноз.'''

import os

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

STUDENT = "user1"
EXPERIMENT = f"{STUDENT}_step5"           # эксперимент в MLflow
MODEL_NAME = f"{STUDENT}_step5_model"     # модель в реестре


def mlflow_on():
    '''MLflow из задачи: адрес — окружение, логин — ваш, пароль — из Airflow Variable.'''
    import mlflow

    os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    os.environ["MLFLOW_TRACKING_USERNAME"] = STUDENT
    os.environ["MLFLOW_TRACKING_PASSWORD"] = Variable.get(f"{STUDENT}_mlflow_password",
                                                          default_var="")
    return mlflow


def log_sklearn_model(mlflow, model, X_example):
    '''Сохраняет sklearn-модель в текущий запуск (формат skops — как в практикуме 2).'''
    import skops.io as sio
    from mlflow.models import infer_signature

    return mlflow.sklearn.log_model(
        model, name="model", pip_requirements=["scikit-learn==1.6.1", "skops==0.16.0"],
        signature=infer_signature(X_example, model.predict(X_example)),
        input_example=X_example.head(3),
        skops_trusted_types=sio.get_untrusted_types(data=sio.dumps(model)),
    )


@dag(dag_id=f"{STUDENT}_step5", schedule=None,
     start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=[STUDENT])
def step5():

    @task
    def prepare(ds=None) -> str:
        '''Breast Cancer → S3 (parquet). Наружу — ключ.'''
        import io
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from sklearn.datasets import load_breast_cancer

        df = load_breast_cancer(as_frame=True).frame
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        key = f"airflow/{ds}/cancer.parquet"
        S3Hook(aws_conn_id="s3_local").load_bytes(buf.getvalue(), key=key,
                                                  bucket_name=STUDENT, replace=True)
        return key

    @task
    def train(model_name: str, key: str) -> dict:
        '''Обучает одну модель и пишет запуск в MLflow. Две задачи train идут параллельно.'''
        import pandas as pd
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import roc_auc_score
        from sklearn.model_selection import train_test_split
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler

        local = S3Hook(aws_conn_id="s3_local").download_file(key=key, bucket_name=STUDENT)   # вернёт путь к файлу
        df = pd.read_parquet(local)

        train_df, test_df = train_test_split(df, test_size=0.25, random_state=42,
                                             stratify=df["target"])
        from sklearn.neighbors import KNeighborsClassifier
        from sklearn.tree import DecisionTreeClassifier
        models = {
            "logreg": make_pipeline(StandardScaler(), LogisticRegression(max_iter=5000)),
            "random_forest": RandomForestClassifier(random_state=42),
            "knn": make_pipeline(StandardScaler(), KNeighborsClassifier()),
            "tree": DecisionTreeClassifier(random_state=42),
        }
        X_test, y_test = test_df.drop(columns="target"), test_df["target"]

        model = models[model_name].fit(train_df.drop(columns="target"), train_df["target"])
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])

        mlflow = mlflow_on()
        mlflow.set_experiment(EXPERIMENT)
        with mlflow.start_run(run_name=model_name) as run:
            mlflow.log_params({"model": model_name, "seed":42, "test_size":0.25})
            mlflow.log_metric("roc_auc", float(auc))
            log_sklearn_model(mlflow, model, X_test)
        return {"model": model_name, "roc_auc": float(auc), "run_id": run.info.run_id}

    @task
    def select_best(rows):
        ordered=sorted(rows,key=lambda r:-r['roc_auc']);print(ordered);return ordered[0]
    @task
    def release(best):
        from sklearn.datasets import load_breast_cancer
        mlflow=mlflow_on(); c=mlflow.MlflowClient(); value=best['roc_auc']; old=None

        if old is None or value>old:
            v=mlflow.register_model(f"runs:/{best['run_id']}/model",MODEL_NAME);c.set_registered_model_alias(MODEL_NAME,'champion',v.version)
        version=c.get_model_version_by_alias(MODEL_NAME,'champion').version
        m=mlflow.sklearn.load_model(f'models:/{MODEL_NAME}@champion');print(m.predict(load_breast_cancer(as_frame=True).data.head()).tolist());print('Переезд только при улучшении:',old,value,'версия',version);return {'old':old,'auc':value,'version':version}
    key=prepare();release(select_best([train.override(task_id='train_'+k)(k,key) for k in ['logreg', 'random_forest', 'tree', 'knn']]))

step5()
