'''DAG и S3: prepare → check. Ключ — по дате запуска ds.'''

import io

import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

STUDENT = "user1"
BUCKET = STUDENT                                  # бакет называется вашим логином


@dag(dag_id=f"{STUDENT}_s3hook", schedule=None,
     start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=[STUDENT])
def s3hook_demo():

    @task
    def prepare(ds=None) -> str:
        '''ds — дата запуска (например 2026-11-06): у каждого запуска своя «папка» в S3.'''
        from sklearn.datasets import load_iris

        df = load_iris(as_frame=True).frame
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)            # DataFrame -> parquet (буфер в памяти)

        key = f"airflow/{ds}/iris.parquet"
        S3Hook(aws_conn_id="s3_local").load_bytes(  # байты -> S3, ключи берёт из Connection
            buf.getvalue(), key=key, bucket_name=BUCKET, replace=True)
        return key                                 # дальше передаём только ключ

    @task
    def check(key: str):
        hook = S3Hook(aws_conn_id="s3_local")
        print("файл на месте:", hook.check_for_key(key, bucket_name=BUCKET))
        print("в airflow/ лежит:", hook.list_keys(prefix="airflow/", bucket_name=BUCKET))

    check(prepare())


s3hook_demo()
