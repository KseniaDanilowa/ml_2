'''Проверочный DAG: если он зелёный — весь стек работает.'''

import os

import pendulum
from airflow.decorators import dag, task

os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")


@dag(dag_id="local_smoke", schedule=None,
     start_date=pendulum.datetime(2026, 1, 1), catchup=False, tags=["local"])
def smoke():

    @task
    def s3_ok():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="s3_local")
        hook.load_string("проверка стека", key="smoke/hello.txt",
                         bucket_name="airflow-logs", replace=True)
        return hook.check_for_key("smoke/hello.txt", bucket_name="airflow-logs")

    @task
    def pg_ok(s3_result):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_main")
        hook.run('CREATE SCHEMA IF NOT EXISTS "smoke"')
        hook.run('CREATE TABLE IF NOT EXISTS "smoke"."checks" (what TEXT, ok BOOLEAN, '
                 'created_at TIMESTAMP DEFAULT now())')
        hook.insert_rows('"smoke"."checks"', rows=[("s3", s3_result)])
        return True

    @task
    def mlflow_ok(pg_result):
        import mlflow

        mlflow.set_experiment("local_smoke")
        with mlflow.start_run(run_name="smoke"):
            mlflow.log_param("pg", pg_result)
            mlflow.log_metric("ok", 1.0)
            mlflow.log_dict({"probe": True}, "probe.json")   # артефакт -> S3, бакет mlflow
        return "MLflow доступен"

    @task
    def done(message: str):
        print("итог:", message)

    done(mlflow_ok(pg_ok(s3_ok())))


smoke()
