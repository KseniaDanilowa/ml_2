import pendulum
from airflow.decorators import dag, task
from datetime import timedelta

@dag(dag_id='user1_step3',schedule=None,start_date=pendulum.datetime(2026,1,1),catchup=False,tags=['user1'])
def step3():
    @task
    def prepare(ds=None):
        from sklearn.datasets import load_iris
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        key=f'airflow/{ds}/iris.parquet';S3Hook(aws_conn_id='s3_local').load_bytes(load_iris(as_frame=True).frame.to_parquet(index=False),key,'user1',replace=True);return key
    @task
    def train(key,ds=None):
        import pandas as pd
        from tempfile import TemporaryDirectory
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import train_test_split
        with TemporaryDirectory() as tmp: d=pd.read_parquet(S3Hook(aws_conn_id='s3_local').download_file(key,'user1',local_path=tmp))
        a,b=train_test_split(d,test_size=.25,stratify=d.target,random_state=42);m=LogisticRegression(max_iter=500).fit(a.drop(columns='target'),a.target);accuracy=m.score(b.drop(columns='target'),b.target)
        pg=PostgresHook(postgres_conn_id='postgres_main');pg.run('CREATE SCHEMA IF NOT EXISTS user1');pg.run('CREATE TABLE IF NOT EXISTS user1.runs(ds text, task text, metric double precision, created_at timestamp default now())');pg.insert_rows('user1.runs',[(ds,'step3_train',accuracy)],target_fields=['ds','task','metric']);return accuracy
    @task
    def read_back(accuracy):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        print(PostgresHook(postgres_conn_id='postgres_main').get_records('SELECT * FROM user1.runs ORDER BY created_at DESC LIMIT 5'))
    read_back(train(prepare()))

step3()
