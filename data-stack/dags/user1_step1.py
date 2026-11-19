import pendulum
from airflow.decorators import dag, task
from datetime import timedelta

@dag(dag_id='user1_step1',schedule=None,start_date=pendulum.datetime(2026,1,1),catchup=False,tags=['user1'])
def step1():
    @task
    def extract():
        from sklearn.datasets import load_wine
        df=load_wine(as_frame=True).data
        return {'rows':len(df),'means':df.mean().to_dict()}
    @task
    def transform(info):
        name=max(info['means'],key=info['means'].get)
        return {'rows':info['rows'],'column':name,'mean':info['means'][name]}
    @task
    def load(report):
        print(report);return report
    load(transform(extract()))

step1()
