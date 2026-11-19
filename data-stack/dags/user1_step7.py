import pendulum
from airflow.decorators import dag, task
from datetime import timedelta

@dag(dag_id='user1_step7',schedule='*/10 * * * *',start_date=pendulum.datetime(2026,1,1),catchup=False,tags=['user1'])
def step7():
    @task
    def heartbeat(ts=None):
        from churn7_core import put
        key='airflow/heartbeat/'+ts.replace(':','')+'.txt';put(key,f'пульс {ts}'.encode());return key
    heartbeat()

step7()
