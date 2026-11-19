import pendulum
from airflow.decorators import dag, task
from datetime import timedelta

@dag(dag_id='user1_step2',schedule=None,start_date=pendulum.datetime(2026,1,1),catchup=False,tags=['user1'])
def step2():
    @task
    def prepare():
        from sklearn.datasets import load_iris
        from sklearn.model_selection import train_test_split
        d=load_iris(as_frame=True).frame
        a,b=train_test_split(d,test_size=.25,stratify=d.target,random_state=42)
        from churn7_core import put
        for k,v in [('train',a),('test',b)]:put('practice/iris/'+k+'.parquet',v.to_parquet(index=False))
        return {'train':'practice/iris/train.parquet','test':'practice/iris/test.parquet'}
    @task
    def train(name,keys):
        import random,pandas as pd
        from churn7_core import read
        from sklearn.neighbors import KNeighborsClassifier
        from sklearn.tree import DecisionTreeClassifier
        from sklearn.linear_model import LogisticRegression
        if name=='tree' and random.random()<.3:raise RuntimeError('Временный сбой дерева')
        a,b=[pd.read_parquet(read(keys[k])) for k in ('train','test')]
        m={'logreg':LogisticRegression(max_iter=500),'knn':KNeighborsClassifier(),'tree':DecisionTreeClassifier(random_state=42)}[name]
        m.fit(a.drop(columns='target'),a.target)
        return {'model':name,'accuracy':m.score(b.drop(columns='target'),b.target)}
    @task
    def select_best(results):return max(results,key=lambda r:r['accuracy'])
    keys=prepare();select_best([train.override(task_id='train_'+k,retries=3 if k=='tree' else 0,retry_delay=timedelta(seconds=10))(k,keys) for k in ['logreg','knn','tree']])

step2()
