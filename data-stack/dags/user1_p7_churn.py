"""Вариант 7: ручное обновление чемпиона TeleData."""
import random
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
import churn7_core as core


@dag(dag_id='user1_p7_churn', schedule=None, start_date=pendulum.datetime(2026, 1, 1),
     catchup=False, max_active_runs=1, tags=['user1', 'churn'], default_args={'owner': 'user1'})
def pipeline():
    @task(retries=6, retry_delay=timedelta(seconds=30))
    def load_raw():
        if random.random() < 0.35:
            raise RuntimeError('Временный сбой учебного источника')
        key = f'{core.PREFIX}/raw/customers_7.csv'
        core.s3().head_object(Bucket=core.BUCKET, Key=key)
        return key

    @task(multiple_outputs=False)
    def prepare(key, ds=None, run_id=None):
        import pandas as pd
        train, test = core.split(core.clean(pd.read_csv(core.read(key))))
        prefix = f'{core.PREFIX}/{ds}/{run_id}'
        for name, subset in [('train',train),('test',test)]:
            core.put(f'{prefix}/{name}.parquet', subset.to_parquet(index=False))
        return {'train':f'{prefix}/train.parquet','test':f'{prefix}/test.parquet', 'prefix':prefix}

    @task
    def train_model(family, keys, run_id=None, dag_run=None):
        import pandas as pd
        # При отсутствии параметров из сетки берём среднюю конфигурацию семейства.
        value = dag_run.conf.get('params_by_family', {}).get(family, core.GRID[family][1][1])
        if value not in core.GRID[family][1]:
            raise ValueError('Параметр вне сетки')
        return core.fit_run(family, value, pd.read_parquet(core.read(keys['train'])),
                            pd.read_parquet(core.read(keys['test'])), 'dag', run_id)

    @task
    def select_best(results):
        print(sorted(results, key=lambda x:-x[core.METRIC]))
        return max(results, key=lambda x:x[core.METRIC])

    @task
    def quality_gate(best):
        result = core.gate(best)
        print(result)
        return result

    @task
    def register_champion(best, decision):
        return core.register(best, decision)

    @task
    def top_risk(keys, version, ds=None):
        import mlflow.sklearn
        import pandas as pd
        from contextlib import closing
        from psycopg2.extras import execute_values
        test = pd.read_parquet(core.read(keys['test']))
        model = mlflow.sklearn.load_model(f'models:/{core.MODEL}@champion')
        probs = model.predict_proba(core.features(test))[:,1]
        top = test[['customer_id']].assign(probability=probs).sort_values(
            ['probability','customer_id'], ascending=[False,True]).head(100)
        key = keys['prefix']+'/top_risk.csv'
        core.put(key, top.to_csv(index=False).encode())
        with closing(core.pg()) as con, con, con.cursor() as cur:
            cur.execute('CREATE SCHEMA IF NOT EXISTS user1_project')
            cur.execute('''CREATE TABLE IF NOT EXISTS user1_project.top_risk
                (ds date, customer_id text, probability double precision NOT NULL,
                 PRIMARY KEY(ds,customer_id))''')
            cur.execute('DELETE FROM user1_project.top_risk WHERE ds=%s', (ds,))
            execute_values(cur,'INSERT INTO user1_project.top_risk VALUES %s',
                           [(ds,i,p) for i,p in top.itertuples(index=False)])
        print(f'Версия {version}; {len(top)} строк; {key}')
        return key

    keys = prepare(load_raw())
    best = select_best([train_model.override(task_id='train_'+name)(name, keys) for name in core.GRID])
    top_risk(keys, register_champion(best, quality_gate(best)))


churn_dag = pipeline()
