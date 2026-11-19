import pendulum
from airflow.decorators import dag, task
from datetime import timedelta

@dag(dag_id='user1_step4',schedule=None,start_date=pendulum.datetime(2026,1,1),catchup=False,tags=['user1'])
def step4():
    @task
    def prepare(ds=None):
        from sklearn.datasets import load_breast_cancer
        from sklearn.model_selection import train_test_split
        from churn7_core import put
        d=load_breast_cancer(as_frame=True).frame;a,b=train_test_split(d,test_size=.25,stratify=d.target,random_state=42);prefix=f'airflow/{ds}/step4/'
        for k,v in [('train',a),('test',b)]:put(prefix+k+'.parquet',v.to_parquet(index=False))
        return {'train':prefix+'train.parquet','test':prefix+'test.parquet','prefix':prefix}
    @task
    def train(keys):
        import io,json,joblib,pandas as pd
        from churn7_core import read,put
        from sklearn.preprocessing import StandardScaler
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import make_pipeline
        from sklearn.metrics import roc_auc_score,ConfusionMatrixDisplay
        a,b=[pd.read_parquet(read(keys[k])) for k in ('train','test')]; x=b.drop(columns='target');m=make_pipeline(StandardScaler(),LogisticRegression(max_iter=500)).fit(a.drop(columns='target'),a.target)
        buf=io.BytesIO();joblib.dump(m,buf);prefix=keys['prefix'];put(prefix+'model.joblib',buf.getvalue());put(prefix+'metrics.json',json.dumps({'accuracy':m.score(x,b.target),'roc_auc':roc_auc_score(b.target,m.predict_proba(x)[:,1])}).encode());fig=ConfusionMatrixDisplay.from_predictions(b.target,m.predict(x)).figure_; buf=io.BytesIO();fig.savefig(buf,format='png');put(prefix+'confusion_matrix.png',buf.getvalue());return keys
    @task
    def predict(keys):
        import joblib,pandas as pd
        from churn7_core import read
        m=joblib.load(read(keys['prefix']+'model.joblib'));pred=m.predict(pd.read_parquet(read(keys['test'])).drop(columns='target').head(5)).tolist(); print(pred);return pred
    predict(train(prepare()))

step4()
