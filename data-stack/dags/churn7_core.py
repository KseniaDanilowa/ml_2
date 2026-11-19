"""Единый контракт данных для ноутбука и Airflow. Тяжёлые импорты — внутри задач."""
import io
import os

BUCKET, PREFIX = 'user1', 'project/7'
EXPERIMENT, MODEL = 'user1_p7_churn', 'user1_p7_model'
METRIC = 'average_precision'
GRID = {'LogisticRegression': ('C', [0.01, 0.5, 10]),
        'RandomForest': ('max_depth', [4, 8, 14]),
        'DecisionTree': ('max_depth', [2, 5, 10]),
        'KNN': ('n_neighbors', [5, 25, 75]),
        'ExtraTrees': ('max_depth', [5, 10, 18])}


def s3():
    import boto3
    return boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL', os.getenv('MLFLOW_S3_ENDPOINT_URL', 'http://s3:9000')))


def read(key):
    return io.BytesIO(s3().get_object(Bucket=BUCKET, Key=key)['Body'].read())


def put(key, data):
    s3().put_object(Bucket=BUCKET, Key=key, Body=data)


def pg():
    import psycopg2
    return psycopg2.connect(os.getenv('PROJECT_DSN', 'postgresql://airflow:airflow@postgres/postgres'))


def clean(raw):
    d = raw.drop_duplicates('customer_id').loc[lambda x: x.monthly_charges <= 118].copy()
    d.total_charges = d.total_charges.fillna(d.tenure_months * d.monthly_charges)
    d['churn'] = d.churn.map({'Yes': 1, 'No': 0}).astype(int)
    return d.drop(columns=['region', 'referral_code', 'cancellation_request'])


def split(d):
    from sklearn.model_selection import train_test_split
    return train_test_split(d, test_size=0.25, random_state=7, stratify=d.churn)


def features(d):
    return d.drop(columns=['customer_id', 'churn'])


def fit_run(name, value, train, test, stage, batch):
    import mlflow.sklearn
    import skops.io
    import sklearn, skops, pandas, numpy, scipy
    from sklearn.compose import ColumnTransformer
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sklearn.pipeline import Pipeline
    from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
    from sklearn.tree import DecisionTreeClassifier
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score, average_precision_score
    estimators = {'LogisticRegression': LogisticRegression(max_iter=500, random_state=7),
                  'RandomForest': RandomForestClassifier(n_estimators=100, random_state=7, n_jobs=1),
                  'ExtraTrees': ExtraTreesClassifier(n_estimators=100, random_state=7, n_jobs=1),
                  'DecisionTree': DecisionTreeClassifier(random_state=7), 'KNN': KNeighborsClassifier()}
    param = GRID[name][0]
    estimator = estimators[name].set_params(**{param: value})
    X = features(train)
    cat = X.select_dtypes(exclude='number').columns.tolist()
    prep = ColumnTransformer([('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), cat),
                              ('num', StandardScaler(), X.columns.difference(cat).tolist())])
    model = Pipeline([('prepare', prep), ('classifier', estimator)])
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name=f'{name}_{value}', tags={'stage':stage, 'batch':batch}) as run:
        model.fit(X, train.churn)
        prob = model.predict_proba(features(test))[:, 1]
        metrics = {'roc_auc': float(roc_auc_score(test.churn, prob)),
                   METRIC: float(average_precision_score(test.churn, prob))}
        mlflow.log_params({'family': name, 'split_seed': 7, 'test_size': 0.25, **estimator.get_params()})
        mlflow.log_metrics(metrics)
        # Типы известны из собственного, только что обученного объекта (не из чужого файла).
        trusted = skops.io.get_untrusted_types(data=skops.io.dumps(model))
        info = mlflow.sklearn.log_model(model, name='model', skops_trusted_types=trusted,
            pip_requirements=[f'{p}=={m.__version__}' for p,m in [('scikit-learn',sklearn),('skops',skops),('pandas',pandas),('numpy',numpy),('scipy',scipy)]])
        return {'family':name,'value':value,'run_id':run.info.run_id,'uri':info.model_uri, **metrics}


def gate(candidate, alias='champion', registry=MODEL):
    from mlflow import MlflowClient
    from mlflow.exceptions import MlflowException
    c = MlflowClient()
    try:
        v = c.get_model_version_by_alias(registry, alias)
    except MlflowException as exc:
        if exc.error_code != 'RESOURCE_DOES_NOT_EXIST' and not (exc.error_code == 'INVALID_PARAMETER_VALUE' and 'alias' in str(exc).lower() and 'not found' in str(exc).lower()):
            raise
        return {'previous':None, 'threshold':None, 'candidate':candidate[METRIC], 'promote':True}
    old = c.get_run(v.run_id).data.metrics[METRIC]
    return {'previous':old,'threshold':old+0.01,'candidate':candidate[METRIC],'promote':candidate[METRIC] > old+0.01}


def register(best, decision, alias='champion', registry=MODEL):
    import mlflow
    c = mlflow.MlflowClient()
    if decision['promote']:
        v = mlflow.register_model(best['uri'], registry)
        c.set_registered_model_alias(registry, alias, v.version)
    return c.get_model_version_by_alias(registry, alias).version
