'''ИТОГОВЫЙ ПРОЕКТ: конвейер оттока клиентов.

Переименуйте файл в user1_p{N}_churn.py и положите в папку dags.
Заполните все TODO. N — ваш номер по журналу.

Схема:
  load_raw (ненадёжная: падает с вероятностью P_FLAKY, retries=6)
    └─> prepare (чистка + train/test -> S3 parquet)
          └─> train × МОДЕЛИ (параллельно, каждая -> запуск в MLflow)
                └─> select_best (по вашей главной метрике)
                      └─> quality_gate (чемпион + порог из паспорта)
                            └─> register_champion (только если прошла ворота)
                                  └─> top_risk (top-100 клиентов -> S3 + PostgreSQL)
'''

import os
import random
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

STUDENT = "user1"                     # ваш логин в стеке
N = 7                                 # TODO: ваш номер по журналу
BUCKET = STUDENT
PREFIX = f"project/{N}"               # всё лежит в «своей» папке

EXPERIMENT = f"user1_p{N}_churn"      # эксперимент MLflow (из части 4)
MODEL_NAME = f"user1_p{N}_model"      # модель в реестре

PRIMARY_METRIC = "roc_auc"            # TODO: roc_auc или average_precision — по паспорту
P_FLAKY = 0.35                        # TODO: вероятность сбоя load_raw — по паспорту
GATE_MARGIN = 0.01                    # TODO: порог ворот — метрика чемпиона + столько

MODELS = ["logreg", "random_forest", "tree", "knn", "extra"]   # TODO: 5-й — по варианту


def s3():
    return S3Hook(aws_conn_id="s3_local")


def pg():
    return PostgresHook(postgres_conn_id="postgres_main")


def mlflow_on():
    '''MLflow-клиент (адрес и пароль уже в окружении Airflow).'''
    import mlflow
    os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    return mlflow


@dag(dag_id=f"user1_p{N}_churn", schedule=None,
     start_date=pendulum.datetime(2026, 1, 1), catchup=False,
     tags=["project", f"user1_p{N}"], default_args={"owner": STUDENT})
def churn_pipeline():

    @task(retries=6, retry_delay=timedelta(seconds=20))
    def load_raw() -> str:
        '''Ненадёжный источник: «база продаж моргнула» — падает с вероятностью P_FLAKY.
        Airflow повторяет задачу: в Grid должна появиться оранжевая попытка.'''
        if random.random() < P_FLAKY:
            raise RuntimeError("источник не ответил (учебный временный сбой)")
        key = f"{PREFIX}/raw/customers_{N}.csv"
        # TODO: проверить, что сырой файл есть в S3 (check_for_key);
        #       если нет — загрузить его заново нельзя, значит забыт шаг из части 1
        return key

    @task
    def prepare(raw_key: str, ds=None) -> dict:
        '''Чистка (дубликаты, пропуски, выбросы, утечка!) и разбиение.
        train/test -> parquet в S3. Наружу — словарь с ключами.'''
        # TODO:
        # 1) скачать raw (hook.download_file(key=raw_key, bucket_name=BUCKET))
        # 2) убрать дубликаты customer_id
        # 3) total_charges: пропуски заполнить tenure * monthly
        # 4) monthly_charges: убрать невозможные выбросы (>118)
        # 5) ВАЖНО: исключить колонку-утечку (подсказка в части 1 задания)
        #    и бесполезные customer_id / referral_code / region
        # 6) train_test_split с размером теста ИЗ ПАСПОРТА, stratify=y
        # 7) оба куска -> parquet -> S3: {PREFIX}/{ds}/train.parquet, test.parquet
        # 8) вернуть {"train_key": ..., "test_key": ..., "rows": ...}
        ...

    @task
    def train(model_name: str, keys: dict) -> dict:
        '''Обучает одну модель, пишет запуск в MLflow (параметры, метрики, модель).
        Задачи train запускаются ПАРАЛЛЕЛЬНО.'''
        # TODO:
        # 1) скачать train/test parquet
        # 2) pipeline: OneHotEncoder для категорий + StandardScaler для чисел
        # 3) словарь моделей: logreg / random_forest / tree / knn + ваша по варианту
        # 4) обучить, посчитать roc_auc И average_precision на тесте
        # 5) mlflow: set_experiment(EXPERIMENT), start_run(run_name=model_name),
        #    log_params, log_metrics обе метрики, log_model (skops — см. практикум 2)
        # 6) вернуть {"model": model_name, PRIMARY_METRIC: значение, "run_id": ...}
        ...

    @task
    def select_best(results: list) -> dict:
        '''Ждёт все train-ы, выбирает лучшую по ВАШЕЙ метрике.'''
        # TODO: max(results, key=lambda r: r[PRIMARY_METRIC]); напечатать таблицу всех
        ...

    @task
    def quality_gate(best: dict) -> dict:
        '''Ворота качества: сравнить чемпиона и претендента.
        Возвращает решение {"promote": True/False, ...}.'''
        # TODO:
        # 1) клиент MLflow; try: get_model_version_by_alias(MODEL_NAME, "champion")
        #    except: чемпиона ещё нет -> promote=True
        # 2) метрика чемпиона: client.get_run(champ.run_id).data.metrics[PRIMARY_METRIC]
        # 3) promote = best[PRIMARY_METRIC] > champ_metric + GATE_MARGIN
        # 4) напечатать оба числа и решение; вернуть словарь с решением
        ...

    @task
    def register_champion(gate: dict, best: dict) -> None:
        '''Регистрирует новую версию и перекладывает @champion —
        только если ворота пройдены (иначе задача честно печатает «отказ» и
        завершается успехом: отказ — это нормальный исход конвейера!).'''
        # TODO: if gate["promote"]: register_model + set_registered_model_alias
        ...

    @task
    def top_risk(ds=None) -> str:
        '''Прогноз для всех клиентов текущим чемпионом (models:/...@champion).
        Top-100 по вероятности ухода -> CSV в S3 + таблица в PostgreSQL.'''
        # TODO:
        # 1) загрузить чемпиона: mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@champion")
        # 2) скачать test.parquet, predict_proba
        # 3) top-100: customer_id, вероятность — сохранить CSV в S3
        #    {PREFIX}/{ds}/top_risk.csv и вернуть ключ
        # 4) в PostgreSQL: схема user1_project, таблица top_risk
        #    (ds, customer_id, probability), insert_rows
        ...

    # порядок (заполните): load_raw -> prepare -> train[] -> select_best ->
    # quality_gate -> register_champion -> top_risk


churn_pipeline()
