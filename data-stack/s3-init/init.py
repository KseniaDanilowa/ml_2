"""Однократная настройка S3 (RustFS): создаёт бакеты, пока не появится сервер."""
import os
import sys
import time

import boto3
from botocore.config import Config

endpoint = os.environ["S3_ENDPOINT"]
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    config=Config(retries={"max_attempts": 2}, connect_timeout=3, read_timeout=5),
)

# ждём, пока RustFS поднимется (healthcheck уже прошёл, но на всякий случай)
for attempt in range(20):
    try:
        s3.list_buckets()
        break
    except Exception as e:
        print(f"ждём S3 ({attempt + 1}/20): {e}")
        time.sleep(3)
else:
    sys.exit("S3 не ответил за 60 секунд")

buckets = ["students", "mlflow", "airflow-logs"] + [f"user{i}" for i in range(1, 31)]
existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
for name in buckets:
    if name not in existing:
        s3.create_bucket(Bucket=name)
        print("создан бакет", name)

print("=== S3 init готово. Бакетов:", len(buckets), "===")
