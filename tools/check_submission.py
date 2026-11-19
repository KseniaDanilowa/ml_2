"""Офлайн-проверка данных, сохранённых результатов и модели без сервисов."""
import hashlib
import json
import sys
from pathlib import Path

import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

root = Path(__file__).resolve().parents[1] / 'data-stack'
w = root / 'work/students/user1'
e = w / 'evidence'
sys.path.insert(0, str(root / 'dags'))
import churn7_core as core

raw = w / 'customers_7.csv'
assert hashlib.sha256(b'rudn2026:7:' + raw.read_bytes()).hexdigest()[:16] == 'dbe598ae595cf3d2'
for name in ['01_s3', '02_mlflow', '03_airflow', 'project_report_7']:
    book = json.loads((w / f'{name}.ipynb').read_text())
    cells = [c for c in book['cells'] if c['cell_type'] == 'code']
    assert all(c['execution_count'] is not None for c in cells), name
    assert not any(o['output_type'] == 'error' for c in cells for o in c['outputs']), name
assert '10/10' in ''.join(''.join(o.get('text', [])) for o in cells[-1]['outputs'])
train, test = core.split(core.clean(pd.read_csv(raw)))
assert (len(train), len(test)) == (5841, 1947)
model = mlflow.sklearn.load_model(str(e / 'champion'))
p = model.predict_proba(core.features(test))[:, 1]
metrics = json.loads((e / 'registry.json').read_text())['metrics']
assert np.isclose(metrics['average_precision'], average_precision_score(test.churn, p))
assert np.isclose(metrics['roc_auc'], roc_auc_score(test.churn, p))
top = test.assign(probability=p).sort_values(['probability', 'customer_id'], ascending=[False, True]).head(100)
saved = pd.read_csv(e / 'top_risk.csv')
assert top.customer_id.tolist() == saved.customer_id.tolist() and np.allclose(top.probability, saved.probability)
history = json.loads((e / 'airflow_runs.json').read_text())
assert len(history) >= 2 and all(r['state'] == 'success' for r in history)
assert any(t['task_id']=='load_raw' and t['try_number']>1 for r in history for t in r['tasks'])
print('OK: 4 ноутбука, паспорт, разбиение, 2 метрики, все 100 ID и вероятностей, DAG и retry.')
