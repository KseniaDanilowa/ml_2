# Локальный MLOps-стек для MacBook

Личная копия учебного сервера (10.162.1.86): **PostgreSQL + S3 (RustFS) + MLflow + Airflow + Jupyter**
в Docker. Подходит для запуска всех трёх практикумов (`01_s3`, `02_mlflow`, `03_airflow`)
и **итогового проекта** (см. папку `project/`). Эту же папку выдают студентам: она
самодостаточна — поднимается на любом компьютере с Docker Desktop.

> **Установка с нуля (Docker + запуск стека) — пошагово в [`INSTALL.md`](INSTALL.md)**:
> macOS / Windows / Linux, настройка памяти, проверка интерфейсов, типовые проблемы.

## Запуск и остановка

```bash
cd ~/Desktop/airflow/data-stack
docker compose up -d          # поднять всё (первый раз — ~2 мин, образы уже собраны)
docker compose ps             # статус: все сервисы должны быть Up / healthy
docker compose down           # остановить (данные в volumes сохранятся)
docker compose down -v        # остановить и стереть все данные
```

Нужен запущенный Docker Desktop. Первая сборка уже выполнена; после `down` пересборка не требуется.

## Адреса и пароли

| Сервис | Адрес | Логин | Пароль |
|---|---|---|---|
| Jupyter | http://localhost:8888 | — (токена нет) | — |
| Airflow | http://localhost:8080 | `student` | `student` |
| MLflow | **http://localhost:5001** | без входа | — |
| S3 API | http://localhost:9000 | `user1` | `12345678` |
| S3-консоль | http://localhost:9001 | `user1` | `12345678` |
| PostgreSQL | `localhost:5432` (из docker-сети `postgres:5432`) | `airflow` | `airflow` |

> Порт 5000 на Mac занят AirPlay Receiver, поэтому MLflow снаружи — на **5001**
> (внутри сети контейнеров — как на сервере, `http://mlflow:5000`).

## Где что лежит

```
data-stack/
├── dags/                  # DAG-и: эту папку читает Airflow и видит Jupyter (work/dags)
│   └── local_smoke.py     # проверочный DAG: S3 + PostgreSQL + MLflow за один запуск
├── project/               # ИТОГОВЫЙ ПРОЕКТ (выдаётся студентам вместе со стеком)
│   ├── README_project.md  # задание: 6 частей, критерии, персональные варианты
│   ├── make_dataset.py    # генератор персональных датасетов «отток клиентов»
│   ├── dag_template.py    # скелет DAG проекта с TODO
│   ├── report_template.ipynb  # шаблон отчёта с паспортом и самопроверкой
│   └── teacher_guide.md   # методичка преподавателя (проверка, антиплагиат)
├── work/students/user1/   # локальные версии практикумов *_local.ipynb (+ решения)
├── postgres-init/init.sql # при первом старте создаёт базу mlflow
├── s3-init/init.py        # при первом старте создаёт 33 бакета (user1..user30 и служебные)
├── docker-compose.yml
└── Dockerfile.*           # образы airflow / mlflow / jupyter
```

Ноутбуки: в `work/students/user1/` лежат **локальные версии** (`01_s3_local.ipynb`,
`02_mlflow_local.ipynb`, `03_airflow_local.ipynb` и их `*_solutions_local`) — адреса заменены
на localhost, вход `user1/12345678`, MLflow без пароля на `:5001`. Серверные оригиналы —
в `~/Desktop/airflow/`. Все три локальных ноутбука выполнены от начала до конца (outputs внутри)
на этом стеке; DAG `user1_full` из раздела 9 прошёл целиком: S3 → две модели параллельно →
MLflow → реестр с меткой `@champion`.

Данные (бакеты, база, логи) — в docker-volumes `rustfs-data`, `postgres-data`, `airflow-logs`;
при `docker compose down` они остаются.

## Быстрая проверка, что всё живое

1. **Jupyter** http://localhost:8888 → папка `work/students/user1` → ноутбуки на месте.
2. **Airflow** http://localhost:8080 (student/student) → DAG `local_smoke` → ▶ → все 4 задачи
   зеленеют (он проверяет S3, PostgreSQL и MLflow разом).
3. **MLflow** http://localhost:5001 → эксперимент `local_smoke` → запуск `smoke` → артефакт `probe.json`.
4. **S3-консоль** http://localhost:9001 (user1/12345678) → бакеты `user1`, `mlflow`, `airflow-logs`.

## Отличия от учебного сервера

| На сервере | Локально |
|---|---|
| MLflow: `http://10.162.1.86:5000`, вход по паролю | `http://localhost:5001`, без входа |
| S3 — RustFS, у каждого студента свой ключ `user1..user30` | Тоже RustFS, но один общий ключ `user1/12345678` |
| «Чужой» бакет даёт `AccessDenied` (ячейка в 01_s3) | Все бакеты открыты одним ключом — демонстрация AccessDenied не сработает |
| Веб-консоль S3 на `:9001/rustfs/...` | Консоль RustFS на `:9001` |
| Подключения `s3_local`, `postgres_main` создаёт админ | Уже вшиты в контейнер через переменные окружения |
| Пароль MLflow для DAG-ов — Airflow Variable | Уже задан: `user1_mlflow_password` (Variable из env) |

Мелочи, которые стоит знать, прогоняя ноутбуки локально:

- **01_s3, presigned-ссылка (раздел 8.3):** в ячейке внешний адрес сервера `http://10.162.1.86:9000` —
  замените на `http://localhost:9000`.
- **02_mlflow:** ноутбук печатает ссылки вида `http://10.162.1.86:5000/...` — локально открывайте
  тот же путь на `http://localhost:5001`.
- **03_airflow:** всё работает как написано: папка `dags` общая, DAG-и подхватываются за ~10 секунд,
  кнопка ▶ запускает. Ваш логин локально всегда `user1`.

## Что внутри образов

- **airflow** (`Dockerfile.airflow`): Airflow 2.11 + провайдеры amazon/postgres + sklearn,
  pandas, matplotlib, mlflow-skinny 3.16.1 — всё, что нужно задачам DAG-ов.
- **jupyter** (`Dockerfile.jupyter`): scipy-notebook + boto3, s3fs, mlflow-skinny, skops.
- **mlflow** (`Dockerfile.mlflow`): MLflow 3.16.1; метаданные в PostgreSQL (база `mlflow`),
  артефакты в S3 (бакет `mlflow`), `--allowed-hosts "*"` — иначе он режет заголовок `Host: mlflow`.
- **s3**: RustFS — то же хранилище, что на учебном сервере. MinIO не подходит: образы удалены
  из Docker Hub и закрыты на quay.io.
