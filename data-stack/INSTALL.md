# УСТАНОВКА: все инструменты проекта через Docker

Эта инструкция — шаг за шагом поднимает на **вашем компьютере** полный стек
MLOps: PostgreSQL + S3 (RustFS) + MLflow + Airflow + Jupyter. Ничего, кроме
Docker, устанавливать не нужно — всё остальное живёт в контейнерах.

Подойдёт для macOS (включая M1/M2/M3), Windows 10/11 и Linux.
Нужны: **8+ ГБ оперативной памяти, ~10 ГБ на диске, интернет** (только для
скачивания образов при первой сборке, дальше — без сети).

---

## Шаг 1. Установите Docker Desktop

### macOS (Apple Silicon M1/M2/M3 или Intel)
1. Скачайте установщик: https://www.docker.com/products/docker-desktop/ →
   «Docker Desktop for Mac» (Apple Silicon — чип, Intel — если Mac до 2020 г.).
2. Откройте скачанный `.dmg` → перетащите Docker в Applications → запустите.
3. Разрешите привилегии (спросит пароль) и примите условия.

> На Mac есть занятный нюанс: порт 5000 занимает системная служба AirPlay,
> поэтому MLflow в нашем стеке доступен на порту **5001** — это уже учтено,
> ничего делать не надо, просто запомните адрес.

### Windows 10/11
1. Скачайте «Docker Desktop for Windows»: https://www.docker.com/products/docker-desktop/
2. При установке оставьте галочку **Use WSL 2 instead of Hyper-V**.
3. Если попросит — включите функцию WSL2 и перезагрузитесь
   (в PowerShell от администратора: `wsl --install`).
4. Запустите Docker Desktop из меню Пуск.

### Linux (Ubuntu/Debian)
Docker Desktop не обязателен, достаточно движка:

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER   # чтобы работать без sudo
newgrp docker                   # применяем группу без перелогина
sudo apt-get install -y docker-compose-plugin   # команда docker compose
```

## Шаг 2. Проверьте, что Docker работает

Откройте терминал (macOS — Терминал, Windows — PowerShell) и выполните:

```bash
docker version          # должны показаться Client и Server
docker run hello-world  # скачает крошечный образ и поздоровается
```

Если вместо Server — ошибка «daemon not running»: запустите приложение
**Docker Desktop** и подождите, пока в его меню закрепится зелёный статус
«Engine running».

## Шаг 3. Выделите Docker память

Стек из 6 сервисов требует ~4–6 ГБ. Откройте Docker Desktop →
Settings (шестерёнка) → **Resources**:

- macOS: Memory → **6 ГБ** (на Apple Silicon можно оставить 8).
- Windows (WSL2): память настраивается файлом `%UserProfile%\.wslconfig`:
  ```
  [wsl2]
  memory=6GB
  ```
  после сохранения выполните в PowerShell `wsl --shutdown` и запустите Docker заново.

Примените настройки (Apply & Restart).

## Шаг 4. Скопируйте папку проекта

Получите у преподавателя папку **`data-stack`** (zip) и распакуйте в удобное
место, например:

- macOS: `~/Desktop/data-stack`
- Windows: `C:\\Users\\<вы>\\Desktop\\data-stack`
- Linux: `~/data-stack`

Внутри: `docker-compose.yml`, три `Dockerfile.*`, папки `dags/`, `project/`,
`s3-init/`, `postgres-init/`, `work/`. Ничего не переименовывайте.

## Шаг 5. Запустите стек (первый раз — 10–20 минут)

В терминале перейдите в папку и запустите:

```bash
cd путь/к/data-stack
docker compose up -d
```

При первом запуске Docker **собирает три образа** (Airflow, MLflow, Jupyter):
в терминале побегут строки загрузки — это нормально, просто ждите.
Дальше запуски будут занимать ~30 секунд, пересборка не нужна.

Следить за процессом удобно второй командой в другом окне терминала:

```bash
docker compose ps       # статусы сервисов
docker compose logs -f  # общий лог; Ctrl+C — выйти
```

Готово, когда все сервисы `Up`, а у большинства — пометка `(healthy)`
(занимает 1–3 минуты после старта):

```
SERVICE             STATUS
airflow-scheduler   Up
airflow-webserver   Up (healthy)
jupyter             Up (healthy)
mlflow              Up (healthy)
postgres            Up (healthy)
s3                  Up (healthy)
```

## Шаг 6. Проверьте все пять интерфейсов

Откройте в браузере (вход — в таблице; если страница не открывается, подождите
минуту и обновите):

| Инструмент | Адрес | Логин / пароль |
|---|---|---|
| Jupyter (здесь работаете) | http://localhost:8888 | без входа |
| Airflow | http://localhost:8080 | `student` / `student` |
| MLflow | http://localhost:5001 | без входа |
| S3-консоль (файлы глазами) | http://localhost:9001 | `user1` / `12345678` |
| PostgreSQL | внутри стека: `postgres:5432` | `airflow` / `airflow` |

Быстрая проверка одной командой (из папки data-stack):

```bash
curl -s localhost:5001/health && echo " MLflow OK"     # должно напечатать: OK MLflow OK
docker compose exec airflow-scheduler airflow dags list | head -5
```

## Шаг 7. Начало работы

Откройте Jupyter → папка `work/students/user1/` — там практикумы и шаблон
итогового проекта (задание — `project/README_project.md`, шаблон отчёта —
`project/report_template.ipynb`: скопируйте его в свою папку и работайте в копии).

Папка `dags/` общая для Jupyter и Airflow: файл DAG, сохранённый туда из
Jupyter, Airflow подхватит сам за ~10 секунд.

---

## Ежедневные команды

| Что делаем | Команда (из папки data-stack) |
|---|---|
| поднять стек | `docker compose up -d` |
| статус сервисов | `docker compose ps` |
| остановить (данные сохранятся) | `docker compose down` |
| логи одного сервиса | `docker compose logs -f mlflow` |
| перезапустить один сервис | `docker compose restart airflow-scheduler` |
| полный сброс (стереть ВСЁ: бакеты, базу, запуски) | `docker compose down -v` |

> «Всё» = бакеты S3 с вашими данными, эксперименты MLflow, DAG-раны Airflow,
> таблицы PostgreSQL. Применяйте `down -v` только если хотите начать с чистого
> листа (после этого шаг 5 пройдёт заново, но уже быстро).

## Обновление, если преподаватель выдал новую версию папки

```bash
docker compose down          # остановить
# замените файлы папки (кроме work/ — там ваши ноутбуки)
docker compose build         # пересобрать образы, если менялись
docker compose up -d
```

## Типовые проблемы

| Симптом | Причина и решение |
|---|---|
| `Cannot connect to the Docker daemon` | Не запущен Docker Desktop — запустите и дождитесь зелёного статуса |
| Сборка падает на скачивании пакетов | Слабый интернет или VPN — отключите VPN, запустите `docker compose build` заново (продолжит с места падения) |
| `port is already allocated` (8080/8888/9000) | Порт занят другой программой. Закройте её, либо поменяйте левую цифру в `ports:` нужного сервиса в `docker-compose.yml` (например `"8081:8080"`) |
| Сервисы постоянно перезапускаются | Не хватило памяти — вернитесь к шагу 3 и повысьте лимит |
| Страница MLflow не открывается на `:5000` | Так и должно быть: на Mac порт 5000 занят AirPlay, MLflow живёт на **5001** |
| Airflow показывает Import Errors на DAG | Опечатка в вашем файле в `dags/` — откройте ошибку в интерфейсе, там номер строки |
| После `down` данные исчезли | Так не бывает: данные в томах Docker. Проверьте, что запустили `up -d` из той же папки |
| Windows: `wsl --status` ругается | Обновите WSL: `wsl --update`, перезагрузите компьютер |

Если проблема не решается — приложите к вопросу вывод `docker compose ps` и
`docker compose logs <сервис>`, так помогается в разы быстрее.
