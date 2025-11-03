#!/usr/bin/env bash
set -euxo pipefail

echo ">>> Installing deps (no torch/ctgan) ..."
pip install --no-cache-dir \
  apache-airflow==2.10.3 \
  apache-airflow-providers-postgres==6.3.0 \
  psycopg2-binary>=2.9.9 \
  duckdb>=1.1.3 \
  pandas>=2.2 \
  pyarrow>=17.0.0 \
  rdt>=1.8.0 \
  copulas>=0.12.1 \
  sdmetrics>=0.14.0

echo ">>> Installing sdv core without heavy deps ..."
pip install --no-cache-dir sdv==1.14.0 --no-deps

echo ">>> Init Airflow DB (Postgres) ..."
# 防止半初始化残留
airflow db reset -y || true
airflow db init

echo ">>> Create admin user (idempotent) ..."
airflow users create \
  --role Admin \
  --username admin \
  --password admin \
  --firstname a --lastname a --email a@a.a || true

echo ">>> Start scheduler (background) ..."
airflow scheduler &

echo ">>> Start webserver (foreground) ..."
exec airflow webserver --port 8080
