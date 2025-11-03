from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.sdv_walmart import run_sdv_job

LOCAL_TZ = "America/New_York"

with DAG(
    dag_id="walmart_sdv_daily",
    description="Train SDV on source table and write to main.walmart_sdv in MotherDuck",
    start_date=pendulum.datetime(2025, 10, 1, tz=LOCAL_TZ),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sdv", "motherduck", "walmart"],
) as dag:

    fit_and_generate = PythonOperator(
        task_id="fit_and_generate",
        python_callable=run_sdv_job,
    )

    fit_and_generate
