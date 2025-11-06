from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


CLOUD_FUNCTION_URL = "https://fetch-christmas-decor-trends-756516792287.us-central1.run.app"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def trigger_google_trends_cf(**_):
    """
    Trigger the Google Cloud Function that fetches yearly (12-month) Google Trends data
    and loads it into the MotherDuck 'raw_google_trends_christmas_decor' table.
    """
    print(f"Invoking Cloud Function: {CLOUD_FUNCTION_URL}")
    try:
        resp = requests.post(CLOUD_FUNCTION_URL, timeout=90)
        print(f"Cloud Function status code: {resp.status_code}")
        print(f"Cloud Function response body: {resp.text}")

        if resp.status_code != 200:
            raise RuntimeError(f"Cloud Function failed with status {resp.status_code}: {resp.text}")

        print(" Cloud Function triggered successfully.")
    except Exception as e:
        print(f" Error during Cloud Function call: {e}")
        raise


with DAG(
    dag_id="google_trends_to_motherduck",
    default_args=default_args,
    description="Fetch 12-month Google Trends data daily and load to MotherDuck",
    start_date=datetime(2025, 11, 3),
    schedule_interval="@daily",  
    catchup=False,
    tags=["google_trends", "motherduck", "gcf"],
) as dag:

    call_google_trends_cf = PythonOperator(
        task_id="trigger_google_trends_cloud_function",
        python_callable=trigger_google_trends_cf,
    )

    call_google_trends_cf

