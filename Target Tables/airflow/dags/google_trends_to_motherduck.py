from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# === Cloud Function endpoint (your deployed function) ===
CLOUD_FUNCTION_URL = "https://fetch-christmas-decor-trends-756516792287.us-central1.run.app"

# === Default DAG arguments ===
default_args = {
    "owner": "airflow",
    "retries": 1,  # Retry once if it fails
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes before retry
}

# === Core callable function ===
def trigger_google_trends_cf(**_):
    """
    Trigger the Google Cloud Function that fetches Google Trends data
    for 'Christmas Decor' (rolling 52 weeks) and loads it into MotherDuck.
    """
    print(f"Invoking Cloud Function: {CLOUD_FUNCTION_URL}")
    try:
        resp = requests.post(CLOUD_FUNCTION_URL, timeout=90)
        print(f"Cloud Function status code: {resp.status_code}")
        print(f"Cloud Function response body: {resp.text}")

        # Raise error if failed
        if resp.status_code != 200:
            raise RuntimeError(f"Cloud Function failed with status {resp.status_code}: {resp.text}")

        print("✅ Cloud Function triggered successfully.")
    except Exception as e:
        print(f"❌ Error during Cloud Function call: {e}")
        raise

# === DAG definition ===
with DAG(
    dag_id="google_trends_to_motherduck",
    default_args=default_args,
    description="Load Google Trends (Christmas Decor) data into MotherDuck raw table",
    start_date=datetime(2025, 11, 3),  # Earliest start date (no backfill)
    schedule_interval="0 6 * * 1",  # Every Monday at 06:00
    catchup=False,
    tags=["google_trends", "motherduck", "gcf"],
) as dag:

    # Define PythonOperator task
    call_google_trends_cf = PythonOperator(
        task_id="trigger_google_trends_cloud_function",
        python_callable=trigger_google_trends_cf,
    )

    # Set task execution order (only one here)
    call_google_trends_cf
