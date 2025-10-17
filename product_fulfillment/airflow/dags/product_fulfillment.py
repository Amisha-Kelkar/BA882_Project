from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests


@dag(
    dag_id="target_api_to_motherduck",
    description="Trigger Cloud Function every 5 minutes to load Target API → MotherDuck",
    schedule="0 6 * * MON",  # at 6am on Mondays
    start_date=datetime(2025, 10, 15),  # static start date
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["target", "motherduck", "gcf"],
)
def target_api_to_motherduck():
    """Triggers a Google Cloud Function that loads Target API data into MotherDuck."""

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def trigger_cloud_function():
        url = "https://us-central1-ba882-team4.cloudfunctions.net/load_api_to_motherduck"
        headers = {"Content-Type": "application/json"}

        print(f"📡 Sending GET request to {url}")
        response = requests.get(url, headers=headers)

        print(f"Response status code: {response.status_code}")
        print(f"Response body: {response.text}")

        response.raise_for_status()  # raise exception if not 2xx
        return {"status_code": response.status_code, "body": response.text}

    return trigger_cloud_function()


target_api_to_motherduck()

