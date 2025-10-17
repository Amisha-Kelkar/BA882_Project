from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests

@dag(
    dag_id="target_product_search_to_motherduck",
    description="Trigger Cloud Function weekly to load Target Product Search data → MotherDuck",
    schedule="0 6 * * MON",  # every Monday at 6 AM
    start_date=datetime(2025, 10, 15),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["target", "motherduck", "product_search", "gcf"],
)
def target_product_search_to_motherduck():
    """Triggers a Google Cloud Function that loads Target product_search data into MotherDuck."""

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def trigger_cloud_function():
        # Replace this with your actual Cloud Function URL
        url = "https://product-search-api-to-duckdb-756516792287.us-central1.run.app"
        headers = {"Content-Type": "application/json"}

        print(f"📡 Sending GET request to {url}")
        response = requests.get(url, headers=headers)

        print(f"Response status code: {response.status_code}")
        print(f"Response body: {response.text}")

        response.raise_for_status()  # Fail the task if response is not 2xx
        return {"status_code": response.status_code, "body": response.text}

    return trigger_cloud_function()


target_product_search_to_motherduck()
