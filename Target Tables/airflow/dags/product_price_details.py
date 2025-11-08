# dags/product_fulfillment.py
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests

@dag(
    dag_id="target_price_details_to_motherduck",
    description="Trigger Cloud Function to load Target API → MotherDuck",
    schedule=None,  # runs only when triggered by your DAG
    start_date=datetime(2025, 10, 15),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["target", "motherduck", "api", "gcf"],
)
def target_price_details_to_motherduck():

    @task()
    def trigger_price_function():
        url = "https://us-central1-ba882-team4.cloudfunctions.net/target_product_price_details_to_motherduck"
        headers = {"Content-Type": "application/json"}
        resp = requests.get(url, headers=headers, timeout=300)
        print(resp.status_code, resp.text)
        resp.raise_for_status()
        return resp.status_code

    trigger_price_function()

target_price_details_to_motherduck()
