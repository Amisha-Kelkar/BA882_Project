# dags/gold_layer.py
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests

@dag(
    dag_id="gold_layer_to_motherduck",
    description="Trigger Cloud Function to build Gold layer after Silver",
    schedule=None,   # only runs when triggered by silver DAG
    start_date=datetime(2025, 10, 15),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["target", "motherduck", "gold", "gcf"],
)
def gold_layer_to_motherduck():

    @task()
    def trigger_gold_layer():
        url = "https://us-central1-ba882-team4.cloudfunctions.net/load_gold_layer"
        headers = {"Content-Type": "application/json"}
        resp = requests.get(url, headers=headers, timeout=540)
        print(f"Status: {resp.status_code}\nResponse: {resp.text[:300]}...")
        resp.raise_for_status()
        return resp.status_code

    trigger_gold_layer()

gold_layer_to_motherduck()
