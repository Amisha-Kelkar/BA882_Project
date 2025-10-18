from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests

# ============ YOUR DAG ============
@dag(
    dag_id="target_product_search_to_motherduck",
    description="Trigger Cloud Run for Target Product Search → then start API DAG",
    schedule="0 6 * * MON",
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

    @task()
    def trigger_product_search():
        url = "https://product-search-api-to-duckdb-756516792287.us-central1.run.app"
        headers = {"Content-Type": "application/json"}
        print(f"📡 Calling {url}")
        response = requests.get(url, headers=headers)
        print(response.status_code, response.text)
        response.raise_for_status()
        return response.status_code

    # ✅ Trigger her DAG programmatically once yours finishes
    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_target_api_dag",
        trigger_dag_id="target_api_to_motherduck",  # her DAG id
        reset_dag_run=True,
        wait_for_completion=False,
    )

    trigger_product_search() >> trigger_next_dag


# ============ HER DAG ============
@dag(
    dag_id="target_api_to_motherduck",
    description="Trigger Cloud Function to load Target API → MotherDuck",
    schedule=None,  # runs only when triggered
    start_date=datetime(2025, 10, 15),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["target", "motherduck", "api", "gcf"],
)
def target_api_to_motherduck():

    @task()
    def trigger_api_function():
        url = "https://us-central1-ba882-team4.cloudfunctions.net/load_api_to_motherduck"
        headers = {"Content-Type": "application/json"}
        print(f"📡 Calling {url}")
        response = requests.get(url, headers=headers)
        print(response.status_code, response.text)
        response.raise_for_status()
        return response.status_code

    trigger_api_function()


# Instantiate both DAGs
product_dag = target_product_search_to_motherduck()
api_dag = target_api_to_motherduck()

