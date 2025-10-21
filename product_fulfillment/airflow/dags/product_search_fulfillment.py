# dags/product_search_fulfillment.py
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests

@dag(
    dag_id="target_product_search_to_motherduck",
    description="Trigger Cloud Run for Target Product Search → then start API DAG",
    schedule="0 6 * * MON",
    start_date=datetime(2025, 10, 15),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["target", "motherduck", "product_search", "gcf"],
)
def target_product_search_to_motherduck():

    @task()
    def trigger_product_search():
        url = "https://product-search-api-to-duckdb-756516792287.us-central1.run.app"
        headers = {"Content-Type": "application/json"}
        resp = requests.get(url, headers=headers, timeout=300)
        print(resp.status_code, resp.text)
        resp.raise_for_status()
        return resp.status_code

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_target_api_dag",
        trigger_dag_id="target_api_to_motherduck",  # her DAG ID (lives in the other file)
        reset_dag_run=True,
        wait_for_completion=False,
    )

    trigger_product_search() >> trigger_next

target_product_search_to_motherduck()
