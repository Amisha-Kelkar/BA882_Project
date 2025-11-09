from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests

@dag(
    dag_id="target_product_search_to_motherduck",
    description="Trigger Cloud Function for Target Product Search → then start API DAG",
    schedule="0 6 * * MON",  # every Monday 6AM UTC
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
        url = "https://us-central1-ba882-team4.cloudfunctions.net/load_product_search_to_motherduck"
        headers = {"Content-Type": "application/json"}

        try:
            resp = requests.get(url, headers=headers, timeout=540)  # match GCF timeout
            print(f"Status: {resp.status_code}\nResponse: {resp.text[:300]}...")
            resp.raise_for_status()
            return resp.status_code
        except Exception as e:
            print(f"❌ Error calling Cloud Function: {e}")
            raise

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_target_api_dag",
        trigger_dag_id="target_api_to_motherduck",  # next DAG in your pipeline
        reset_dag_run=True,
        wait_for_completion=False,
    )

    trigger_price_details = TriggerDagRunOperator(
        task_id="trigger_target_price_details_dag",
        trigger_dag_id="target_price_details_to_motherduck",  # next DAG in your pipeline
        reset_dag_run=True,
        wait_for_completion=False,
    )

    trigger_silver_layer = TriggerDagRunOperator(
        task_id="trigger_silver_layer_dag",
        trigger_dag_id="silver_layer_to_motherduck",  # next DAG in your pipeline
        reset_dag_run=True,
        wait_for_completion=False,
    )

        
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_layer_dag",
        trigger_dag_id="gold_layer_to_motherduck",
        reset_dag_run=True,
        wait_for_completion=False,
    )
    
    trigger_product_search() >> trigger_next >> trigger_price_details >> trigger_silver_layer >> trigger_gold


target_product_search_to_motherduck()
