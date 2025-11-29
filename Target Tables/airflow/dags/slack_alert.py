# dags/slack_alert.py
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests

@dag(
    dag_id="slack_alert",
    description="Trigger Cloud Function to send stockout alerts to Slack",
    schedule=None,  # runs only when triggered by your DAG
    start_date=datetime(2025, 10, 15),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["target", "motherduck", "api", "gcf","slack"],
)
def slack_alert():

    @task()
    def trigger_api_function():
        url = "https://us-central1-ba882-team4.cloudfunctions.net/slack-bot?run=alert"
        headers = {"Content-Type": "application/json"}
        resp = requests.get(url, headers=headers, timeout=300)
        print(resp.status_code, resp.text)
        resp.raise_for_status()
        return resp.status_code

    trigger_api_function()

slack_alert()
