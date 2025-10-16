from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import os

# Cloud Function URL (env var or fallback)
CLOUD_FUNCTION_URL = os.getenv(
    "CLOUD_FUNCTION_URL",
    "https://us-central1-ba882-team4.cloudfunctions.net/load_api_to_motherduck",
)

default_args = {"owner": "airflow", "depends_on_past": False}

def _print_context(**context):
    print("DAG triggered successfully!")

# Create the DAG object first (so Airflow always registers it)
dag = DAG(
    dag_id="target_api_to_motherduck_weekly",
    default_args=default_args,
    schedule="0/5 * * * *",  # every Monday 6AM UTC
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["target", "motherduck"],
)

# Define task inside the DAG context
trigger_function = SimpleHttpOperator(
    task_id="trigger_cloud_function",
    method="GET",
    endpoint="https://us-central1-ba882-team4.cloudfunctions.net/load_api_to_motherduck",
    http_conn_id=None,  # optional but safe to include
    dag=dag,
)

trigger_function
