from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# ---------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------
@dag(
    dag_id="target_api_to_motherduck",
    description="Trigger Cloud Function every 5 minutes to load Target API → MotherDuck",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # prevent overlap
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["target", "motherduck", "gcf"],
)
def target_api_to_motherduck():
    """
    DAG that triggers the deployed Google Cloud Function which loads Target API data into MotherDuck.
    """

    # -----------------------------------------------------------------
    # Task: Trigger Cloud Function (direct URL)
    # -----------------------------------------------------------------
    trigger_cloud_function = SimpleHttpOperator(
        task_id="trigger_cloud_function",
        http_conn_id=None,  # No Airflow connection — we’ll use the full URL directly
        endpoint="https://us-central1-ba882-team4.cloudfunctions.net/load_api_to_motherduck",
        method="GET",
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    trigger_cloud_function


target_api_to_motherduck()
