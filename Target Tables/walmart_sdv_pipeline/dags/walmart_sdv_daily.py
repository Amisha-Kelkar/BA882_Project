# walmart_sdv_daily.py
# Minimal Airflow 2.7.x / Composer 3 DAG: triggers and waits for a Cloud Run Job using CloudRunExecuteJobOperator.
# You’ve already granted your service account the roles: run.invoker, run.jobsExecutor, and run.viewer.
# Note: Do NOT include parameters such as wait_until_complete or overrides, which are not accepted in this version.

from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.operators.empty import EmptyOperator  # Placeholder task: for initial testing; replace with real quality checks later

DAG_ID = "walmart_sdv_daily"

# ======= Base Configuration =======
PROJECT_ID = "ba882-team4"
REGION = "us-central1"
JOB_NAME = "walmart-loader"

# Set the start_date in the past so Composer begins scheduling right away.
# catchup=False disables backfilling of historical runs.
with DAG(
    dag_id=DAG_ID,
    description="Daily SDV sampling and load into MotherDuck via Cloud Run Job",
    start_date=datetime(2025, 10, 30),
    schedule="@daily",          # You can change this to a custom cron expression if needed
    catchup=False,
    tags=["ba882", "sdv", "motherduck", "cloudrun"],
    default_args={
        "owner": "airflow",
        "retries": 0,           # Start with 0 for easier debugging; increase to 1–3 once stable
    },
) as dag:

    # Trigger the Cloud Run Job and wait for completion (synchronous polling)
    # This avoids deferrable mode, which can require additional triggers or permissions.
    generate_sdv = CloudRunExecuteJobOperator(
        task_id="generate_sdv",
        project_id=PROJECT_ID,      # ✅ Explicit project ID
        region=REGION,              # ✅ Region
        job_name=JOB_NAME,          # ✅ Job name in Cloud Run
        deferrable=False,           # ✅ Disable deferrable mode to poll directly on the worker
        # Note: Do NOT pass wait_until_complete / overrides or other unsupported parameters
    )

    # Optional: placeholder for post-generation data quality checks
    # You can later replace this with a PythonOperator or BashOperator to validate output data
    quality_checks = EmptyOperator(task_id="quality_checks")

    generate_sdv >> quality_checks

