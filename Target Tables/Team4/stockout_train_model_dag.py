from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# -----------------------------------------
# Default configuration for the DAG
# -----------------------------------------
default_args = {
    "owner": "ba882_team4",
    "depends_on_past": False,
    "retries": 0,
}

# -----------------------------------------
# DAG definition
# -----------------------------------------
with DAG(
    dag_id="stockout_train_model",
    description="Train the stockout model using the local ml-train-model script",
    default_args=default_args,
    schedule_interval="@daily",       # ✅ Runs automatically once per day (00:00 UTC)
    start_date=datetime(2025, 11, 1),
    catchup=False,                    # Skip historical runs
    tags=["ba882", "mlops", "train"],
) as dag:

    # ------------------------------------------------------
    # Task: Run the Python training script on Composer worker
    # ------------------------------------------------------
    run_main_py = BashOperator(
        task_id="run_main_py",
        bash_command="""
        # Retrieve MotherDuck token from Secret Manager
        export MOTHERDUCK_TOKEN=$(gcloud secrets versions access latest --secret=project_motherduck_token)

        echo "Using MOTHERDUCK_TOKEN: ${MOTHERDUCK_TOKEN:0:5}***"

        # Execute the training script inside the Composer DAGs folder
        python /home/airflow/gcs/dags/lib/ml-train-model/main.py
        """,
    )
