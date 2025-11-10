from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# -----------------------------------------
# Default DAG configuration
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
    schedule="@weekly",          # Runs once per week
    start_date=datetime(2025, 11, 1),
    catchup=False,               # Skip historical runs
    tags=["ba882", "mlops", "train"],
) as dag:

    # ------------------------------------------------------
    # Task: Run the Python training script inside container
    # ------------------------------------------------------
    run_main_py = BashOperator(
        task_id="run_main_py",
        bash_command=r"""
        set -e

        echo "Running stockout training script in Astronomer..."

        # Airflow home in Astronomer is usually /usr/local/airflow
        echo "AIRFLOW_HOME: ${AIRFLOW_HOME:-/usr/local/airflow}"
        cd "${AIRFLOW_HOME:-/usr/local/airflow}"

        echo "Current working directory: $(pwd)"

        echo "Listing training script directory:"
        ls -R "Target Tables/cloud_functions/ml-train-model" || {
            echo "Directory not found under $(pwd)"; exit 1;
        }

        python "Target Tables/cloud_functions/ml-train-model/main.py"

        echo "Training script finished successfully."
        """,
    )
