import os
import pathlib
import json
import uuid
import logging
from datetime import datetime, timezone

import duckdb
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from flask import Request  # For Cloud Function HTTP request type
from google.cloud import secretmanager  # For loading token from Secret Manager


BASE_DIR = pathlib.Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "sql"


def get_md_connection():
    """
    Connect to MotherDuck using a token from env var or Secret Manager.

    Priority:
    1) MOTHERDUCK_TOKEN environment variable (if set)
    2) Secret Manager: projects/ba882-team4/secrets/project_motherduck_token/versions/latest
    """
    token = os.environ.get("MOTHERDUCK_TOKEN")

    if not token:
        print("MOTHERDUCK_TOKEN not found in env. Trying Secret Manager...")
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = (
                "projects/ba882-team4/"
                "secrets/project_motherduck_token/"
                "versions/latest"
            )
            response = client.access_secret_version(request={"name": name})
            token = response.payload.data.decode("UTF-8")
            print("Loaded MotherDuck token from Secret Manager.")
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve MotherDuck token: {e}")

    # Log only the prefix to avoid leaking the full token
    print(f"Connecting to MotherDuck with token prefix: {token[:8]}***")

    conn_str = f"md:project_882?motherduck_token={token}"
    return duckdb.connect(conn_str)


def run_sql_file(conn, path, label):
    """Execute a SQL file if it exists."""
    if path.exists():
        print(f"Running {label} SQL from {path} ...")
        conn.execute(path.read_text())
        print(f"Finished running {label} SQL.")
    else:
        print(f"[WARN] Missing SQL file: {path}")


def build_stockout_features(conn):
    """
    Phase 0: Rebuild feature dataset in MotherDuck.

    This executes build_stockout_features.sql to create/refresh
    project_882.ai_datasets.stockout_features from the latest gold.stockout_daily.
    """
    sql_path = SQL_DIR / "build_stockout_features.sql"
    run_sql_file(conn, sql_path, "build stockout_features dataset")


def prepare_features(df):
    """Prepare features and labels for model training."""
    id_cols = ["store_id", "product_id", "event_date"]
    drop_cols = ["brand", "category", "split", "created_at"]
    label_col = "stockout_label"
    feature_cols = [c for c in df.columns if c not in id_cols + [label_col] + drop_cols]

    X = df[feature_cols].fillna(0)
    y = df[label_col].astype(int)
    print(f"Using {len(feature_cols)} feature columns: {', '.join(feature_cols)}")
    return X, y, feature_cols


def main():
    """
    End-to-end training pipeline:

    Phase 0: Rebuild feature dataset (ai_datasets.stockout_features)
    Phase 1: Register model & dataset metadata
    Phase 2: Train & evaluate logistic regression model
    Phase 3: Generate predictions and write to MotherDuck
    Phase 4: Log MLOps metadata (training_run & model_version)
    """
    conn = get_md_connection()

    # ==============================
    # Phase 0: Rebuild feature dataset
    # ==============================
    print("Phase 0: Rebuilding ai_datasets.stockout_features ...")
    build_stockout_features(conn)
    print("Finished rebuilding ai_datasets.stockout_features.\n")

    # ==============================
    # Phase 1: Register model & dataset
    # ==============================
    run_sql_file(conn, SQL_DIR / "register_model.sql", "model registration")
    run_sql_file(conn, SQL_DIR / "register_dataset.sql", "dataset registration")

    # ==============================
    # Phase 2: Load dataset & train model
    # ==============================
    print("Loading dataset from project_882.ai_datasets.stockout_features ...")
    df = conn.execute(
        """
        SELECT * FROM project_882.ai_datasets.stockout_features
        WHERE stockout_label IS NOT NULL
        """
    ).df()
    print(f"Loaded {len(df):,} rows total.")

    # Split into train / validation
    train_df = df[df["split"] == "train"].copy()
    valid_df = df[df["split"] == "test"].copy()
    print(f"Train rows: {len(train_df):,} | Valid rows: {len(valid_df):,}")

    X_train, y_train, feature_cols = prepare_features(train_df)
    X_valid, y_valid, _ = prepare_features(valid_df)

    # Feature standardization
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_valid_scaled = scaler.transform(X_valid)

    # Train Logistic Regression
    print("Scaling features and training LogisticRegression model...")
    model = LogisticRegression(max_iter=5000, n_jobs=-1)
    model.fit(X_train_scaled, y_train)
    print("Model training finished.")

    # Training metrics
    train_pred = model.predict(X_train_scaled)
    train_acc = accuracy_score(y_train, train_pred)
    train_f1 = f1_score(y_train, train_pred)
    print(
        f"\nTrain metrics:\n"
        f"  Accuracy : {train_acc:.4f}\n"
        f"  F1 score : {train_f1:.4f}"
    )

    # Validation metrics
    valid_proba = model.predict_proba(X_valid_scaled)[:, 1]
    valid_pred = (valid_proba >= 0.5).astype(int)
    valid_acc = accuracy_score(y_valid, valid_pred)
    valid_f1 = f1_score(y_valid, valid_pred)
    valid_roc = roc_auc_score(y_valid, valid_proba)
    print(
        f"\nValidation metrics:\n"
        f"  Accuracy : {valid_acc:.4f}\n"
        f"  F1 score : {valid_f1:.4f}\n"
        f"  ROC AUC  : {valid_roc:.4f}"
    )

    # ==============================
    # Phase 3: Generate predictions & write to MotherDuck
    # ==============================
    X_all, y_all, _ = prepare_features(df)
    X_all_scaled = scaler.transform(X_all)
    proba_all = model.predict_proba(X_all_scaled)[:, 1]
    pred_all = (proba_all >= 0.5).astype(int)

    preds_df = df[["store_id", "product_id", "event_date", "stockout_label"]].copy()
    preds_df["predicted_stockout_proba"] = proba_all
    preds_df["predicted_stockout_flag"] = pred_all

    # Create output schema/table and write predictions
    conn.execute("CREATE SCHEMA IF NOT EXISTS model_outputs;")
    conn.register("predictions_df", preds_df)
    conn.execute(
        """
        CREATE OR REPLACE TABLE project_882.model_outputs.stockout_predictions_v1 AS
        SELECT * FROM predictions_df;
        """
    )
    print("Done. Table: project_882.model_outputs.stockout_predictions_v1")

    # ==============================
    # Phase 4: Log MLOps metadata
    # ==============================
    model_id = "stockout_v1"

    # Get latest dataset_id for this model
    dataset_id = conn.execute(
        """
        SELECT dataset_id FROM project_882.mlops.dataset
        WHERE model_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [model_id],
    ).fetchone()[0]

    run_id = f"run_{uuid.uuid4().hex[:8]}"
    model_version_id = f"mv_{uuid.uuid4().hex[:8]}"

    metrics_json = json.dumps(
        {
            "train_acc": float(train_acc),
            "train_f1": float(train_f1),
            "valid_acc": float(valid_acc),
            "valid_f1": float(valid_f1),
            "valid_roc_auc": float(valid_roc),
        }
    )
    params_json = json.dumps(
        {
            "model": "logistic_regression",
            "scaler": "standard",
        }
    )

    # Insert training_run record
    conn.execute(
        """
        INSERT INTO project_882.mlops.training_run
        (run_id, model_id, dataset_id, params, metrics, artifact, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            model_id,
            dataset_id,
            params_json,
            metrics_json,
            "project_882.model_outputs.stockout_predictions_v1",
            "success",
        ],
    )

    # Insert model_version record
    conn.execute(
        """
        INSERT INTO project_882.mlops.model_version
        (model_version_id, model_id, training_run_id, artifact_path, metrics_json, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            model_version_id,
            model_id,
            run_id,
            "project_882.model_outputs.stockout_predictions_v1",
            metrics_json,
            "candidate",
        ],
    )

    print(f"Logged training_run {run_id} and model_version {model_version_id}.")
    conn.close()
    print("\nAll steps completed successfully.")


def run_training(request: Request):
    """
    Cloud Function HTTP entry point.

    This function is used as the target when deploying:
    --entry-point=run_training
    """
    try:
        logging.info("Starting training from Cloud Function...")
        main()
        logging.info("Training finished successfully.")
        return ("Training run completed.", 200)
    except Exception as e:
        logging.exception("Training failed.")
        # Return error text for easier debugging from HTTP caller
        return (f"Training failed: {e}", 500)


if __name__ == "__main__":
    # Local execution entry point
    main()
