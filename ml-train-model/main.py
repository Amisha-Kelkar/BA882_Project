import os
import pathlib
import json
import uuid
from datetime import datetime, timezone

import duckdb
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score


BASE_DIR = pathlib.Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "sql"


def get_md_connection():
    """Connect to MotherDuck using the environment variable token."""
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN not set.")
    conn_str = f"md:project_882?motherduck_token={token}"
    print(f"Connecting to MotherDuck with: {conn_str}")
    return duckdb.connect(conn_str)


def run_sql_file(conn, path, label):
    """Execute a SQL file if it exists."""
    if path.exists():
        print(f"Running {label} SQL from {path} ...")
        conn.execute(path.read_text())
        print(f"Finished running {label} SQL.")
    else:
        print(f"[WARN] Missing SQL file: {path}")


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
    conn = get_md_connection()

    # 1. Register model and dataset
    run_sql_file(conn, SQL_DIR / "register_model.sql", "model registration")
    run_sql_file(conn, SQL_DIR / "register_dataset.sql", "dataset registration")

    # 2. Load dataset
    print("Loading dataset from project_882.ai_datasets.stockout_features ...")
    df = conn.execute("""
        SELECT * FROM project_882.ai_datasets.stockout_features
        WHERE stockout_label IS NOT NULL
    """).df()
    print(f"Loaded {len(df):,} rows total.")

    train_df = df[df["split"] == "train"].copy()
    valid_df = df[df["split"] == "test"].copy()
    print(f"Train rows: {len(train_df):,} | Valid rows: {len(valid_df):,}")

    X_train, y_train, feature_cols = prepare_features(train_df)
    X_valid, y_valid, _ = prepare_features(valid_df)

    # 3. Feature standardization
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_valid_scaled = scaler.transform(X_valid)

    # 4. Logistic Regression (without class_weight)
    print("Scaling features and training LogisticRegression model...")
    model = LogisticRegression(max_iter=5000, n_jobs=-1)
    model.fit(X_train_scaled, y_train)
    print("Model training finished.")

    # 5. Training metrics
    train_pred = model.predict(X_train_scaled)
    train_acc = accuracy_score(y_train, train_pred)
    train_f1 = f1_score(y_train, train_pred)
    print(f"\nTrain metrics:\n  Accuracy : {train_acc:.4f}\n  F1 score : {train_f1:.4f}")

    # 6. Validation metrics
    valid_proba = model.predict_proba(X_valid_scaled)[:, 1]
    valid_pred = (valid_proba >= 0.5).astype(int)
    valid_acc = accuracy_score(y_valid, valid_pred)
    valid_f1 = f1_score(y_valid, valid_pred)
    valid_roc = roc_auc_score(y_valid, valid_proba)
    print(f"\nValidation metrics:\n  Accuracy : {valid_acc:.4f}\n  F1 score : {valid_f1:.4f}\n  ROC AUC  : {valid_roc:.4f}")

    # 7. Generate predictions and write results to MotherDuck
    X_all, y_all, _ = prepare_features(df)
    X_all_scaled = scaler.transform(X_all)
    proba_all = model.predict_proba(X_all_scaled)[:, 1]
    pred_all = (proba_all >= 0.5).astype(int)
    preds_df = df[["store_id", "product_id", "event_date", "stockout_label"]].copy()
    preds_df["predicted_stockout_proba"] = proba_all
    preds_df["predicted_stockout_flag"] = pred_all

    conn.execute("CREATE SCHEMA IF NOT EXISTS model_outputs;")
    conn.register("predictions_df", preds_df)
    conn.execute("""
        CREATE OR REPLACE TABLE project_882.model_outputs.stockout_predictions_v1 AS
        SELECT * FROM predictions_df;
    """)
    print("Done. Table: project_882.model_outputs.stockout_predictions_v1")

    # 8. Log MLOps metadata
    model_id = "stockout_v1"
    dataset_id = conn.execute("""
        SELECT dataset_id FROM project_882.mlops.dataset
        WHERE model_id = ? ORDER BY created_at DESC LIMIT 1
    """, [model_id]).fetchone()[0]

    run_id = f"run_{uuid.uuid4().hex[:8]}"
    model_version_id = f"mv_{uuid.uuid4().hex[:8]}"
    metrics_json = json.dumps({
        "train_acc": train_acc,
        "train_f1": train_f1,
        "valid_acc": valid_acc,
        "valid_f1": valid_f1,
        "valid_roc_auc": valid_roc
    })
    params_json = json.dumps({"model": "logistic_regression", "scaler": "standard"})

    conn.execute("""
        INSERT INTO project_882.mlops.training_run
        (run_id, model_id, dataset_id, params, metrics, artifact, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [run_id, model_id, dataset_id, params_json, metrics_json,
          "project_882.model_outputs.stockout_predictions_v1", "success"])

    conn.execute("""
        INSERT INTO project_882.mlops.model_version
        (model_version_id, model_id, training_run_id, artifact_path, metrics_json, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [model_version_id, model_id, run_id,
          "project_882.model_outputs.stockout_predictions_v1", metrics_json, "candidate"])

    print(f"Logged training_run {run_id} and model_version {model_version_id}.")
    conn.close()
    print("\nAll steps completed successfully.")


if __name__ == "__main__":
    main()
