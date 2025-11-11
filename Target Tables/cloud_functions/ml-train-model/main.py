import os
import pathlib
import duckdb
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score




BASE_DIR = pathlib.Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "sql"


def get_md_connection() -> duckdb.DuckDBPyConnection:

    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError(
            "MOTHERDUCK_TOKEN is not set. "
            "Run: export MOTHERDUCK_TOKEN=your_token_here"
        )

    conn_str = f"md:project_882?motherduck_token={token}"
    print(f"Connecting to MotherDuck with: {conn_str}")
    conn = duckdb.connect(conn_str)
    return conn


def load_dataframe(conn: duckdb.DuckDBPyConnection, sql_path: pathlib.Path) -> pd.DataFrame:
 
    sql_text = sql_path.read_text(encoding="utf-8")
    print(f"Running SQL from {sql_path}")
    df = conn.execute(sql_text).df()
    print(f"  -> Loaded {len(df):,} rows from {sql_path.name}")
    return df


def prepare_features(df: pd.DataFrame):

    id_cols = ["store_id", "product_id", "ds"]
    label_col = "label"

    feature_cols = [c for c in df.columns if c not in id_cols + [label_col]]

    X = df[feature_cols].copy()
    y = df[label_col].astype(int)

    # Fill missing values with 0 for simplicity
    X = X.fillna(0)

    print(f"Using {len(feature_cols)} feature columns:")
    print("  " + ", ".join(feature_cols))

    return X, y, feature_cols




def main(request):
    conn = get_md_connection()

    # 1) Load train and validation data
    train_df = load_dataframe(conn, SQL_DIR / "load-train-data.sql")
    valid_df = load_dataframe(conn, SQL_DIR / "load-valid-data.sql")

    if train_df.empty:
        raise RuntimeError("Training dataframe is empty. Check ai_datasets.stockout_features_v1 and split='train'.")
    if valid_df.empty:
        print("Warning: validation dataframe is empty. Metrics will be skipped.")

    # 2) Prepare features and labels
    X_train, y_train, feature_cols = prepare_features(train_df)

    if not valid_df.empty:
        X_valid, y_valid, _ = prepare_features(valid_df)
    else:
        X_valid, y_valid = None, None

    # 3) Train logistic regression model
    print("Training LogisticRegression model...")
    model = LogisticRegression(
        max_iter=1000,
        n_jobs=-1
    )
    model.fit(X_train, y_train)
    print("Model training finished.")

    # 4) Evaluate on validation set
    if X_valid is not None and len(X_valid) > 0:
        valid_proba = model.predict_proba(X_valid)[:, 1]
        valid_pred = (valid_proba >= 0.5).astype(int)

        acc = accuracy_score(y_valid, valid_pred)
        f1 = f1_score(y_valid, valid_pred)
        try:
            roc = roc_auc_score(y_valid, valid_proba)
        except ValueError:
            roc = float("nan")

        print("\nValidation metrics:")
        print(f"  Accuracy : {acc:.4f}")
        print(f"  F1 score : {f1:.4f}")
        print(f"  ROC AUC  : {roc:.4f}")
    else:
        print("No validation data available, skipping metrics.")

    # 5) Predict for all labeled rows and write to model_outputs schema
    print("\nLoading all labeled rows for prediction...")
    all_sql = """
        SELECT
            store_id,
            product_id,
            ds,

            stockout_next_7d AS label,

            available_to_promise_qty,
            is_out_of_stock_in_all_stores,
            is_sold_out,
            is_two_day_shipping,
            is_base_shipping_method,

            price,
            listed_price,
            current_retail_min,
            current_retail_max,
            reg_retail_min,
            reg_retail_max,
            rating,
            number_of_reviews,

            google_trend_score,

            inv_7d_avg,
            inv_7d_min,
            inv_7d_max,
            inv_7d_std,
            price_change_7d,
            rating_change_7d,
            reviews_change_7d,
            trend_7d_avg
        FROM project_882.ai_datasets.stockout_features_v1
        WHERE stockout_next_7d IS NOT NULL;
    """
    all_df = conn.execute(all_sql).df()
    print(f"  -> Total labeled rows: {len(all_df):,}")

    X_all, y_all, _ = prepare_features(all_df)
    proba_all = model.predict_proba(X_all)[:, 1]
    pred_all = (proba_all >= 0.5).astype(int)

    preds_df = all_df[["store_id", "product_id", "ds", "label"]].copy()
    preds_df["predicted_stockout_proba"] = proba_all
    preds_df["predicted_stockout_flag"] = pred_all

    print("\nWriting predictions to project_882.model_outputs.stockout_predictions_v1 ...")
    conn.execute("CREATE SCHEMA IF NOT EXISTS model_outputs;")
    conn.register("predictions_df", preds_df)
    conn.execute("""
        CREATE OR REPLACE TABLE project_882.model_outputs.stockout_predictions_v1 AS
        SELECT * FROM predictions_df;
    """)
    print("Done. Table: project_882.model_outputs.stockout_predictions_v1")

    conn.close()
    print("\nAll steps completed successfully.")


if __name__ == "__main__":
    main()
