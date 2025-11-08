import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import secretmanager
import duckdb
# ---------------------------------------------------------------------
# Helper: Retrieve secret from Google Secret Manager
# ---------------------------------------------------------------------
def get_secret(secret_id: str) -> str:
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


# ---------------------------------------------------------------------
# Main Cloud Function
# ---------------------------------------------------------------------
def target_product_price_details_to_motherduck(request):
    """HTTP Cloud Function — Generate synthetic Target Price Snapshot 
       for the latest record_date and 6 days before it (7 total)."""
    # ------------------------------------------------------------
    # Step 1 — Connect to MotherDuck and get recent (7-day window) product search data
    # ------------------------------------------------------------
    md_token = get_secret("project_motherduck_token")
    conn = duckdb.connect(f"md:?motherduck_token={md_token}")
    latest_date = conn.execute("""
        SELECT MAX(record_date)
        FROM project_882.main.target_products_search
        WHERE price IS NOT NULL
            """).fetchone()[0]
    latest_date = pd.to_datetime(latest_date)
    start_date = latest_date - timedelta(days=6)
    query = f"""
        SELECT store_id, product_id, price, record_date
        FROM project_882.main.target_products_search
        WHERE load_date BETWEEN '{start_date.date()}' AND '{latest_date.date()}'
          AND price IS NOT NULL
        """
    df = conn.execute(query).fetchdf()


    df["record_date"] = pd.to_datetime(df["record_date"])
    df["price"] = df["price"].astype(float)

    date_range = [latest_date - timedelta(days=i+1) for i in range(7)]
    df = pd.concat([df.assign(record_date=d) for d in date_range], ignore_index=True)

    # ------------------------------------------------------------
    # Step 2 — Deterministic synthetic enrichment logic
    # ------------------------------------------------------------
    np.random.seed(42)
    n = len(df)
    # Flags
    df["is_current_price_range"] = np.random.rand(n) < 0.4
    df["formatted_current_price_type"] = np.where(np.random.rand(n) < 0.4, "sale", "reg")
    df["formatted_comparison_price_type"] = np.random.choice(["reg", "was"], p=[0.9, 0.1], size=n)

    current_min = []
    current_max = []

    for i in range(n):
        p = df.iloc[i]["price"]
        if not df.iloc[i]["is_current_price_range"]:
            # Single price: keep as-is
            min_p = max_p = p
        else:
            # Controlled symmetric spread ±10%
            lower_pct = np.random.uniform(0.90, 0.98)
            upper_pct = np.random.uniform(1.02, 1.10)
            min_p = p * lower_pct
            max_p = p * upper_pct
        current_min.append(round(min_p, 2))
        current_max.append(round(max_p, 2))


    df["current_retail_min"] = current_min
    df["current_retail_max"] = current_max


        # Regular prices — per-row multipliers + order enforcement
    sale_mask = df["formatted_current_price_type"] == "sale"
    nonsale_mask = ~sale_mask

    mult_min_sale = np.random.uniform(1.10, 1.30, sale_mask.sum())
    mult_max_sale = np.random.uniform(1.10, 1.30, sale_mask.sum())
    mult_min_reg = np.random.uniform(1.00, 1.05, nonsale_mask.sum())
    mult_max_reg = np.random.uniform(1.00, 1.05, nonsale_mask.sum())

    df.loc[sale_mask, "reg_retail_min"] = (
            df.loc[sale_mask, "current_retail_min"].values * mult_min_sale
        ).round(2)
    df.loc[sale_mask, "reg_retail_max"] = (
            df.loc[sale_mask, "current_retail_max"].values * mult_max_sale
        ).round(2)

    df.loc[nonsale_mask, "reg_retail_min"] = (
            df.loc[nonsale_mask, "current_retail_min"].values * mult_min_reg
        ).round(2)
    df.loc[nonsale_mask, "reg_retail_max"] = (
            df.loc[nonsale_mask, "current_retail_max"].values * mult_max_reg
        ).round(2)

        # Enforce min ≤ max for regular prices
    df["reg_retail_min"], df["reg_retail_max"] = np.minimum(
            df["reg_retail_min"], df["reg_retail_max"]
        ), np.maximum(df["reg_retail_min"], df["reg_retail_max"])

        # Add-on threshold scaled to price
    def stable_threshold(price, seed=None):
        # Use the price itself (or product_id) to seed randomness deterministically
        rng = np.random.default_rng(abs(hash(price)) % (2**32))
        if price < 150:
            return int(np.clip(rng.normal(15, 5), 1, 20))
        elif price < 200:
            return int(np.clip(rng.normal(12, 5), 1, 20))
        else:
            return int(np.clip(rng.normal(18, 5), 1, 20))

    df["cart_add_on_threshold"] = df["price"].apply(stable_threshold)

        # Formatted strings
    def fmt(min_p, max_p, is_range):
            return f"${min_p:.2f} - ${max_p:.2f}" if is_range else f"${min_p:.2f}"

    df["formatted_current_price"] = [
            fmt(a, b, r) for a, b, r in zip(df["current_retail_min"], df["current_retail_max"], df["is_current_price_range"])
        ]
    df["formatted_comparison_price"] = [
            fmt(a, b, r) for a, b, r in zip(df["reg_retail_min"], df["reg_retail_max"], df["is_current_price_range"])
        ]

    # ------------------------------------------------------------
    # Step 3 — Write to  MotherDuck
    # ------------------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE project_882.main.target_product_price_details AS
        SELECT * FROM df
    """)
    conn.close()

    return f"✅ Generated synthetic Target Price Snapshot for {start_date.date()}–{latest_date.date()} ({len(df)} rows)."