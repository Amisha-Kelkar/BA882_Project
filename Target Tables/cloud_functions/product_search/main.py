import os
import json
import random
import pandas as pd
import duckdb
import numpy as np
from datetime import datetime, date
from google.cloud import secretmanager
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata


# ---------------------------------------------------------------------
# Helper: Retrieve secret from Google Secret Manager
# ---------------------------------------------------------------------
def get_secret(secret_id: str) -> str:
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "ba882-team4")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


# ---------------------------------------------------------------------
# Utility: bounded random variation functions
# ---------------------------------------------------------------------
def vary_price(base_price: float, pct=0.10):
    """Add ±pct variation and round to nearest $0.50."""
    if pd.isna(base_price):
        return None
    factor = 1 + np.random.uniform(-pct, pct)
    varied = base_price * factor
    varied = round(varied * 2) / 2  # nearest 0.50
    return round(varied, 2)


def vary_rating(base_rating: float, pct=0.10):
    """Add ±pct variation and clamp between 0 and 5."""
    if pd.isna(base_rating):
        return None
    factor = 1 + np.random.uniform(-pct, pct)
    varied = base_rating * factor
    return float(np.clip(varied, 0, 5))


# ---------------------------------------------------------------------
# Main Cloud Function
# ---------------------------------------------------------------------
def load_product_search_to_motherduck(request):
    """Weekly loader for Target product_search → MotherDuck using cleaned data + SDV."""

    print("🔸 main.py invoked...")

    # --- Load Secrets ---
    md_token = get_secret("project_motherduck_token")

    # --- Connect to MotherDuck ---
    conn = duckdb.connect(f"md:?motherduck_token={md_token}")
    conn.sql("CREATE DATABASE IF NOT EXISTS project_882")
    conn.sql("USE project_882")
    print("✅ Connected to MotherDuck project_882")

    # --- Mode toggle ---
    use_synthetic = os.environ.get("USE_SYNTHETIC_DATA", "True").lower() == "true"

    # --- Metadata for ingestion ---
    load_dt = date.today().isoformat()
    load_ts = datetime.utcnow().isoformat()

    if use_synthetic:
        print("⚙️ Using SDV synthetic generator with cleaned base data.")

        # ---------------------------------------------------------------
        # 1️⃣ Load base reference products (clean, fixed IDs/titles)
        # ---------------------------------------------------------------
        try:
            ref_df = pd.read_csv("reference_products_cleaned_avg.csv", dtype={"product_id": str})
            ref_df["price"] = pd.to_numeric(ref_df["price"], errors="coerce")
            ref_df["rating"] = pd.to_numeric(ref_df["rating"], errors="coerce")
            ref_df = ref_df.dropna(subset=["product_id", "title"])
            print(f"✅ Loaded {len(ref_df)} reference products.")
        except Exception as e:
            print(f"❌ Could not read cleaned reference file: {e}")
            return ("Missing or unreadable reference_products_cleaned_avg.csv", 500)

        # ---------------------------------------------------------------
        # 2️⃣ Store universe (Boston stores)
        # ---------------------------------------------------------------
        boston_stores = [
            2822, 3226, 3222, 1441, 3368, 1898, 3304, 1442,
            1229, 3363, 3223, 3287, 3325, 1942, 2693, 1290,
            3253, 2649, 3305, 1266
        ]

        combos = pd.MultiIndex.from_product(
            [boston_stores, ref_df["product_id"]],
            names=["store_id", "product_id"]
        ).to_frame(index=False)

        df = combos.merge(ref_df, on="product_id", how="left")

        # ---------------------------------------------------------------
        # 3️⃣ Generate random auxiliary columns safely (no SDV dependence)
        # ---------------------------------------------------------------
        aux_cols = ["brand", "availability", "reviews", "url", "category"]

        # You can still load SDV for style / distributions
        try:
            metadata = SingleTableMetadata.load_from_json("target_product_metadata.json")
            synth = GaussianCopulaSynthesizer.load("target_product_synth.pkl")
            synth_df = synth.sample(len(ref_df))
            print("✅ SDV model loaded for auxiliary distribution reference.")
        except Exception:
            synth_df = pd.DataFrame()
            print("⚠️ Proceeding without SDV (random aux columns only).")

        # Generate random or sampled attributes directly (1 row per combo)
        if "brand" in synth_df and synth_df["brand"].dropna().nunique() > 0:
            df["brand"] = np.random.choice(synth_df["brand"].dropna().unique(), len(df), replace=True)
        else:
            df["brand"] = "Wondershop"

        df["availability"] = np.random.randint(0, 2, len(df))  # 0 = out of stock, 1 = available
        df["reviews"] = np.random.randint(0, 200, len(df))
        df["url"] = "https://www.target.com/p/" + df["product_id"].astype(str)
        df["category"] = "Christmas Decor"

        # ---------------------------------------------------------------
        # 4️⃣ Apply store-level intelligent variation
        # ---------------------------------------------------------------
        df["price"] = df["price"].apply(vary_price)
        df["rating"] = df["rating"].apply(vary_rating)

        # ---------------------------------------------------------------
        # 5️⃣ Add metadata columns
        # ---------------------------------------------------------------
        df["keyword"] = "christmas_decor"
        df["load_date"] = load_dt
        df["load_timestamp"] = load_ts
        df["record_date"] = load_ts

        print(f"✅ Generated {len(df)} rows ({len(ref_df)} products × {len(boston_stores)} stores).")
        print(f"💲 Price range: ${df['price'].min()}–${df['price'].max()} | ⭐ Rating range: {df['rating'].min()}–{df['rating'].max()}")

    else:
        print("🔗 Live API mode not implemented in this function.")
        return ("Live API mode not available.", 200)

    # ---------------------------------------------------------------
    # 🚀 Load to MotherDuck
    # ---------------------------------------------------------------
    conn.register("df_view", df)

    table_exists = (
        conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'target_products_search'
        """).fetchone()[0]
        > 0
    )

    if not table_exists:
        conn.execute("CREATE TABLE target_products_search AS SELECT * FROM df_view")
        print("🆕 Created target_products_search table.")
    else:
        existing_cols = [r[1] for r in conn.execute("PRAGMA table_info('target_products_search')").fetchall()]
        for col in existing_cols:
            if col not in df.columns:
                df[col] = None
        df = df[existing_cols]
        conn.register("df_view", df)
        col_str = ", ".join(existing_cols)
        conn.execute(f"INSERT INTO target_products_search ({col_str}) SELECT {col_str} FROM df_view")
        print(f"📈 Appended {len(df)} rows to target_products_search.")

    conn.close()
    msg = f"Loaded {len(df)} synthetic per-store rows into MotherDuck."
    print(msg)
    return (msg, 200)
