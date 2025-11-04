import os
import json
import time
import pandas as pd
import duckdb
import http.client
import numpy as np
from datetime import datetime, date, timedelta
from google.cloud import secretmanager
from pandas import json_normalize
from random import choice

# --- SDV imports ---
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata


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
def load_api_to_motherduck(request):
    """Weekly incremental loader for Target API → MotherDuck (supports synthetic mode)."""

    # --- Load Secrets ---
    md_token = get_secret("project_motherduck_token")
    api_key = get_secret("target_api_test_amisha")

    # --- Mode toggle ---
    use_synthetic = os.environ.get("USE_SYNTHETIC_DATA", "True").lower() == "true"

    with duckdb.connect(f"md:?motherduck_token={md_token}") as conn:
        conn.sql("CREATE DATABASE IF NOT EXISTS project_882")
        conn.sql("USE project_882")

        # -----------------------------------------------------------------
        # Fetch valid (store_id, product_id) pairs from product_search
        # -----------------------------------------------------------------
        pairs_df = conn.execute("""
            SELECT DISTINCT
                CAST(store_id AS INTEGER) AS store_id,
                CAST(product_id AS STRING) AS tcin
            FROM project_882.main.target_products_search
            WHERE store_id IS NOT NULL
              AND product_id IS NOT NULL
              AND load_date=(SELECT MAX(load_date) FROM project_882.main.target_products_search)
        """).fetchdf()
        pairs = pairs_df.to_dict("records")
        print(f"Fetched {len(pairs)} unique store/product_id pairs from product_search.")

        # -----------------------------------------------------------------
        # Define 7-day rolling date window
        # -----------------------------------------------------------------
        end_date = date.today()
        start_date = end_date - timedelta(days=6)
        load_ts = datetime.utcnow().isoformat()
        load_dt = str(end_date)
        days_to_load = [d.strftime("%Y-%m-%d") for d in pd.date_range(start=start_date, end=end_date)]

        print(f"Loading days: {days_to_load}")

        # ==============================================================
        # 🔁 SYNTHETIC MODE (SDV)
        # ==============================================================
        if use_synthetic:
            print("⚙️ Using SDV synthetic data generator instead of live API.")

            metadata = SingleTableMetadata.load_from_json("target_full_metadata.json")
            synth = CTGANSynthesizer.load("target_full_synth.pkl")

            base_n = max(len(pairs) * len(days_to_load), 1000)
            synthetic_df = synth.sample(base_n)

            # --- Constrain IDs to real store/product pairs ---
            valid_pairs = pd.DataFrame(pairs)
            repeated_pairs = pd.concat(
                [valid_pairs] * (base_n // len(valid_pairs) + 1),
                ignore_index=True
            ).sample(n=base_n, random_state=42).reset_index(drop=True)
            synthetic_df["store_id"] = repeated_pairs["store_id"]
            synthetic_df["tcin"] = repeated_pairs["tcin"]

            # --- Inject rolling 7-day date logic ---
            synthetic_df["record_date"] = [choice(days_to_load) for _ in range(base_n)]
            synthetic_df["load_date"] = load_dt
            synthetic_df["load_timestamp"] = load_ts

            synthetic_df["min_delivery_date"] = pd.to_datetime(synthetic_df["record_date"]) + pd.to_timedelta(
                np.random.randint(1, 3, size=len(synthetic_df)), unit="D"
            )
            synthetic_df["max_delivery_date"] = synthetic_df["min_delivery_date"] + pd.to_timedelta(
                np.random.randint(1, 3, size=len(synthetic_df)), unit="D"
            )

            # Clean up ID columns to avoid float/exponential formats
            id_cols = ["fulfillment_product_id", "product_id", "tcin"]
            for c in id_cols:
                if c in synthetic_df.columns:
                    synthetic_df[c] = synthetic_df[c].astype(str).str.replace(r"\.0$", "", regex=True)

            df = synthetic_df.copy()
            print(f"✅ Generated {len(df)} synthetic rows for {len(days_to_load)} days.")

        # ==============================================================
        # 🔗 LIVE API MODE (original logic)
        # ==============================================================
        else:
            print("🔗 Fetching live Target API data.")
            all_json = []
            for pair in pairs:
                store_id = int(pair["store_id"])
                tcin = str(pair["tcin"])
                for record_date in days_to_load:
                    try:
                        conn_api = http.client.HTTPSConnection("target-com-shopping-api.p.rapidapi.com")
                        headers = {
                            "x-rapidapi-key": api_key,
                            "x-rapidapi-host": "target-com-shopping-api.p.rapidapi.com",
                            "accept": "application/json",
                            "cache-control": "no-cache",
                            "authority": "redsky.target.com",
                        }
                        endpoint = f"/product_fulfillment?store_id={store_id}&state=MA&tcin={tcin}"
                        conn_api.request("GET", endpoint, headers=headers)
                        res = conn_api.getresponse()
                        data = res.read().decode("utf-8")

                        j = json.loads(data)
                        j["store_id"] = store_id
                        j["tcin"] = tcin
                        j["record_date"] = record_date
                        j["load_date"] = load_dt
                        j["load_timestamp"] = load_ts
                        all_json.append(j)
                        time.sleep(0.3)
                    except Exception as e:
                        print(f"Error fetching store={store_id}, tcin={tcin}, date={record_date}: {e}")

            if not all_json:
                return ("No data fetched from API.", 200)

            # Flatten JSON to DataFrame
            expanded_json = []
            for record in all_json:
                services_list = (
                    record.get("data", {})
                    .get("product", {})
                    .get("fulfillment", {})
                    .get("shipping_options", {})
                    .get("services", [])
                )
                if isinstance(services_list, list) and services_list:
                    for s in services_list:
                        new_row = record.copy()
                        new_row.update({f"services_{k}": v for k, v in s.items()})
                        expanded_json.append(new_row)
                else:
                    expanded_json.append(record)

            df = json_normalize(expanded_json, sep="_")
            df.columns = [c.lower().replace(".", "_") for c in df.columns]

        # ==============================================================
        # 🚀 Load into MotherDuck
        # ==============================================================
        conn.register("df_view", df)

        table_exists = (
            conn.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'target_products_fulfillment'
            """).fetchone()[0]
            > 0
        )

        if not table_exists:
            conn.execute("CREATE TABLE target_products_fulfillment AS SELECT * FROM df_view")
            print("🆕 Created target_products_fulfillment.")
        else:
            existing_cols = [r[1] for r in conn.execute("PRAGMA table_info('target_products_fulfillment')").fetchall()]
            for col in existing_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[existing_cols]
            conn.register("df_view", df)
            col_str = ", ".join(existing_cols)
            conn.execute(f"INSERT INTO target_products_fulfillment ({col_str}) SELECT {col_str} FROM df_view")
            print(f"Appended {len(df)} rows to target_products_fulfillment.")

        msg = f"Loaded {len(df)} {'synthetic' if use_synthetic else 'real'} rows for 7-day window."
        print(msg)
        return (msg, 200)
