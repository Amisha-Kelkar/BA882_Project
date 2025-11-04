import os
import json
import time
import pandas as pd
import duckdb
import http.client
from datetime import datetime, date, timedelta
from google.cloud import secretmanager
from pandas import json_normalize


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
    """Weekly incremental loader for Target API → MotherDuck (one-row-per-service flattening)."""

    # --- Load Secrets ---
    md_token = get_secret("project_motherduck_token")
    api_key = get_secret("target_api_test_amisha")

    # -----------------------------------------------------------------
    # Connect to MotherDuck and fetch store/product pairs
    # -----------------------------------------------------------------
    with duckdb.connect(f"md:?motherduck_token={md_token}") as conn:
        conn.sql("CREATE DATABASE IF NOT EXISTS project_882")
        conn.sql("USE project_882")

        # --- Pull unique (store_id, product_id) pairs ---
        try:
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
        except Exception as e:
            print(f"Error fetching pairs from product_search: {e}")
            return ("Failed to fetch store/product pairs.", 500)

        # -----------------------------------------------------------------
        # Check existing load_dates to avoid duplicates
        # -----------------------------------------------------------------
        try:
            existing_dates = set(
                r[0] for r in conn.execute("SELECT DISTINCT load_date FROM raw_target_api").fetchall()
            )
        except Exception:
            existing_dates = set()

        print(f"Already loaded load_dates: {existing_dates}")

        # -----------------------------------------------------------------
        # Define date window and days to load
        # -----------------------------------------------------------------
        end_date = date.today()
        # start_date = end_date - timedelta(days=3)
        start_date = end_date - timedelta(days=6)
        load_ts = datetime.utcnow().isoformat()
        load_dt = str(end_date)

        days_to_load = [
            d.strftime("%Y-%m-%d")
            for d in pd.date_range(start=start_date, end=end_date)
            if d.strftime("%Y-%m-%d") not in existing_dates
        ]

        if not days_to_load:
            print("No new load_date to load. Exiting cleanly.")
            return ("No new data to load.", 200)

        print(f"Loading days: {days_to_load}")

        # -----------------------------------------------------------------
        # Fetch API data
        # -----------------------------------------------------------------
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

                    print(f"Fetched store={store_id}, tcin={tcin}, date={record_date}")
                    time.sleep(0.3)
                except Exception as e:
                    print(f"Error fetching store={store_id}, tcin={tcin}, date={record_date}: {e}")

        if not all_json:
            return ("No data fetched from API.", 200)

        # -----------------------------------------------------------------
        # Flatten `services` list and normalize
        # -----------------------------------------------------------------
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
                    if s:
                        new_row = record.copy()
                        new_row.update({f"services_{k}": v for k, v in s.items()})
                        expanded_json.append(new_row)
            else:
                expanded_json.append(record)

        df = json_normalize(expanded_json, sep="_")
        df.columns = [c.lower().replace(".", "_") for c in df.columns]

        # -----------------------------------------------------------------
        # Ensure key columns exist
        # -----------------------------------------------------------------
        meta_cols = ["store_id", "tcin", "record_date", "load_date", "load_timestamp"]
        for m in meta_cols:
            if m not in df.columns:
                df[m] = None

        service_cols = [
            "services_shipping_method_id",
            "services_shipping_method_short_description",
            "services_service_level_description",
            "services_min_delivery_date",
            "services_max_delivery_date",
            "services_cutoff",
            "services_is_base_shipping_method",
            "services_is_two_day_shipping",
        ]
        for s in service_cols:
            if s not in df.columns:
                df[s] = None

        front_cols = meta_cols + service_cols
        df = df[front_cols + [c for c in df.columns if c not in front_cols]]
        df = df.drop_duplicates(
            subset=["store_id", "tcin", "load_date", "services_shipping_method_id"], keep="last"
        )

        # -----------------------------------------------------------------
        # Write to raw_target_api (schema-safe append)
        # -----------------------------------------------------------------
        conn.register("df_view", df)
        table_exists = (
            conn.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'raw_target_api'
            """).fetchone()[0]
            > 0
        )

        if not table_exists:
            conn.execute("CREATE TABLE raw_target_api AS SELECT * FROM df_view")
            print("🆕 Created new table raw_target_api.")
        else:
            existing_cols = [r[1] for r in conn.execute("PRAGMA table_info('raw_target_api')").fetchall()]
            df_cols = df.columns.tolist()

            for col in existing_cols:
                if col not in df_cols:
                    df[col] = None
                    print(f"Added missing column '{col}' to dataframe as NULL.")

            df = df[existing_cols]
            conn.register("df_view", df)
            col_str = ", ".join(existing_cols)
            conn.execute(f"INSERT INTO raw_target_api ({col_str}) SELECT {col_str} FROM df_view")
            print(f"Appended {len(df)} rows to raw_target_api.")

        # -----------------------------------------------------------------
        # Create or append to structured table (target_products_fulfillment)
        # -----------------------------------------------------------------
        tpf_exists = (
            conn.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = 'target_products_fulfillment'
            """).fetchone()[0]
            > 0
        )

        tpf_select_query = """
            SELECT
                store_id::INT AS store_id,
                tcin::STRING AS tcin_product_id,
                record_date::STRING AS record_date,
                load_date::STRING AS load_date,
                load_timestamp::TIMESTAMP AS load_ts,
                services_shipping_method_id::STRING AS shipping_method_id,
                services_shipping_method_short_description::STRING AS shipping_method_short_description,
                services_service_level_description::STRING AS service_level_description,
                services_min_delivery_date::DATE AS min_delivery_date,
                services_max_delivery_date::DATE AS max_delivery_date,
                services_cutoff::TIMESTAMP AS cutoff_time,
                services_is_base_shipping_method::BOOLEAN AS is_base_shipping_method,
                services_is_two_day_shipping::BOOLEAN AS is_two_day_shipping,
                data_product_tcin::STRING AS product_tcin,
                data_product___typename::STRING AS product_typename,
                data_product_fulfillment_scheduled_delivery_availability_status::STRING AS scheduled_delivery_status,
                data_product_fulfillment_scheduled_delivery_location_available_to_promise_quantity::FLOAT AS scheduled_delivery_promise_qty,
                data_product_fulfillment_scheduled_delivery_location_id::INT AS scheduled_delivery_location_id,
                data_product_fulfillment_is_out_of_stock_in_all_store_locations::BOOLEAN AS is_out_of_stock_in_all_stores,
                data_product_fulfillment_product_id::INT AS product_id,
                data_product_fulfillment_sold_out::BOOLEAN AS is_sold_out,
                data_product_fulfillment_shipping_options_availability_status::STRING AS shipping_availability_status,
                data_product_fulfillment_shipping_options_available_to_promise_quantity::FLOAT AS available_to_promise_qty,
                data_product_fulfillment_shipping_options_loyalty_availability_status::STRING AS loyalty_availability_status,
                data_product_fulfillment_product_id::STRING AS fulfillment_product_id,
                data_product_notify_me_eligible::BOOLEAN AS notify_me_eligible,
                data_product_notify_me_enabled::BOOLEAN AS notify_me_enabled,
                data_product_pay_per_order_charges_scheduled_delivery::FLOAT AS charge_scheduled_delivery,
                data_product_pay_per_order_charges_one_day::FLOAT AS charge_one_day,
                data_product_store_positions::STRING AS store_positions
            FROM raw_target_api
        """

        if not tpf_exists:
            conn.execute(f"CREATE TABLE target_products_fulfillment AS {tpf_select_query}")
            print("Created target_products_fulfillment.")
        else:
            conn.execute(f"""
                INSERT INTO target_products_fulfillment
                {tpf_select_query}
                WHERE load_date NOT IN (
                    SELECT DISTINCT load_date FROM target_products_fulfillment
                )
            """)
            print("Appended new load_date rows to target_products_fulfillment.")

        try:
            conn.execute("""
        DELETE FROM raw_target_api
        WHERE CAST(load_date AS DATE) < (CURRENT_DATE - INTERVAL 7 DAY)
    """)
            print("Cleaned up raw_target_api: deleted rows older than 7 days.")
        except Exception as e:
            print(f"Cleanup step failed: {e}")
        # -----------------------------------------------------------------
        # Completion message
        # -----------------------------------------------------------------
        msg = f"Loaded {len(df)} rows for {len(days_to_load)} new load_dates."
        print(msg)
        return (msg, 200)
