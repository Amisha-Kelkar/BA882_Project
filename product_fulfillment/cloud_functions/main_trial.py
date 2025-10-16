import os
import json
import pandas as pd
import duckdb
import http.client
from datetime import datetime, date, timedelta
from google.cloud import secretmanager, storage
from pandas import json_normalize

# Get secret value
def get_secret(secret_id: str) -> str:
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")

# Helper: Read store/tcin list from GCS
def get_store_tcin_list(bucket_name, file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    csv_bytes = blob.download_as_bytes()
    df = pd.read_csv(pd.io.common.BytesIO(csv_bytes))
    return df.to_dict("records")

# Main Cloud Function

def load_api_to_motherduck(request):
    """Weekly incremental loader for Target API → MotherDuck (one-row-per-service flattening)."""

    # --- Load secrets ---
    md_token = get_secret("project_motherduck_token")
    api_key = get_secret("target_api_test_amisha")

    # --- Load store/tcin list from Cloud Storage ---
    bucket_name = "store_product_ids"
    file_name = "Store_Product Ids.csv"
    pairs = get_store_tcin_list(bucket_name, file_name)

    # --- Connect to MotherDuck (create DB if missing) ---
    with duckdb.connect(f"md:?motherduck_token={md_token}") as conn:
        conn.sql("CREATE DATABASE IF NOT EXISTS project_882")
        conn.sql("USE project_882")

        # --- Check already-loaded dates ---
        try:
            existing_dates = set(
                r[0] for r in conn.execute("SELECT DISTINCT load_date FROM raw_target_api").fetchall()
            )
        except Exception:
            existing_dates = set()

        print(f"Already loaded load_dates: {existing_dates}")

        # --- Define date window ---
        end_date = date.today()
        start_date = end_date - timedelta(days=7)
        load_ts = datetime.utcnow().isoformat()
        load_dt = str(end_date)

        # --- Identify which days to load ---
        days_to_load = [
            d.strftime("%Y-%m-%d")
            for d in pd.date_range(start=start_date, end=end_date)
            if d.strftime("%Y-%m-%d") not in existing_dates
        ]

        if not days_to_load:
            print("No new load_date to load. Exiting cleanly.")
            return ("No new data to load.", 200)

        print(f"Loading days: {days_to_load}")

        # -------------------------------
        # Fetch API data
        # -------------------------------
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
                    endpoint = (
                        f"/product_fulfillment?"
                        f"longitude=-122.200&store_id={store_id}&state=MA&tcin={tcin}&zip=02134&latitude=37.820"
                    )
                    conn_api.request("GET", endpoint, headers=headers)
                    res = conn_api.getresponse()
                    data = res.read().decode("utf-8")
                    conn_api.close()

                    j = json.loads(data)
                    j["store_id"] = store_id
                    j["tcin"] = tcin
                    j["record_date"] = record_date
                    j["load_date"] = load_dt
                    j["load_timestamp"] = load_ts
                    all_json.append(j)

                    print(f"store={store_id}, tcin={tcin}, date={record_date}")

                except Exception as e:
                    print(f"Error fetching store={store_id}, tcin={tcin}, date={record_date}: {e}")

        if not all_json:
            return ("No data fetched from API.", 200)

        # -------------------------------
        # Flatten 'services' list
        # -------------------------------
        expanded_json = []
        for record in all_json:
            services_list = (
                record.get("data", {})
                .get("product", {})
                .get("fulfillment", {})
                .get("shipping_options", {})
                .get("services", [])
            )

            if isinstance(services_list, list) and len(services_list) > 0:
                for s in services_list:
                    if not s:
                        continue
                    new_row = record.copy()
                    new_row.update({f"services_{k}": v for k, v in s.items()})
                    expanded_json.append(new_row)
            else:
                expanded_json.append(record)

        # -------------------------------
        # Normalize + clean
        # -------------------------------
        df = json_normalize(expanded_json, sep="_")
        df.columns = [c.lower().replace(".", "_") for c in df.columns]

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
        other_cols = [c for c in df.columns if c not in front_cols]
        df = df[front_cols + other_cols]

        df = df.drop_duplicates(
            subset=["store_id", "tcin", "load_date", "services_shipping_method_id"], keep="last"
        )

        # -------------------------------
        # Write to MotherDuck
        # -------------------------------
        conn.register("df_view", df)

        # If table doesn’t exist, create it from df_view
        table_exists = (
            conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='raw_target_api'"
            ).fetchone()[0]
            > 0
        )

        if not table_exists:
            conn.execute("CREATE TABLE raw_target_api AS SELECT * FROM df_view")
            print("Created new table raw_target_api.")
        else:
            conn.execute("INSERT INTO raw_target_api SELECT * FROM df_view")
            print("Appended new rows to raw_target_api.")

        # -------------------------------
        # Create structured table
        # -------------------------------
        conn.execute("""
            CREATE TABLE IF NOT EXISTS target_products_fulfillment AS
            SELECT
                store_id::INT AS store_id,
                tcin::STRING AS tcin_product_id,
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
                data_product_fulfillment_is_out_of_stock_in_all_store_locations::BOOLEAN AS is_out_of_stock_in_all_stores,
                data_product_fulfillment_sold_out::BOOLEAN AS is_sold_out,
                data_product_fulfillment_shipping_options_availability_status::STRING AS shipping_availability_status,
                data_product_fulfillment_shipping_options_available_to_promise_quantity::FLOAT AS available_to_promise_qty,
                data_product_fulfillment_shipping_options_loyalty_availability_status::STRING AS loyalty_availability_status,
                data_product_fulfillment_product_id::STRING AS fulfillment_product_id,
                data_product_fulfillment_scheduled_delivery_location_id::INT AS delivery_location,
                data_product_notify_me_enabled::BOOLEAN AS notify_me_enabled,
                data_product_pay_per_order_charges_scheduled_delivery::FLOAT AS charge_scheduled_delivery,
                data_product_pay_per_order_charges_one_day::FLOAT AS charge_one_day
            FROM raw_target_api
        """)

    return (f"Loaded {len(df)} rows for {len(days_to_load)} new load_dates.", 200)

