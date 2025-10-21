import os
import json
import http.client
import pandas as pd
import duckdb
import urllib.parse
import time
from datetime import datetime, date
from google.cloud import secretmanager
from pandas import json_normalize
from datetime import datetime

# ------------------------------------------------
# Helper: Get secret from Secret Manager
# ------------------------------------------------
def get_secret(secret_id: str) -> str:
    project_id = "ba882-team4"
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")

# ------------------------------------------------
# Cloud Function entrypoint
# ------------------------------------------------
def load_product_search_to_motherduck(request):
    """Weekly loader for Target product_search across multiple Boston stores → MotherDuck."""

    # --- Load secrets ---
    md_token = get_secret("project_motherduck_token")
    api_key = get_secret("target_api_test_amisha")

    # --- Connect to MotherDuck (persistent project_882 DB) ---
    conn = duckdb.connect(f"md:project_882?motherduck_token={md_token}")
    print("Connected to MotherDuck project_882")

    # Debug: show connected DB
    db_list = conn.execute("PRAGMA database_list;").fetchdf()
    print("Current database list:")
    print(db_list)

    # ----------------------------------------------------
    # Boston-area Target stores 
    # ----------------------------------------------------
    # boston_stores = [
    #     2822, 3226, 3222, 1441, 3368, 1898, 3304, 1442,
    #     1229, 3363, 3223, 3287, 3325, 1942, 2693, 1290,
    #     3253, 2649, 3305, 1266
    # ]

    # Main Boston stores.
    boston_stores = [2822,3226,3222,3368,1898]

    # Seasonal keywords
    keywords = ["halloween_decor", "christmas_decor", "winter_jackets"]

    # ----------------------------------------------------
    # Metadata for ingestion
    # ----------------------------------------------------
    load_dt = date.today().isoformat()
    load_ts = datetime.utcnow().isoformat()

    all_json = []

    # ----------------------------------------------------
    # Fetch data for each store + keyword
    # ----------------------------------------------------
    for store_id in boston_stores:
        for keyword in keywords:
            safe_keyword = urllib.parse.quote_plus(keyword)
            endpoint = f"/product_search?store_id={store_id}&offset=0&keyword={safe_keyword}&count=25"

            success = False
            for attempt in range(3):  # retry loop
                try:
                    conn_api = http.client.HTTPSConnection("target-com-shopping-api.p.rapidapi.com")
                    headers = {
                        "x-rapidapi-key": api_key,
                        "x-rapidapi-host": "target-com-shopping-api.p.rapidapi.com",
                        "accept": "application/json"
                    }
                    conn_api.request("GET", endpoint, headers=headers)
                    res = conn_api.getresponse()
                    status = res.status
                    data = res.read().decode("utf-8")
                    conn_api.close()

                    if status != 200:
                        print(f"HTTP {status} for store={store_id}, keyword={keyword} (try {attempt+1})")
                        time.sleep(2)
                        continue

                    j = json.loads(data)
                    products = j.get("data", {}).get("search", {}).get("products", [])

                    if not products:
                        print(f"No products found for store={store_id}, keyword={keyword}")
                        break

                    j["store_id"] = store_id
                    j["keyword"] = keyword
                    j["load_date"] = load_dt
                    j["load_timestamp"] = load_ts
                    all_json.append(j)
                    success = True
                    print(f"store={store_id}, keyword={keyword}, products={len(products)}")
                    break
                except Exception as e:
                    print(f"Error store={store_id}, keyword={keyword}, try={attempt+1}: {e}")
                    time.sleep(2)

            if not success:
                print(f"Skipped store={store_id}, keyword={keyword} after 3 failed attempts.")

    if not all_json:
        print("No valid product data found.")
        return ("No valid product data found.", 200)

    # ----------------------------------------------------
    # Flatten product data
    # ----------------------------------------------------
    expanded_json = []
    for record in all_json:
        products = record.get("data", {}).get("search", {}).get("products", [])
        for p in products:
            price_info = p.get("price", {})
            brand_info = p.get("primary_brand", {})
            record_flat = {
                "store_id": record["store_id"],
                "product_id": p.get("tcin"),
                "title": (
                    p.get("item", {}).get("product_description", {}).get("title")
                    or p.get("product_description", {}).get("title")
                ),
                "brand": brand_info.get("name"),
                "price": price_info.get("formatted_current_price") or price_info.get("current_retail"),
                "availability": p.get("availability_status"),
                "rating": (
                    p.get("ratings_and_reviews", {}).get("statistics", {}).get("rating", {}).get("average")
                ),
                "reviews": (
                    p.get("ratings_and_reviews", {}).get("statistics", {}).get("rating", {}).get("count")
                ),
                "url": (
                    p.get("enrichment", {}).get("buy_url")
                    or p.get("item", {}).get("enrichment", {}).get("buy_url")
                ),
                "category": (
                    p.get("product_classification", {}).get("item_type", {}).get("name")
                ),
                "keyword": record["keyword"],
                "load_date": record["load_date"],
                "load_timestamp": record["load_timestamp"],
            }
            expanded_json.append(record_flat)

    df = pd.DataFrame(expanded_json)
    df.columns = [c.lower().replace(".", "_") for c in df.columns]
    print(f"Flattened {len(df)} product rows.")
    if "record_date" not in df.columns:
        df["record_date"] = datetime.utcnow().isoformat()

    if df.empty:
        return ("No valid product data found.", 200)

    # ----------------------------------------------------
    # Write to MotherDuck
    # ----------------------------------------------------
    conn.register("df_view", df)

    table_exists = (
        conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='raw_target_product_search'"
        ).fetchone()[0]
        > 0
    )

    if not table_exists:
        conn.execute("CREATE TABLE raw_target_product_search AS SELECT * FROM df_view")
        print("Created new table raw_target_product_search.")
    else:
        conn.execute("INSERT INTO raw_target_product_search SELECT * FROM df_view")
        print("Appended new rows to raw_target_product_search.")

    # ----------------------------------------------------
    # Create or update structured table with deduplication
    # ----------------------------------------------------

    # Check if target table exists
    tps_exists = (
        conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'target_products_search'
        """).fetchone()[0]
        > 0
    )

    # Define your clean SELECT query (typed and safe)
    tps_select_query = """
        SELECT
            TRY_CAST(store_id AS INT) AS store_id,
            product_id::STRING AS product_id,
            title::STRING AS title,
            brand::STRING AS brand,
            price::STRING AS price,
            availability::STRING AS availability,
            TRY_CAST(rating AS FLOAT) AS rating,
            TRY_CAST(reviews AS INTEGER) AS reviews,
            url::STRING AS url,
            category::STRING AS category,
            keyword::STRING AS keyword,
            load_date::STRING AS load_date,
            load_timestamp::TIMESTAMP AS load_timestamp
        FROM raw_target_product_search
    """

    # Create or append logic
    if not tps_exists:
        # First run — create the structured table
        conn.execute(f"CREATE TABLE target_products_search AS {tps_select_query}")
        print("Created target_products_search.")
    else:
        # Before inserting, record count for logging
        before_count = conn.execute("SELECT COUNT(*) FROM target_products_search").fetchone()[0]

        # Append only new or updated rows (dedup by all non-date fields)
        conn.execute(f"""
            INSERT INTO target_products_search
            SELECT
                TRY_CAST(r.store_id AS INT) AS store_id,
                r.product_id::STRING,
                r.title::STRING,
                r.brand::STRING,
                r.price::STRING,
                r.availability::STRING,
                TRY_CAST(r.rating AS FLOAT),
                TRY_CAST(r.reviews AS INTEGER),
                r.url::STRING,
                r.category::STRING,
                r.keyword::STRING,
                r.load_date::STRING,
                r.load_timestamp::TIMESTAMP
            FROM ({tps_select_query}) AS r
            LEFT JOIN target_products_search t
            ON r.store_id = t.store_id
            AND r.product_id = t.product_id
            AND COALESCE(r.title, '') = COALESCE(t.title, '')
            AND COALESCE(r.brand, '') = COALESCE(t.brand, '')
            AND COALESCE(r.price, '') = COALESCE(t.price, '')
            AND COALESCE(r.availability, '') = COALESCE(t.availability, '')
            AND COALESCE(r.rating, -1) = COALESCE(t.rating, -1)
            AND COALESCE(r.reviews, -1) = COALESCE(t.reviews, -1)
            AND COALESCE(r.url, '') = COALESCE(t.url, '')
            AND COALESCE(r.category, '') = COALESCE(t.category, '')
            AND COALESCE(r.keyword, '') = COALESCE(t.keyword, '')
            WHERE t.store_id IS NULL
        """)

        # After insert, record new count for metrics
        after_count = conn.execute("SELECT COUNT(*) FROM target_products_search").fetchone()[0]
        inserted_rows = after_count - before_count
        print(f"Appended {inserted_rows} new or updated rows to target_products_search.")

    print("Deduplication and update complete.")

    print(f"Data successfully written to MotherDuck — {len(df)} rows.")
    conn.close()

    return (f"Loaded {len(df)} rows across {len(boston_stores)} stores.", 200)
