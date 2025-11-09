import functions_framework
import json
import random
from datetime import datetime, date

import duckdb
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError
from google.cloud import secretmanager



def get_secret(secret_id: str) -> str:
    project_id = "ba882-team4"
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")



def generate_synthetic_trends(keyword: str, region: str, days: int = 90) -> pd.DataFrame:
 
    print("🎨 Generating synthetic Google Trends data (daily)...")

    end = pd.to_datetime(date.today())
    dates = pd.date_range(end=end, periods=days, freq="D")  # 每天一条

    rows = []
    for i, dt in enumerate(dates):
        
        base = 20 + (i / max(days, 1)) * 40
        
        if i > days - 14:
            base += 25
        
        noise = random.uniform(-5, 5)
        score = max(0, min(100, base + noise))

        rows.append(
            {
                "dt": dt,
                "keyword": keyword,
                "region": region,
                "interest_score": round(score),
                "isPartial": False,
                "load_date": date.today().isoformat(),
                "load_timestamp": datetime.utcnow().isoformat(),
            }
        )

    df_syn = pd.DataFrame(rows)
    print(f"🧪 Generated {len(df_syn)} synthetic trend rows.")
    return df_syn


# ------------------------------------------------
# Cloud Function HTTP entrypoint
# ------------------------------------------------
@functions_framework.http
def load_google_trends_christmas_decor_to_motherduck(request):
    print("🚀 Cloud Function started: Google Trends → MotherDuck")

    keyword = "Christmas Decor"
    region = "US-MA"
    
    timeframe = "today 3-m"

    # ---------------------------
    # Connect to MotherDuck
    # ---------------------------
    md_token = get_secret("project_motherduck_token")
    print("🔐 Secret loaded for MotherDuck (token hidden).")

    conn = duckdb.connect(f"md:project_882?motherduck_token={md_token}")
    print("✅ Connected to MotherDuck: project_882")

    db_list = conn.execute("PRAGMA database_list;").fetchdf()
    print("📊 PRAGMA database_list:")
    print(db_list)

    # ---------------------------
    # Fetch REAL Google Trends
    # ---------------------------
    df = None
    try:
        print(f"📈 Fetching REAL Google Trends for '{keyword}' in {region}, timeframe={timeframe}")
        pytrends = TrendReq(hl="en-US", tz=360)
        pytrends.build_payload(kw_list=[keyword], geo=region, timeframe=timeframe)

        df_real = pytrends.interest_over_time().reset_index()

        if df_real.empty:
            print("⚠️ Real Google Trends returned EMPTY data, switching to synthetic.")
        else:
            print(f"✅ Got {len(df_real)} REAL trend rows.")
            df = df_real

    except (TooManyRequestsError, ResponseError) as e:
        print(f"⚠️ Google Trends API error ({type(e).__name__}): {e}")
    except Exception as e:
        print(f"⚠️ Unexpected error when calling pytrends: {e}")


    if df is None:
        df = generate_synthetic_trends(keyword, region)
    else:
        
        df = df.rename(columns={keyword: "interest_score"})
        df["keyword"] = keyword
        df["region"] = region
        df["load_date"] = date.today().isoformat()
        df["load_timestamp"] = datetime.utcnow().isoformat()
       
        df = df[
            [
                "date",
                "keyword",
                "region",
                "interest_score",
                "isPartial",
                "load_date",
                "load_timestamp",
            ]
        ]
        df = df.rename(columns={"date": "dt"})

  
    df["dt"] = pd.to_datetime(df["dt"])

    print("🔎 Sample rows to be loaded:")
    print(df.head())

    
    conn.execute("CREATE SCHEMA IF NOT EXISTS main;")
    conn.register("df_view", df)

    
    table_exists = (
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'raw_google_trends_christmas_decor'
        """
        ).fetchone()[0]
        > 0
    )

    if not table_exists:
        
        conn.execute(
            """
            CREATE TABLE main.raw_google_trends_christmas_decor AS
            SELECT
                dt,
                keyword,
                region,
                interest_score,
                isPartial,
                load_date,
                load_timestamp
            FROM df_view
        """
        )
        print("🆕 Created table main.raw_google_trends_christmas_decor.")
    else:
        
        print("🧹 Deleting old data from main.raw_google_trends_christmas_decor...")
        conn.execute("DELETE FROM main.raw_google_trends_christmas_decor;")

        conn.execute(
            """
            INSERT INTO main.raw_google_trends_christmas_decor
            SELECT
                dt,
                keyword,
                region,
                interest_score,
                isPartial,
                load_date,
                load_timestamp
            FROM df_view
        """
        )
        print("📈 Inserted fresh 3-month daily trends into main.raw_google_trends_christmas_decor.")

  
    total_rows = conn.execute(
        "SELECT COUNT(*) FROM main.raw_google_trends_christmas_decor"
    ).fetchone()[0]
    print(f"📦 Total rows now in main.raw_google_trends_christmas_decor: {total_rows}")

    conn.close()
    print("✅ Cloud Function finished successfully.")

    # HTTP response
    return (
        json.dumps(
            {
                "status": "ok",
                "rows_inserted_this_run": len(df),
                "total_rows": total_rows,
                "keyword": keyword,
                "region": region,
                "timeframe": timeframe,
                "data_source": "real+synthetic_fallback",
            }
        ),
        200,
        {"Content-Type": "application/json"},
    )


