import os
import duckdb
from google.cloud import secretmanager


def get_secret(secret_id: str) -> str:
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def load_gold_layer(request):
    """
    Triggered via HTTP.
    Executes gold layer SQL to update project_882.gold.stockout_daily.
    """
    md_token = get_secret("project_motherduck_token")

    # Connect directly to your MotherDuck workspace
    conn = duckdb.connect(f"md:?motherduck_token={md_token}")
    print("✅ Connected to MotherDuck")

    # Step 1: Ensure schema/table setup
    with open("target-gold-schema-setup.sql", "r") as f:
        conn.execute(f.read())
    print("✅ Gold schema verified or created.")

    # Step 2: Run gold incremental script
    with open("target-gold-incremental.sql", "r") as f:
        conn.execute(f.read())
    print("✅ Gold layer incremental load completed.")

    # Step 3: Verify results
    count = conn.execute("""
        SELECT COUNT(*) 
        FROM project_882.gold.stockout_daily
    """).fetchone()[0]

    print(f"✅ Rows now in gold.stockout_daily: {count}")

    return ("Gold layer updated successfully in project_882.gold!", 200)
