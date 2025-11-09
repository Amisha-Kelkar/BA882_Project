import os
import time
import duckdb
from google.cloud import secretmanager

def get_secret(secret_id: str) -> str:
    """Fetch MotherDuck token securely from Secret Manager"""
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def load_gold_layer(request):
    """
    Triggered via HTTP.
    Runs the GOLD LAYER SQL to create/refresh stockout_daily table in MotherDuck.
    """
    md_token = get_secret("project_motherduck_token")
    conn = duckdb.connect(f"md:?motherduck_token={md_token}")
    print("✅ Connected to MotherDuck")

    # Run schema setup (idempotent)
    with open("target-gold-schema-setup.sql", "r") as f:
        schema_sql = f.read()
    conn.execute(schema_sql)
    print("✅ Gold schema verified/created")

    # Run incremental gold layer logic
    with open("target-gold-incremental.sql", "r") as f:
        gold_sql = f.read()
    conn.execute(gold_sql)
    print("✅ Gold layer incremental load completed")

    return ("Gold layer updated successfully!", 200)
