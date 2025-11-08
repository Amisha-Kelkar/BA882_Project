import os
import json
import time
import pandas as pd
import duckdb
import http.client
import numpy as np
from datetime import datetime, date, timedelta
from google.cloud import secretmanager


def get_secret(secret_id: str) -> str:
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def silver_table_join(request):
    """
    Triggered via HTTP.
    Incrementally updates the silver layer with only new records since the last run.
    """
    md_token = get_secret("project_motherduck_token")
    conn = duckdb.connect(f"md:?motherduck_token={md_token}")
    print("Connected to MotherDuck")

    # --- Get last record_date from silver table (if it exists)
    try:
        last_date = conn.execute("""
            SELECT MAX(record_date) FROM project_882.main.silver_table
        """).fetchone()[0]
        if last_date:
            print(f"Last run had record_date up to: {last_date}")
        else:
            last_date = '1900-01-01'
    except Exception:
        # Silver table doesn't exist yet
        last_date = '1900-01-01'

    # --- Read your SQL template and inject incremental filter
    with open("silver_table.sql", "r") as f:
        sql_script = f.read()

    # Add WHERE filter dynamically to read only new data
    sql_script = sql_script.replace(
        "--INCREMENTAL_FILTER--",
        f"WHERE f.record_date > DATE '{last_date}'"
    )

    # --- Run the SQL incrementally
    conn.execute(sql_script)
    print("Silver layer incremental load completed")

    return ("Silver table updated with new records.", 200)
