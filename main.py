import os
import time
import hashlib
from typing import List, Dict, Any

import requests
from flask import Flask, request, jsonify
import duckdb

# -------------------------------
# Config (env-driven)
# -------------------------------
RAPIDAPI_HOST = "target-com-shopping-api.p.rapidapi.com"
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")  # set in Cloud Run env

# MotherDuck config
MD_TOKEN = os.getenv("MOTHERDUCK_TOKEN")  # mounted from Secret Manager in Cloud Run
MD_CATALOG = os.getenv("MD_CATALOG", "project_data")
MD_SCHEMA = os.getenv("MD_SCHEMA", "raw_data")
MD_TABLE = os.getenv("MD_TABLE", "store_data")

# Simple defaults to help ad-hoc GET tests
DEFAULT_PLACE = os.getenv("PLACE", "10010")
DEFAULT_WITHIN = int(os.getenv("WITHIN", "100"))
DEFAULT_LIMIT = int(os.getenv("LIMIT", "20"))

app = Flask(__name__)


# -------------------------------
# Helpers
# -------------------------------
def _require_env(var: str) -> str:
    val = os.getenv(var)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {var}")
    return val


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _address_str(addr1: str, city: str, state: str, postal: str) -> str:
    parts = [addr1 or "", city or "", state or "", postal or ""]
    parts = [p.strip() for p in parts if p is not None]
    return ", ".join([p for p in parts if p])


def _biz_key(store_id: str, address: str) -> str:
    """Primary business key: store_id if present, otherwise stable hash of address."""
    if store_id and str(store_id).strip():
        return str(store_id)
    h = hashlib.sha256((address or "").encode("utf-8")).hexdigest()[:32]
    return f"addr_{h}"


def fetch_nearby_stores(place_zip: str, within_miles: int, max_results: int) -> List[Dict[str, Any]]:
    """Call RapidAPI Target nearby_stores and normalize rows."""
    key = _require_env("RAPIDAPI_KEY")

    url = f"https://{RAPIDAPI_HOST}/nearby_stores"
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": key,
    }
    params = {
        "place": place_zip,
        "within": within_miles,
        "limit": max_results,
    }

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    rows: List[Dict[str, Any]] = []
    stores = (
        data.get("data", {})
            .get("nearby_stores", {})
            .get("stores", [])
    )

    for s in stores:
        addr = s.get("mailing_address", {}) or {}
        address_line1 = addr.get("address_line1") or ""
        city = addr.get("city") or ""
        state = addr.get("region") or ""
        postal = addr.get("postal_code") or ""
        address = _address_str(address_line1, city, state, postal)

        store_id = s.get("store_id")
        loc_name = s.get("location_name")
        phone = s.get("main_voice_phone_number")
        distance = s.get("distance")
        try:
            distance = float(distance) if distance is not None else None
        except Exception:
            distance = None

        row = {
            "business_key": _biz_key(str(store_id) if store_id is not None else "", address),
            "store_id": str(store_id) if store_id is not None else None,
            "location_name": loc_name,
            "address_line1": address_line1,
            "city": city,
            "state": state,
            "postal_code": postal,
            "address": address,
            "distance": distance,
            "phone": phone,
            "ingested_at": _now_iso(),
        }
        rows.append(row)

    return rows


def _attach_md(con: duckdb.DuckDBPyConnection) -> None:
    token = _require_env("MOTHERDUCK_TOKEN")
    con.execute(f"ATTACH 'md:{MD_CATALOG}' (TOKEN '{token}')")


def ensure_table_and_upsert(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create table if needed, then upsert rows via delete+insert."""
    if not rows:
        # still ensure table exists
        con = duckdb.connect(database=":memory:")
        _attach_md(con)
        con.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {MD_SCHEMA};
            CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} (
                business_key   VARCHAR PRIMARY KEY,
                store_id       VARCHAR,
                location_name  VARCHAR,
                address_line1  VARCHAR,
                city           VARCHAR,
                state          VARCHAR,
                postal_code    VARCHAR,
                address        VARCHAR,
                distance       DOUBLE,
                phone          VARCHAR,
                ingested_at    TIMESTAMP
            );
        """)
        return {"inserted_or_updated": 0, "table_total_after": _count_table(con)}
    # Create an in-memory DuckDB connection, attach MD and run SQL there
    con = duckdb.connect(database=":memory:")
    _attach_md(con)

    # Ensure target table exists
    con.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {MD_SCHEMA};
        CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} (
            business_key   VARCHAR PRIMARY KEY,
            store_id       VARCHAR,
            location_name  VARCHAR,
            address_line1  VARCHAR,
            city           VARCHAR,
            state          VARCHAR,
            postal_code    VARCHAR,
            address        VARCHAR,
            distance       DOUBLE,
            phone          VARCHAR,
            ingested_at    TIMESTAMP
        );
    """)

    # Create a temp staging table and load rows
    con.execute("""
        CREATE TEMPORARY TABLE stage_store_data AS
        SELECT * FROM (SELECT
            ''::VARCHAR AS business_key,
            ''::VARCHAR AS store_id,
            ''::VARCHAR AS location_name,
            ''::VARCHAR AS address_line1,
            ''::VARCHAR AS city,
            ''::VARCHAR AS state,
            ''::VARCHAR AS postal_code,
            ''::VARCHAR AS address,
            NULL::DOUBLE AS distance,
            ''::VARCHAR AS phone,
            now()::TIMESTAMP AS ingested_at
        ) WHERE 1=0;
    """)

    # Insert rows using parameterized approach
    insert_sql = """
        INSERT INTO stage_store_data
        (business_key, store_id, location_name, address_line1, city, state, postal_code, address, distance, phone, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        (
            r.get("business_key"),
            r.get("store_id"),
            r.get("location_name"),
            r.get("address_line1"),
            r.get("city"),
            r.get("state"),
            r.get("postal_code"),
            r.get("address"),
            r.get("distance"),
            r.get("phone"),
            r.get("ingested_at"),
        )
        for r in rows
    ]
    con.executemany(insert_sql, params)

    # Upsert via delete + insert (works across DuckDB versions)
    con.execute(f"""
        BEGIN TRANSACTION;

        DELETE FROM {MD_SCHEMA}.{MD_TABLE} t
        USING stage_store_data s
        WHERE t.business_key = s.business_key;

        INSERT INTO {MD_SCHEMA}.{MD_TABLE}
        SELECT * FROM stage_store_data;

        COMMIT;
    """)

    total = _count_table(con)
    return {"inserted_or_updated": len(rows), "table_total_after": total}


def _count_table(con: duckdb.DuckDBPyConnection) -> int:
    cur = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}")
    return int(cur.fetchone()[0])


# -------------------------------
# Routes
# -------------------------------
@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok", "catalog": MD_CATALOG, "schema": MD_SCHEMA, "table": MD_TABLE})


@app.get("/debug_token")
def debug_token():
    tok = os.getenv("MOTHERDUCK_TOKEN")
    if not tok:
        return jsonify({"env_present": False})
    head = tok[:12]
    tail = tok[-12:]
    return jsonify({"env_present": True, "head": head, "len": len(tok), "tail": tail})


@app.get("/debug_md")
def debug_md():
    return jsonify({"catalog": MD_CATALOG, "schema": MD_SCHEMA, "table": MD_TABLE})


@app.get("/nearby_stores")
def nearby_stores_get():
    place = request.args.get("place", DEFAULT_PLACE)
    within = int(request.args.get("within", DEFAULT_WITHIN))
    limit = int(request.args.get("limit", DEFAULT_LIMIT))
    try:
        rows = fetch_nearby_stores(place, within, limit)
        return jsonify({"params": {"place": place, "within": within, "limit": limit}, "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.post("/nearby_stores")
def nearby_stores_post():
    body = request.get_json(silent=True) or {}
    place = str(body.get("place", DEFAULT_PLACE))
    within = int(body.get("within", DEFAULT_WITHIN))
    limit = int(body.get("limit", DEFAULT_LIMIT))
    try:
        rows = fetch_nearby_stores(place, within, limit)
        return jsonify({"params": {"place": place, "within": within, "limit": limit}, "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.post("/ingest")
def ingest():
    """
    Body: {"place":"02215","within":50,"limit":20}
    Writes into {MD_CATALOG}.{MD_SCHEMA}.{MD_TABLE}
    """
    body = request.get_json(silent=True) or {}
    place = str(body.get("place", DEFAULT_PLACE))
    within = int(body.get("within", DEFAULT_WITHIN))
    limit = int(body.get("limit", DEFAULT_LIMIT))

    try:
        rows = fetch_nearby_stores(place, within, limit)
        result = ensure_table_and_upsert(rows)
        return jsonify({
            "params": {"place": place, "within": within, "limit": limit},
            "result": result
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.post("/")
def bq_remote_function():
    """
    BigQuery Remote Function protocol:
    Input:  {"calls":[["02215",50,20]]}
    Output: {"replies":[ [ {...}, {...} ] ]}
    """
    body = request.get_json(silent=True) or {}
    try:
        calls = body.get("calls", [])
        if not calls or not isinstance(calls[0], list):
            raise ValueError("Invalid 'calls' payload")

        place = str(calls[0][0])
        within = int(calls[0][1])
        limit = int(calls[0][2])

        rows = fetch_nearby_stores(place, within, limit)
        return jsonify({"replies": [rows]})
    except Exception as e:
        return jsonify({"errorMessage": str(e), "replies": [[]]}), 400


# -------------------------------
# Entrypoint (for local run)
# -------------------------------
if __name__ == "__main__":
    # For local debugging only; Cloud Run uses gunicorn
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

