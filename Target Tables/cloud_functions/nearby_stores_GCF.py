import os
import time
import hashlib
from typing import List, Dict, Any

import requests
from flask import Flask, request, jsonify
import duckdb
from google.cloud import secretmanager

# ---------- Secrets (enforce GSM only, no env fallback) ----------
def _get_project_id() -> str:
    pid = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not pid:
        raise RuntimeError("GOOGLE_CLOUD_PROJECT must be set in Cloud Run.")
    return pid

def _get_secret(secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{_get_project_id()}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("utf-8")

# Load once at process start (fail fast if missing)
RAPIDAPI_HOST = "target-com-shopping-api.p.rapidapi.com"
RAPIDAPI_KEY = _get_secret("rapidapi_target_key")
MD_TOKEN     = _get_secret("project_motherduck_token")

# ---------- Config (fixed; override only if you really need) ----------
MD_CATALOG = os.getenv("MD_CATALOG", "project_882")
MD_SCHEMA  = os.getenv("MD_SCHEMA",  "main")
MD_TABLE   = os.getenv("MD_TABLE",   "store_data")

app = Flask(__name__)

# ---------- Helpers ----------
def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _address_str(a, c, s, p) -> str:
    parts = [a or "", c or "", s or "", p or ""]
    parts = [x.strip() for x in parts if x]
    return ", ".join(parts)

def _biz_key(store_id: str, address: str) -> str:
    if store_id and str(store_id).strip():
        return str(store_id)
    import hashlib as _hl
    return "addr_" + _hl.sha256((address or "").encode()).hexdigest()[:32]

def _attach_md(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"ATTACH 'md:{MD_CATALOG}' (TOKEN '{MD_TOKEN}')")

def _count_table(con: duckdb.DuckDBPyConnection) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0])

def _fetch_nearby(place: str, within: int, limit: int) -> List[Dict[str, Any]]:
    url = f"https://{RAPIDAPI_HOST}/nearby_stores"
    headers = {"x-rapidapi-host": RAPIDAPI_HOST, "x-rapidapi-key": RAPIDAPI_KEY}
    params  = {"place": place, "within": within, "limit": limit}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    stores = data.get("data", {}).get("nearby_stores", {}).get("stores", []) or []
    rows: List[Dict[str, Any]] = []
    for s in stores:
        addr = s.get("mailing_address", {}) or {}
        addr1 = addr.get("address_line1") or ""
        city  = addr.get("city") or ""
        state = addr.get("region") or ""
        postal= addr.get("postal_code") or ""
        address = _address_str(addr1, city, state, postal)
        store_id = s.get("store_id")
        dist = s.get("distance")
        try:
            dist = float(dist) if dist is not None else None
        except Exception:
            dist = None
        rows.append({
            "business_key": _biz_key(str(store_id) if store_id is not None else "", address),
            "store_id": str(store_id) if store_id is not None else None,
            "location_name": s.get("location_name"),
            "address_line1": addr1,
            "city": city, "state": state, "postal_code": postal,
            "address": address, "distance": dist,
            "phone": s.get("main_voice_phone_number"),
            "ingested_at": _now_iso(),
        })
    return rows

def _ensure_table_and_upsert(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
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
    if not rows:
        return {"inserted_or_updated": 0, "table_total_after": _count_table(con)}

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
    con.executemany("""
        INSERT INTO stage_store_data
        (business_key, store_id, location_name, address_line1, city, state, postal_code, address, distance, phone, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(
        r["business_key"], r["store_id"], r["location_name"], r["address_line1"],
        r["city"], r["state"], r["postal_code"], r["address"], r["distance"],
        r["phone"], r["ingested_at"]
    ) for r in rows])

    con.execute(f"""
        BEGIN;
        DELETE FROM {MD_SCHEMA}.{MD_TABLE} t
        USING stage_store_data s
        WHERE t.business_key = s.business_key;
        INSERT INTO {MD_SCHEMA}.{MD_TABLE}
        SELECT * FROM stage_store_data;
        COMMIT;
    """)
    return {"inserted_or_updated": len(rows), "table_total_after": _count_table(con)}

# ---------- Routes (production-minimal) ----------
@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok"})

@app.post("/ingest")
def ingest():
    """
    Body: {"place":"02215","within":10,"limit":20}
    """
    body = request.get_json(silent=True) or {}
    place = str(body.get("place"))
    within = int(body.get("within"))
    limit  = int(body.get("limit"))
    rows = _fetch_nearby(place, within, limit)
    result = _ensure_table_and_upsert(rows)
    return jsonify({"result": result})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))


