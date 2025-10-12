import hashlib
import os
from typing import List, Dict, Any

import requests
import pandas as pd
import duckdb
from flask import Flask, request, jsonify

app = Flask(__name__)

# ========= 基本配置 =========
RAPIDAPI_HOST = "target-com-shopping-api.p.rapidapi.com"

# 默认查询参数（可被 query/body 覆盖）
DEFAULT_PLACE  = "10010"
DEFAULT_WITHIN = 100
DEFAULT_LIMIT  = 20

# RapidAPI Key：优先环境变量，兜底为示例占位
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "e1eee8a0d8msh13230a214c849c3p15c396jsne1c69be2b4f4").strip()

# MotherDuck Token：优先环境变量（去除可能的换行/空白），否则用你的 fallback
MD_TOKEN = (os.getenv("MOTHERDUCK_TOKEN") or
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJlbWFpbCI6InloNDU4N2FAc3R1ZGVudC5hbWVyaWNhbi5lZHUiLCJzZXNzaW9uIjoieWg0NTg3YS5z"
            "dHVkZW50LmFtZXJpY2FuLmVkdSIsInBhdCI6IllKNG9zLTBUYWU0Yk9PcHhqVDh5ODNsRFliRjBiSWhp"
            "RHB4ZUlFV0tESXMiLCJ1c2VySWQiOiIwZDViYjlkNy03NjlmLTRlYTAtYWY4Yi04NjAxMTkyYmM3MzQi"
            "LCJpc3MiOiJtZF9wYXQiLCJyZWFkT25seSI6ZmFsc2UsInRva2VuVHlwZSI6InJlYWRfd3JpdGUiLCJp"
            "YXQiOjE3NjAzMDA2MTN9."
            "ILEMr6gFxdee6ZsR72q7uHXtBiXs0NQaufiw_Mxc2P4"
           ).strip()

# MotherDuck 目标位置
MD_CATALOG = "project_data"
MD_SCHEMA  = "raw_data"
MD_TABLE   = "store_data"

# 目标表列定义（不含经纬度）
TARGET_COLUMNS: Dict[str, str] = {
    "business_key":  "VARCHAR",
    "store_id":      "VARCHAR",
    "location_name": "VARCHAR",
    "address_line1": "VARCHAR",
    "city":          "VARCHAR",
    "state":         "VARCHAR",
    "postal_code":   "VARCHAR",
    "address":       "VARCHAR",
    "distance":      "DOUBLE",
    "phone":         "VARCHAR",
    "ingested_at":   "TIMESTAMP",
}
TARGET_COL_LIST = list(TARGET_COLUMNS.keys())
# ===========================


# ---------- 调 RapidAPI: Target nearby_stores ----------
def fetch_nearby_stores(place_zip: str, within_miles: int, limit: int) -> List[Dict[str, Any]]:
    url = "https://target-com-shopping-api.p.rapidapi.com/nearby_stores"
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
        "accept": "application/json",
    }
    params = {"place": place_zip, "within": within_miles, "limit": limit}

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    stores = ((data or {}).get("data", {}).get("nearby_stores", {}).get("stores", []))
    rows = []
    for s in stores:
        addr = s.get("mailing_address", {}) or {}
        rows.append({
            "store_id": str(s.get("store_id") or ""),
            "location_name": s.get("location_name"),
            "address_line1": addr.get("address_line1"),
            "city": addr.get("city"),
            "state": addr.get("region"),
            "postal_code": addr.get("postal_code"),
            "address": f"{addr.get('address_line1','')}, {addr.get('city','')}, "
                       f"{addr.get('region','')} {addr.get('postal_code','')}".strip(", "),
            "distance": float(s.get("distance") or 0),
            "phone": s.get("main_voice_phone_number"),
        })
    return rows


# ---------- MotherDuck：建表 + 自动迁移 + 伪 Upsert（DELETE+INSERT） ----------
def ensure_table_and_upsert(rows: List[Dict[str, Any]]):
    if not rows:
        return {"inserted_or_updated": 0, "table_total_after": None, "batch_rows": 0}

    df = pd.DataFrame(rows)
    df["ingested_at"] = pd.Timestamp.utcnow()

    # 业务主键：优先 store_id；否则地址要素哈希（不含经纬度）
    def mk_key(row):
        sid = (row.get("store_id") or "").strip()
        if sid:
            return sid
        base = "|".join(str(row.get(k, "") or "") for k in
                        ["address_line1", "city", "state", "postal_code"])
        return hashlib.md5(base.encode("utf-8")).hexdigest()

    df["business_key"] = df.apply(mk_key, axis=1)

    con = duckdb.connect(f"md:{MD_CATALOG}", config={"motherduck_token": MD_TOKEN})

    # 1) 确保 schema
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {MD_SCHEMA};")

    # 2) 如表不存在则按目标结构创建
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} (
        {", ".join([f"{col} {typ}" for col, typ in TARGET_COLUMNS.items()])}
    );
    """)

    # 3) 自动迁移：补齐缺列（只按 TARGET_COLUMNS）
    existing_cols = [r[1] for r in con.execute(f"PRAGMA table_info('{MD_SCHEMA}.{MD_TABLE}')").fetchall()]
    for col, typ in TARGET_COLUMNS.items():
        if col not in existing_cols:
            con.execute(f"ALTER TABLE {MD_SCHEMA}.{MD_TABLE} ADD COLUMN {col} {typ};")
            existing_cols.append(col)

    # 4) 注册批次，确保列顺序与目标一致
    for col in TARGET_COL_LIST:
        if col not in df.columns:
            df[col] = None
    df = df[TARGET_COL_LIST].copy()

    con.register("new_batch", df)
    con.execute(f"""
    CREATE TEMP TABLE _staging AS
    SELECT {", ".join(TARGET_COL_LIST)} FROM new_batch;
    """)

    # 5) 事务：先按 store_id 清理，再按 business_key 清理，最后插入
    con.execute("BEGIN;")
    if "store_id" in existing_cols:
        con.execute(f"""
        DELETE FROM {MD_SCHEMA}.{MD_TABLE}
        WHERE store_id IS NOT NULL AND store_id <> ''
          AND store_id IN (
            SELECT store_id FROM _staging
            WHERE store_id IS NOT NULL AND store_id <> ''
          );
        """)
    if "business_key" in existing_cols:
        con.execute(f"""
        DELETE FROM {MD_SCHEMA}.{MD_TABLE}
        WHERE business_key IS NOT NULL AND business_key <> ''
          AND business_key IN (
            SELECT business_key FROM _staging
            WHERE business_key IS NOT NULL AND business_key <> ''
          );
        """)
    con.execute(f"""
    INSERT INTO {MD_SCHEMA}.{MD_TABLE} ({", ".join(TARGET_COL_LIST)})
    SELECT {", ".join(TARGET_COL_LIST)} FROM _staging;
    """)
    con.execute("COMMIT;")

    total = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE};").fetchone()[0]
    return {"inserted_or_updated": len(df), "table_total_after": total, "batch_rows": len(df)}


# ---------------- Flask Endpoints ----------------
@app.get("/healthz")
def healthz():
    return jsonify({
        "status": "ok",
        "catalog": MD_CATALOG,
        "schema": MD_SCHEMA,
        "table": MD_TABLE
    })

@app.get("/debug_token")
def debug_token():
    tok_env = os.getenv("MOTHERDUCK_TOKEN")
    eff = (tok_env or MD_TOKEN or "").strip()
    return jsonify({
        "env_present": tok_env is not None,
        "len": len(eff),
        "head": eff[:12],
        "tail": eff[-12:]
    })

@app.get("/nearby_stores")
def nearby_stores_get():
    place = request.args.get("place", DEFAULT_PLACE)
    within = int(request.args.get("within", DEFAULT_WITHIN))
    limit  = int(request.args.get("limit",  DEFAULT_LIMIT))
    try:
        rows = fetch_nearby_stores(place, within, limit)
        return jsonify({"params": {"place": place, "within": within, "limit": limit},
                        "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.post("/ingest")
def ingest_post():
    body = request.get_json(silent=True) or {}
    place = str(body.get("place", DEFAULT_PLACE))
    within = int(body.get("within", DEFAULT_WITHIN))
    limit  = int(body.get("limit",  DEFAULT_LIMIT))
    try:
        rows = fetch_nearby_stores(place, within, limit)
        result = ensure_table_and_upsert(rows)
        return jsonify({"params": {"place": place, "within": within, "limit": limit},
                        "result": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.post("/")
def bq_remote_function():
    # BigQuery Remote Function 协议（仅返回数据，不入库）
    body = request.get_json(silent=True) or {}
    try:
        calls = body.get("calls", [])
        if not calls or not isinstance(calls[0], list):
            raise ValueError("Invalid 'calls' payload")
        place_zip    = str(calls[0][0])
        within_miles = int(calls[0][1])
        max_results  = int(calls[0][2])
        rows = fetch_nearby_stores(place_zip, within_miles, max_results)
        return jsonify({"replies": [rows]})
    except Exception as e:
        return jsonify({"errorMessage": str(e), "replies": [[]]}), 400


# 本地运行入口（Cloud Run 用 gunicorn）
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
