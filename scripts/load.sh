#!/bin/bash
# Usage: ./load_target_api.sh <ZIP> <WITHIN_MILES> <LIMIT>
# Example: ./load_target_api.sh 10010 100 20
set -euo pipefail

ZIP="${1:-10010}"
WITHIN="${2:-100}"
LIMIT="${3:-20}"

API_URL="https://target-api-bridge-756516792287.us-central1.run.app/nearby_stores?place=${ZIP}&within=${WITHIN}&limit=${LIMIT}"
WAREHOUSE_FILE="target_warehouse.duckdb"
RAW_JSON="api_data.json"
ROWS_NDJSON="rows.ndjson"

echo "🔄 Fetching: $API_URL"
# -s = silent, -S = show errors, -f = fail on HTTP 4xx/5xx
if ! curl -sSf "$API_URL" -o "$RAW_JSON"; then
  echo "❌ HTTP error calling API. Check your key/quota."
  exit 1
fi

# Does the payload contain a .rows array?
if ! jq -e 'has("rows") and (.rows | type=="array")' "$RAW_JSON" > /dev/null; then
  echo "❌ API returned no 'rows' array."
  echo "ℹ️ Error detail:"
  jq -r '.error // .errorMessage // .message // "Unknown error"' "$RAW_JSON"
  exit 1
fi

# Convert the array to newline-delimited JSON for robust DuckDB ingest
jq -c '.rows[]' "$RAW_JSON" > "$ROWS_NDJSON"

echo "📦 Loading into DuckDB: $WAREHOUSE_FILE"
duckdb "$WAREHOUSE_FILE" <<SQL
DROP TABLE IF EXISTS target_stores;
CREATE TABLE target_stores AS
SELECT * FROM read_json_auto('$ROWS_NDJSON');
SQL

echo "✅ Done. Row count:"
duckdb "$WAREHOUSE_FILE" -c "SELECT COUNT(*) AS row_count FROM target_stores;"
echo "👀 Sample:"
duckdb "$WAREHOUSE_FILE" -c "SELECT * FROM target_stores LIMIT 5;"
