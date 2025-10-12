#!/bin/bash
set -euo pipefail

# ===== Config (edit these if needed) =====
export PROJECT_ID="$(gcloud config get-value project)"
export REGION_RUN="us-central1"          # Cloud Run region
export REGION_BQ="us"                    # BigQuery multi-region
export SERVICE="target-api-bridge"       # Cloud Run service name
export DATASET="api"                     # BigQuery dataset
export CONNECTION_NAME="api_conn"        # BigQuery connection name
export SECRET_NAME="rapidapi_target_key" # Secret name for RapidAPI key
export RAPID_KEY="e49944a0f9msh1179adbe5133021p1b92dcjsn9a49a8bebfe5"   # <— paste your RapidAPI key

# ===== Enable required services =====
gcloud services enable \
  run.googleapis.com \
  secretmanager.googleapis.com \
  bigquery.googleapis.com \
  bigqueryconnection.googleapis.com \
  cloudbuild.googleapis.com

# ===== Store RapidAPI key in Secret Manager =====
printf "%s" "$RAPID_KEY" | gcloud secrets create "$SECRET_NAME" --data-file=- 2>/dev/null || \
gcloud secrets versions add "$SECRET_NAME" --data-file=<(printf "%s" "$RAPID_KEY")

# ===== Prepare Cloud Run source =====
WORKDIR="$(mktemp -d)"
cd "$WORKDIR"

cat > main.py <<'PY'
import os, requests
from flask import Flask, request, jsonify
from google.cloud import secretmanager

app = Flask(__name__)
RAPIDAPI_HOST = "target-com-shopping-api.p.rapidapi.com"
_SECRET = None

def get_secret(name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    proj = os.environ["GOOGLE_CLOUD_PROJECT"]
    path = f"projects/{proj}/secrets/{name}/versions/latest"
    return client.access_secret_version(request={"name": path}).payload.data.decode()

@app.post("/nearby_stores")
def nearby_stores():
    global _SECRET
    if _SECRET is None:
        _SECRET = get_secret(os.environ["RAPID_SECRET_NAME"])

    body = request.get_json(silent=True) or {}
    place  = body.get("place", "10010")
    within = body.get("within", 100)
    limit  = body.get("limit", 20)

    url = "https://target-com-shopping-api.p.rapidapi.com/nearby_stores"
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": _SECRET
    }
    params  = {"place": place, "within": within, "limit": limit}

    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    rows = []
    for s in data.get("data", []):
        rows.append({
            "store_id": str(s.get("store_id")),
            "location_name": s.get("location_name"),
            "address": s.get("address"),
            "distance": float(s.get("distance") or 0),
            "phone": s.get("phone"),
        })
    return jsonify({"rows": rows})
PY

cat > requirements.txt <<'REQ'
flask
google-cloud-secret-manager
requests
REQ

echo 'web: gunicorn main:app -b 0.0.0.0:$PORT' > Procfile

# ===== Deploy Cloud Run (private) =====
gcloud run deploy "$SERVICE" \
  --source . \
  --region "$REGION_RUN" \
  --set-env-vars "RAPID_SECRET_NAME=$SECRET_NAME" \
  --no-allow-unauthenticated

SERVICE_URL="$(gcloud run services describe "$SERVICE" --region "$REGION_RUN" --format='value(status.url)')"
echo "✅ Cloud Run deployed: ${SERVICE_URL}"

# ===== Allow the default compute SA to read the secret =====
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
RUN_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
  --member="serviceAccount:${RUN_SA}" \
  --role="roles/secretmanager.secretAccessor" >/dev/null
echo "✅ Secret access granted to: ${RUN_SA}"

# ===== BigQuery dataset =====
bq --location="$REGION_BQ" mk -f --dataset "${PROJECT_ID}:${DATASET}" >/dev/null || true
echo "✅ BigQuery dataset ready: ${DATASET} (${REGION_BQ})"

# ===== BigQuery connection (us) =====
bq query --use_legacy_sql=false \
"CREATE OR REPLACE CONNECTION \`${REGION_BQ}.${CONNECTION_NAME}\`
 WITH CONNECTION_TYPE = 'CLOUD_RESOURCE'" >/dev/null

CONN_SA="$(bq show --connection --format=prettyjson "${REGION_BQ}.${CONNECTION_NAME}" | jq -r '.cloudResource.serviceAccountId')"
echo "✅ BQ Connection SA: ${CONN_SA}"

# Allow the connection SA to invoke Cloud Run
gcloud run services add-iam-policy-binding "$SERVICE" \
  --region "$REGION_RUN" \
  --member="serviceAccount:${CONN_SA}" \
  --role="roles/run.invoker" >/dev/null
echo "✅ Granted run.invoker to BQ connection SA"

# ===== BigQuery remote function =====
bq query --use_legacy_sql=false "
CREATE SCHEMA IF NOT EXISTS \`${DATASET}\`;
CREATE OR REPLACE FUNCTION \`${DATASET}.nearby_stores\`(place STRING, within INT64, limit INT64)
RETURNS ARRAY<STRUCT<
  store_id STRING,
  location_name STRING,
  address STRING,
  distance FLOAT64,
  phone STRING
>>
REMOTE WITH CONNECTION \`${REGION_BQ}.${CONNECTION_NAME}\`
OPTIONS ( endpoint = '${SERVICE_URL}/nearby_stores' );
" >/dev/null
echo "✅ Remote Function created: ${DATASET}.nearby_stores"

# ===== Test query =====
echo "▶ Running test query..."
bq query --use_legacy_sql=false "
SELECT *
FROM UNNEST(\`${DATASET}.nearby_stores\`('10010', 100, 10))
ORDER BY distance
LIMIT 10;
"
echo "🎉 Done. Try it in BigQuery:
SELECT * FROM UNNEST(\`${DATASET}.nearby_stores\`('02134', 50, 20));"
