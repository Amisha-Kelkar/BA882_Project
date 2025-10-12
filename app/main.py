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
    headers = {"x-rapidapi-host": RAPIDAPI_HOST, "x-rapidapi-key": _SECRET}
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
