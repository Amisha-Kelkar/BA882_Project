import os
import json
from datetime import datetime

import requests
import duckdb
import functions_framework
from google.cloud import secretmanager
from google import genai


# ----------------------------------------------------------
# Secret Manager Loader
# ----------------------------------------------------------

def get_secret(secret_id: str) -> str:
    """Fetch secret value from Secret Manager."""
    project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    resp = client.access_secret_version(request={"name": name})
    return resp.payload.data.decode("utf-8")


# ----------------------------------------------------------
# Load Secrets
# ----------------------------------------------------------

SLACK_BOT_TOKEN = get_secret("slack-bot-token-secret")
MD_TOKEN = get_secret("project_motherduck_token")


# ----------------------------------------------------------
# Load Environment Variables
# ----------------------------------------------------------

SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#ba882-stockout_alert")
MOTHERDUCK_DB = os.environ.get("MOTHERDUCK_DB", "project_882")
GCP_PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
GCP_LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
SLACK_BOT_USER_ID = os.environ.get("SLACK_BOT_USER_ID")  # optional


# ----------------------------------------------------------
# Gemini Client
# ----------------------------------------------------------

client = genai.Client(
    vertexai=True,
    project=GCP_PROJECT,
    location=GCP_LOCATION,
)

def safe_get_llm_text(response):
    if hasattr(response, "candidates"):
        # google-genai format
        try:
            return response.candidates[0].content.parts[0].text
        except:
            return None

    if hasattr(response, "text"):
        return response.text

    return None


# ----------------------------------------------------------
# MotherDuck
# ----------------------------------------------------------

def get_md_connection():
    dsn = f"md:{MOTHERDUCK_DB}?motherduck_token={MD_TOKEN}"
    return duckdb.connect(dsn)


def get_new_alert_rows(con):
    """Return rows not yet alerted + proba>=0.80 + flag=1."""
    return con.execute("""
        SELECT
            c.store_id,
            c.product_id,
            c.event_date,
            c.stockout_label,
            c.predicted_stockout_proba,
            c.predicted_stockout_flag
        FROM project_882.model_outputs.stockout_alert_candidates c
        LEFT JOIN project_882.model_outputs.stockout_alert_log l
              ON c.store_id = l.store_id
             AND c.product_id = l.product_id
             AND c.event_date = l.event_date
        WHERE l.store_id IS NULL
          AND c.predicted_stockout_flag = 1
          AND c.predicted_stockout_proba >= 0.80;
    """).fetchall()


# ----------------------------------------------------------
# LLM Summary
# ----------------------------------------------------------

def generate_llm_summary(store, product, date, proba, flag):
    prompt = f"""
    Generate a short stock-out alert:

    Store={store}
    Product={product}
    Date={date}
    Probability={proba:.2f}
    Flag={flag}

    Use simple language + next action.
    """

    try:
        print("Calling Vertex AI…")  

        response = client.models.generate_content(
            model="gemini-2.0-flash-001", #gemini-1.5-flash",       
            contents=prompt,
            config={"temperature": 0.3, "max_output_tokens": 60},
        )

        print(f"Raw LLM response: {response}")  

        text = safe_get_llm_text(response)
        print(f"Extracted text: {text}")        

        if text:
            return text.strip()
        else:
            return "Stockout risk detected (no text returned)."

    except Exception as e:
        print(f"LLM ERROR: {e}")
        return f"Stockout risk detected (LLM error: {e})"



# ----------------------------------------------------------
# Slack Sender
# ----------------------------------------------------------

def slack_post_message(channel, text):
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json;charset=utf-8"
    }
    payload = {"channel": channel, "text": text}

    rsp = requests.post(url, json=payload, headers=headers)
    data = rsp.json()

    if not data.get("ok"):
        
        if data.get("error") == "ratelimited":
            return f"Slack rate-limited: {data}"
        raise RuntimeError(f"Slack error: {data}")


# ----------------------------------------------------------
# Alert Pipeline 
# ----------------------------------------------------------

def run_alert_pipeline():
    con = get_md_connection()
    try:
        rows = get_new_alert_rows(con)
        if rows:
            for row in rows:
                store, product, date, _, proba, flag = row
                summary = generate_llm_summary(store, product, date, proba, flag)

                msg = (
                    f"🚨 *Stock-out Alert*\n"
                    f"{summary}\n\n"
                    f"_Details:_ store={store}, product={product}, "
                    f"date={date}, prob={proba:.2f}, flag={flag}"
                )

                slack_post_message(SLACK_CHANNEL, msg)

            # Log alerts
            now = datetime.utcnow()
            for store, product, date, *_ in rows:
                con.execute("""
                    INSERT INTO project_882.model_outputs.stockout_alert_log
                    VALUES (?, ?, ?, ?)
                """, [store, product, date, now])

        return {"alerts_sent": len(rows)}
    finally:
        con.close()


# ----------------------------------------------------------
# MAIN — Cloud Function Entrypoint
# ----------------------------------------------------------

@functions_framework.http
def slack_bot(request):
    body = request.get_json(silent=True)

   
    if request.method == "GET" and request.args.get("run") == "alert":
        result = run_alert_pipeline()
        return json.dumps(result), 200

    # ------------------------------------------------------
    # Slack verification
    # ------------------------------------------------------
    if body and body.get("type") == "url_verification":
        return body["challenge"], 200

    # ------------------------------------------------------
    # Fallback
    # ------------------------------------------------------
    return "ok", 200
