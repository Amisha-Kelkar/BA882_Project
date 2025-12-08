import os
import re
import duckdb
import pandas as pd
import google.generativeai as genai
import traceback
from flask import Flask, request, jsonify, render_template_string
from datetime import date

# -------------------------------------------------------------------
# 1. Configuration
# -------------------------------------------------------------------
app = Flask(_name_)
api_key = os.environ.get("GEMINI_API_KEY")

if not api_key:
    print("CRITICAL: GEMINI_API_KEY is missing. Please set it in your environment variables.")

# Configure Gemini
if api_key:
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("models/gemini-2.5-flash")

# -------------------------------------------------------------------
# 2. Database & Schema Logic
# -------------------------------------------------------------------
MD_DB = "md:project_882"

# COMPREHENSIVE SCHEMA DEFINITION based on your CSVs
HARDCODED_SCHEMA = """
TABLE: v_stockout_predictions
COLUMNS: store_id (BIGINT), product_id (BIGINT), pred_date (DATE), stockout_prob (DOUBLE), predicted_flag (BIGINT), actual_stockout (BOOLEAN), title (VARCHAR), keyword (VARCHAR), price_usd (DOUBLE), rating (DOUBLE), reviews (BIGINT), store_name (VARCHAR), city (VARCHAR), state (VARCHAR)
DESCRIPTION: Main prediction table. Use for "future risk", "stockout probability", and general risk queries. High risk is > 0.7.

TABLE: v_replenishment_now
COLUMNS: store_id (BIGINT), store_name (VARCHAR), product_id (BIGINT), title (VARCHAR), pred_date (DATE), stockout_prob (DOUBLE), predicted_flag (BIGINT), available_to_promise_qty (DOUBLE)
DESCRIPTION: Items that need immediate attention/replenishment.

TABLE: v_store_risk_summary
COLUMNS: store_id (BIGINT), store_name (VARCHAR), state (VARCHAR), avg_stockout_prob (DOUBLE), high_risk_skus (BIGINT)
DESCRIPTION: Aggregated risk metrics by store. Use for "which store is worst" type questions.

TABLE: v_product_risk_summary
COLUMNS: product_id (BIGINT), title (VARCHAR), keyword (VARCHAR), avg_stockout_prob (DOUBLE), max_stockout_prob (DOUBLE), stores_at_risk (BIGINT)
DESCRIPTION: Aggregated risk metrics by product. Use for "which product is worst" type questions.

TABLE: v_geo_risk_state
COLUMNS: state (VARCHAR), avg_stockout_prob (DOUBLE), high_risk_pairs (BIGINT)
DESCRIPTION: Risk aggregated by State (e.g., MA, NY).

TABLE: fact_fulfillment
COLUMNS: store_id (BIGINT), product_id (BIGINT), record_date (DATE), shipping_availability_status (VARCHAR), available_to_promise_qty (DOUBLE), is_sold_out (BOOLEAN)
DESCRIPTION: Daily inventory snapshot. Use for "current stock", "quantity on hand", "ATP".

TABLE: v_price_promo_weekly
COLUMNS: wk (DATE), product_id (BIGINT), avg_price (DOUBLE), avg_atp (DOUBLE), sold_out_rate (DOUBLE)
DESCRIPTION: Weekly trends on price and stockouts.

TABLE: stockout_alert_log
COLUMNS: store_id (BIGINT), product_id (BIGINT), event_date (DATE), alert_sent_at (TIMESTAMP)
DESCRIPTION: Log of alerts already sent.

TABLE: dim_stores
COLUMNS: store_id (BIGINT), store_name (VARCHAR), city (VARCHAR), state (VARCHAR), zip (VARCHAR), address (VARCHAR)
DESCRIPTION: Detailed store metadata.

TABLE: dim_products
COLUMNS: product_id (BIGINT), title (VARCHAR), price_usd (DOUBLE), rating (DOUBLE), reviews (BIGINT), keyword (VARCHAR)
DESCRIPTION: Detailed product metadata.
"""

def get_connection():
    """
    Connects to MotherDuck. 
    Includes fallback to local memory to prevent crash if network fails.
    """
    try:
        con = duckdb.connect(MD_DB)
        con.execute("PRAGMA search_path = 'main'") 
        return con
    except Exception as e:
        print(f"MotherDuck Connection Failed: {e}")
        print("Falling back to local in-memory DuckDB (Empty) for safety.")
        return duckdb.connect()

def get_logical_date(con):
    """Finds the latest prediction date to ground 'today'."""
    try:
        # Use v_stockout_predictions as the anchor
        query = "SELECT MAX(TRY_CAST(pred_date AS DATE)) FROM v_stockout_predictions"
        result = con.execute(query).fetchone()
        if result and result[0]:
            return result[0]
    except Exception:
        pass
    return date.today()

def get_schema_summary(con):
    """Returns the hardcoded schema to ensure the LLM has perfect context."""
    return HARDCODED_SCHEMA

def clean_sql(text):
    """Extracts clean SQL from LLM response."""
    # First try to extract from a sql ...  fenced block
    match = re.search(r"sql(.*?)", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    match = re.search(r'(?i)\b(SELECT|WITH)\b.*', text, re.DOTALL)
    if match:
        sql = match.group(0).strip()
        if ";" in sql:
            sql = sql[:sql.find(";")+1]
        return sql
        
    return text.strip()

# -------------------------------------------------------------------
# 3. The Logic Loop
# -------------------------------------------------------------------
def generate_and_execute_sql(question, con, schema_text, logical_date):
    
    # PROMPT 1: Generate SQL
    system_prompt = f"""
    You are a DuckDB SQL Expert for a Retail Inventory AI.
    
    CONTEXT:
    - Reference Date: {logical_date} (Use this as 'today').
    - Database: MotherDuck / DuckDB.
    
    AVAILABLE TABLES:
    {schema_text}
    
    USER QUESTION: "{question}"
    
    GUIDELINES:
    1. *Risk Queries*: Use v_stockout_predictions. High risk is stockout_prob > 0.7.
    2. *Store Aggregates*: Use v_store_risk_summary for "worst stores".
    3. *Product Aggregates*: Use v_product_risk_summary for "worst products".
    4. *Inventory Levels*: Use fact_fulfillment or v_replenishment_now for quantity (available_to_promise_qty).
    5. *Joins*: 
       - Join dim_products on product_id for titles.
       - Join dim_stores on store_id for store names/locations.
    6. *Dates*: pred_date and record_date are Dates. Use TRY_CAST if comparing strings.
    7. *Output*: Write ONLY valid DuckDB SQL.
    """
    
    try:
        resp = model.generate_content(system_prompt)
        candidate_sql = clean_sql(resp.text)
        print(f"DEBUG SQL (Attempt 1): {candidate_sql}")
    except Exception as e:
        return None, "", f"LLM Generation Error: {e}"
    
    # 2. Execution & Repair Loop
    max_retries = 2
    last_error = None
    
    for i in range(max_retries + 1):
        try:
            # Attempt execution
            df = con.execute(candidate_sql).fetchdf()
            return df, candidate_sql, None # Success!
            
        except Exception as e:
            last_error = str(e)
            print(f"DEBUG ERROR (Attempt {i+1}): {last_error}")
            # print(traceback.format_exc()) # Uncomment for deep debugging
            
            if i < max_retries:
                fix_prompt = f"""
                Your SQL query failed. Fix it.
                
                QUESTION: {question}
                FAILED SQL: {candidate_sql}
                ERROR: {last_error}
                
                SCHEMA HINT: Use the specific tables provided in the original prompt.
                OUTPUT: ONLY the fixed SQL.
                """
                try:
                    resp = model.generate_content(fix_prompt)
                    candidate_sql = clean_sql(resp.text)
                    print(f"DEBUG SQL (Fix {i+1}): {candidate_sql}")
                except:
                    break

    return None, candidate_sql, last_error

def run_analytics_question(question):
    con = get_connection()
    try:
        # 1. Prep
        logical_date = get_logical_date(con)
        schema_text = get_schema_summary(con)
        
        # 2. Run
        df, final_sql, error = generate_and_execute_sql(question, con, schema_text, logical_date)
        
        # 3. Fail State
        if error:
            return f"*Analysis Failed*\n\nI tried to run:\n`{final_sql}\n\n**Error:**\n{error}`"
            
        if df is None or df.empty:
            return f"Analysis complete. No data found for your specific criteria.\n(Reference Date: {logical_date})"
            
        # 4. Summarize Success
        summary_prompt = f"""
        You are a Retail Operations Analyst.
        
        User Question: "{question}"
        Reference Date: {logical_date}
        
        SQL Results (Top 20 rows):
        {df.head(20).to_string()}
        
        Task: 
        1. Answer the user's question directly based on the data.
        2. Highlight key product names, store names, or risk probabilities.
        3. Keep it professional and concise.
        """
        resp = model.generate_content(summary_prompt)
        return resp.text

    except Exception as e:
        return f"System Error: {e}"
    finally:
        try:
            con.close()
        except:
            pass

# -------------------------------------------------------------------
# 4. Flask App
# -------------------------------------------------------------------
@app.route("/chat", methods=["GET", "POST"])
def chat():
    q = request.form.get("question", "") if request.method == "POST" else ""
    a = run_analytics_question(q) if q else None
    return render_template_string(CHAT_TEMPLATE, question=q, answer=a)

@app.route("/ask", methods=["POST"])
def api_ask():
    try:
        data = request.get_json(force=True)
        return jsonify({"answer": run_analytics_question(data.get("question", ""))})
    except Exception as e:
        return jsonify({"error": str(e)})

# Simple HTML Template
CHAT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<title>Retail Ops Agent</title>
<style>
  body { font-family: -apple-system, sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; background: #f4f4f9; }
  .chat-container { background: #fff; padding: 30px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); }
  .msg { margin-bottom: 20px; padding: 15px; border-radius: 8px; line-height: 1.5; }
  .user { background: #e0e7ff; color: #1e1b4b; border-left: 4px solid #4f46e5; }
  .bot { background: #f3f4f6; color: #1f2937; white-space: pre-wrap; border-left: 4px solid #10b981; }
  textarea { width: 100%; padding: 12px; border-radius: 8px; border: 1px solid #d1d5db; margin-top: 15px; box-sizing: border-box; font-family: inherit; }
  button { margin-top: 10px; padding: 10px 24px; background: #4f46e5; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 500; }
  button:hover { background: #4338ca; }
</style>
</head>
<body>
  <h2>Retail Operations Assistant</h2>
  <div class="chat-container">
    {% if question %}
      <div class="msg user"><strong>You:</strong><br>{{ question }}</div>
    {% endif %}
    {% if answer %}
      <div class="msg bot"><strong>Agent:</strong><br>{{ answer }}</div>
    {% endif %}
    <form method="post" action="/chat">
      <textarea name="question" rows="3" placeholder="e.g. Which stores have the highest stockout risk?">{{ question or '' }}</textarea>
      <button type="submit">Ask</button>
    </form>
  </div>
</body>
</html>
"""

if _name_ == "_main_":
    app.run(host="0.0.0.0", port=5001, debug=True)