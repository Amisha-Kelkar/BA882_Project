CREATE TABLE IF NOT EXISTS project_882.model_outputs.stockout_alert_log (
    store_id TEXT,
    product_id TEXT,
    event_date DATE,
    alert_sent_at TIMESTAMP
);


CREATE OR REPLACE VIEW project_882.model_outputs.stockout_alert_candidates AS
SELECT
    store_id,
    product_id,
    event_date,
    stockout_label,
    predicted_stockout_proba,
    predicted_stockout_flag
FROM project_882.model_outputs.stockout_predictions_v1
WHERE predicted_stockout_flag = 1
   AND predicted_stockout_proba >= 0.80;
