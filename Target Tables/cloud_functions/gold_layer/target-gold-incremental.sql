CREATE OR REPLACE TABLE project_882.gold.stockout_daily AS
WITH src AS (
  SELECT
    CAST(s.store_id AS INT) AS store_id,
    CAST(s.product_id AS INT) AS product_id,
    CAST(s.record_date AS DATE) AS ds,
    s.title,
    s.brand,
    s.category,
    s.location_name,
    s.city,
    s.state,
    s.postal_code,
    TRY_CAST(s.price AS DOUBLE) AS price,
    TRY_CAST(s.price AS DOUBLE) AS listed_price,
    TRY_CAST(s.current_retail_min AS DOUBLE) AS current_retail_min,
    TRY_CAST(s.current_retail_max AS DOUBLE) AS current_retail_max,
    TRY_CAST(s.reg_retail_min AS DOUBLE) AS reg_retail_min,
    TRY_CAST(s.reg_retail_max AS DOUBLE) AS reg_retail_max,
    TRY_CAST(s.rating AS DOUBLE) AS rating,
    TRY_CAST(s.number_of_reviews AS DOUBLE) AS number_of_reviews,
    TRY_CAST(s.available_to_promise_qty AS DOUBLE) AS atp_qty,
    s.shipping_availability_status,
    CASE 
      WHEN LOWER(s.is_out_of_stock_in_all_stores) IN ('true', '1', 't', 'yes') THEN TRUE
      WHEN LOWER(s.is_out_of_stock_in_all_stores) IN ('false', '0', 'f', 'no', '') THEN FALSE
      ELSE NULL
    END AS is_oos_all_stores,
    CASE 
      WHEN LOWER(s.is_sold_out) IN ('true', '1', 't', 'yes') THEN TRUE
      WHEN LOWER(s.is_sold_out) IN ('false', '0', 'f', 'no', '') THEN FALSE
      ELSE NULL
    END AS is_sold_out,
    s.shipping_method,
    CASE 
      WHEN LOWER(s.is_two_day_shipping) IN ('true', '1', 't', 'yes') THEN TRUE
      WHEN LOWER(s.is_two_day_shipping) IN ('false', '0', 'f', 'no', '') THEN FALSE
      ELSE NULL
    END AS is_two_day_shipping,
    CASE 
      WHEN LOWER(s.is_base_shipping_method) IN ('true', '1', 't', 'yes') THEN TRUE
      WHEN LOWER(s.is_base_shipping_method) IN ('false', '0', 'f', 'no', '') THEN FALSE
      ELSE NULL
    END AS is_base_shipping_method,
    TRY_CAST(s.min_delivery_date AS DATE) AS min_delivery_date,
    TRY_CAST(s.max_delivery_date AS DATE) AS max_delivery_date,
    TRY_CAST(s.cutoff_time AS TIMESTAMP) AS cutoff_time,
    TRY_CAST(s.google_trend_score AS DOUBLE) AS google_trend_score,
    s.silver_load_ts
  FROM project_882.main.silver_table s
  WHERE s.store_id IS NOT NULL AND s.product_id IS NOT NULL AND s.record_date IS NOT NULL
),

-- Step 1: Deduplicate per day
daily AS (
  SELECT *, 
    ROW_NUMBER() OVER (
      PARTITION BY store_id, product_id, ds
      ORDER BY silver_load_ts DESC
    ) AS rn
  FROM src
  QUALIFY rn = 1
),

-- Step 2: Rolling aggregations
with_rolling AS (
  SELECT *,
    AVG(atp_qty) OVER (PARTITION BY store_id, product_id ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS inv_7d_avg,
    MIN(atp_qty) OVER (PARTITION BY store_id, product_id ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS inv_7d_min,
    MAX(atp_qty) OVER (PARTITION BY store_id, product_id ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS inv_7d_max,
    STDDEV_SAMP(atp_qty) OVER (PARTITION BY store_id, product_id ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS inv_7d_std,
    AVG(google_trend_score) OVER (PARTITION BY store_id, product_id ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS trend_7d_avg
  FROM daily
),

-- Step 3: 7-day changes
with_changes AS (
  SELECT *,
    listed_price - LAG(listed_price, 7) OVER (PARTITION BY store_id, product_id ORDER BY ds) AS price_change_7d,
    rating - LAG(rating, 7) OVER (PARTITION BY store_id, product_id ORDER BY ds) AS rating_change_7d,
    number_of_reviews - LAG(number_of_reviews, 7) OVER (PARTITION BY store_id, product_id ORDER BY ds) AS reviews_change_7d,
    (COALESCE(atp_qty, 0) <= 0 OR is_sold_out OR is_oos_all_stores) AS is_oos_today
  FROM with_rolling
),

-- Step 4: Future 7-day window for stockouts
future_window AS (
  SELECT
    curr.store_id,
    curr.product_id,
    curr.ds,
    MIN(fut.atp_qty) AS fut_min_qty,
    MAX(
      CASE WHEN fut.is_sold_out OR fut.is_oos_all_stores OR COALESCE(fut.atp_qty, 0) <= 0 THEN 1 ELSE 0 END
    ) AS fut_any_oos
  FROM with_changes curr
  LEFT JOIN with_changes fut
    ON fut.store_id = curr.store_id
    AND fut.product_id = curr.product_id
    AND fut.ds > curr.ds
    AND fut.ds <= curr.ds + INTERVAL '7 days'
  GROUP BY curr.store_id, curr.product_id, curr.ds
)

-- Step 5: Final gold table
SELECT
  c.store_id,
  c.product_id,
  c.ds,
  c.title,
  c.brand,
  c.category,
  c.location_name,
  c.city,
  c.state,
  c.postal_code,
  c.price,
  c.listed_price,
  c.current_retail_min,
  c.current_retail_max,
  c.reg_retail_min,
  c.reg_retail_max,
  c.rating,
  c.number_of_reviews,
  c.atp_qty AS available_to_promise_qty,
  c.shipping_availability_status,
  c.is_oos_all_stores AS is_out_of_stock_in_all_stores,
  c.is_sold_out,
  c.shipping_method,
  c.is_two_day_shipping,
  c.is_base_shipping_method,
  c.min_delivery_date,
  c.max_delivery_date,
  c.cutoff_time,
  c.google_trend_score,
  c.inv_7d_avg,
  c.inv_7d_min,
  c.inv_7d_max,
  c.inv_7d_std,
  c.price_change_7d,
  c.rating_change_7d,
  c.reviews_change_7d,
  c.trend_7d_avg,
  c.is_oos_today,
  CASE
    WHEN COALESCE(f.fut_any_oos, 0) = 1 THEN TRUE
    WHEN COALESCE(f.fut_min_qty, 999999) <= 0 THEN TRUE
    ELSE FALSE
  END AS stockout_next_7d,
  c.silver_load_ts,
  NOW() AS gold_load_ts
FROM with_changes c
LEFT JOIN future_window f
  ON f.store_id = c.store_id AND f.product_id = c.product_id AND f.ds = c.ds;
