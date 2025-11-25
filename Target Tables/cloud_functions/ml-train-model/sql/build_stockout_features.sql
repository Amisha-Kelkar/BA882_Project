-- ============================================
-- Create Stockout Features Dataset
-- ============================================
-- Purpose: Generate ML dataset using daily store-product features
--          to predict whether the product will stock out in the next 7 days.
-- Grain:   One row per (store_id, product_id, ds)
-- Source:  project_882.gold.stockout_daily
-- Target:  project_882.ai_datasets.stockout_features
-- ============================================

CREATE OR REPLACE TABLE project_882.ai_datasets.stockout_features AS
WITH base AS (
  SELECT
    -- Identifiers
    store_id,
    product_id,
    ds,

    -- Target (keep as a separate name in the base CTE)
    stockout_next_7d AS target_stockout_next_7d,

    -- Core features directly from gold layer
    is_oos_today,
    is_sold_out,
    is_out_of_stock_in_all_stores,
    available_to_promise_qty,
    trend_7d_avg,
    google_trend_score,
    rating,
    number_of_reviews,
    current_retail_min,
    current_retail_max,
    reg_retail_min,
    reg_retail_max,
    brand,
    category,

    -- Inventory summary statistics
    inv_7d_avg,
    inv_7d_min,
    inv_7d_max,
    inv_7d_std,

    -- Change features (even if currently NULL)
    price_change_7d,
    rating_change_7d,
    reviews_change_7d

  FROM project_882.gold.stockout_daily
),

valid_samples AS (
  SELECT *
  FROM base
  WHERE target_stockout_next_7d IS NOT NULL
)

SELECT
  -- Identifiers
  store_id,
  product_id,
  ds AS event_date,

  -- Features
  is_oos_today,
  is_sold_out,
  is_out_of_stock_in_all_stores,
  available_to_promise_qty,
  trend_7d_avg,
  google_trend_score,
  rating,
  number_of_reviews,
  current_retail_min,
  current_retail_max,
  reg_retail_min,
  reg_retail_max,
  brand,
  category,
  inv_7d_avg,
  inv_7d_min,
  inv_7d_max,
  inv_7d_std,
  price_change_7d,
  rating_change_7d,
  reviews_change_7d,

  -- Target
  target_stockout_next_7d AS stockout_label,

  -- Train/Test Split (temporal, based on ds: first 70% = train)
  CASE 
    WHEN PERCENT_RANK() OVER (ORDER BY ds) < 0.70 THEN 'train'
    ELSE 'test'
  END AS split,

  -- Metadata
  NOW() AS created_at

FROM valid_samples
ORDER BY store_id, product_id, ds;
