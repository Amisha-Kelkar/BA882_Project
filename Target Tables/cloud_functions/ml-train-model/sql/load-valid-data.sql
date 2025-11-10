-- Load validation data for the stockout model
SELECT
    store_id,
    product_id,
    ds,

    -- target label
    stockout_next_7d AS label,

    -- numeric + boolean features
    available_to_promise_qty,
    is_out_of_stock_in_all_stores,
    is_sold_out,
    is_two_day_shipping,
    is_base_shipping_method,

    price,
    listed_price,
    current_retail_min,
    current_retail_max,
    reg_retail_min,
    reg_retail_max,
    rating,
    number_of_reviews,

    google_trend_score,

    inv_7d_avg,
    inv_7d_min,
    inv_7d_max,
    inv_7d_std,
    price_change_7d,
    rating_change_7d,
    reviews_change_7d,
    trend_7d_avg
FROM project_882.ai_datasets.stockout_features_v1
WHERE split = 'valid'
  AND stockout_next_7d IS NOT NULL;
