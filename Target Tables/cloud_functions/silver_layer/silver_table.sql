CREATE OR REPLACE TABLE project_882.main.silver_table AS
WITH
-- Product Search Cleaned
ps AS (
     SELECT
        store_id::INT AS store_id,
        product_id::INT AS product_id,
        TRIM(title) AS title,
        brand,
        TRY_CAST(price AS DOUBLE) AS price,
        TRY_CAST(rating AS INT) AS rating,
        TRY_CAST(reviews AS INT) AS number_of_reviews,
        LOWER(CAST(availability AS VARCHAR)) AS availability,
        category,
        DATE(load_date) AS load_date
    FROM project_882.main.target_products_search
    WHERE product_id IS NOT NULL
),

-- Fulfillment Cleaned
f AS (
    SELECT
        store_id::INT AS store_id,
        product_tcin::INT AS product_tcin,
        TRY_CAST(record_date AS DATE) AS record_date,
        TRY_CAST(min_delivery_date AS DATE) AS min_delivery_date,
        TRY_CAST(max_delivery_date AS DATE) AS max_delivery_date,
        TRY_CAST(cutoff_time AS DATETIME) AS cutoff_time,
        LOWER(shipping_method_short_description) AS shipping_method,
        is_two_day_shipping,
        is_base_shipping_method,
        is_out_of_stock_in_all_stores,
        is_sold_out,
        shipping_availability_status,
        available_to_promise_qty,
        scheduled_delivery_status,
        scheduled_delivery_promise_qty,
        charge_scheduled_delivery,
        charge_one_day
    FROM project_882.main.target_products_fulfillment
    WHERE store_id IS NOT NULL AND product_tcin IS NOT NULL
),
pp AS (
    SELECT
        store_id::INT AS store_id,
        product_id::INT AS product_id,
        TRY_CAST(record_date AS DATE) AS record_date,
        TRY_CAST(price AS DOUBLE) AS listed_price,
        is_current_price_range,
        formatted_current_price_type,
        formatted_comparison_price_type,
        current_retail_min,
        current_retail_max,
        reg_retail_min,
        reg_retail_max,
        cart_add_on_threshold,
        formatted_current_price,
        formatted_comparison_price
    FROM project_882.main.target_product_price_details
    WHERE product_id IS NOT NULL
),
-- Store Data Cleaned
s AS (
    SELECT DISTINCT
        store_id::INT AS store_id,
        TRIM(location_name) AS location_name,
        city,
        state,
        postal_code
    FROM project_882.main.store_data
    WHERE store_id IS NOT NULL
),

-- Google Trends
g AS (
    SELECT
        DATE(dt) AS date,
        TRY_CAST(interest_score AS INT) AS interest_score
    FROM project_882.main.raw_google_trends_christmas_decor
    WHERE interest_score IS NOT NULL
),
--  Final Joined Silver Table
final_silver AS (
SELECT
    f.store_id,
    f.product_tcin AS product_id,
    ps.title,
    ps.brand,
    ps.category,
    ps.rating,
    ps.number_of_reviews,
    ps.availability,
    s.location_name,
    s.city,
    s.state,
    s.postal_code,
    f.record_date,
    f.min_delivery_date,
    f.max_delivery_date,
    f.cutoff_time,
    f.shipping_method,
    f.is_two_day_shipping,
    f.is_base_shipping_method,
    f.is_out_of_stock_in_all_stores,
    f.is_sold_out,
    f.shipping_availability_status,
    f.available_to_promise_qty,
    f.scheduled_delivery_status,
    f.scheduled_delivery_promise_qty,
    f.charge_scheduled_delivery,
    f.charge_one_day,
    ps.price,
    pp.is_current_price_range,
    pp.formatted_current_price_type,
    pp.formatted_comparison_price_type,
    pp.current_retail_min,
    pp.current_retail_max,
    pp.reg_retail_min,
    pp.reg_retail_max,
    pp.cart_add_on_threshold,
    pp.formatted_current_price,
    pp.formatted_comparison_price,
    g.interest_score AS google_trend_score,
    CURRENT_TIMESTAMP AS silver_load_ts
FROM f
LEFT JOIN ps
    ON f.store_id = ps.store_id AND f.product_tcin = ps.product_id
LEFT JOIN s
    ON f.store_id = s.store_id
LEFT JOIN g
    ON DATE(f.record_date) = g.date
LEFT JOIN pp
    ON f.store_id = pp.store_id 
   AND f.product_tcin = pp.product_id 
   AND DATE(f.record_date) = pp.record_date
)

SELECT * FROM final_silver;