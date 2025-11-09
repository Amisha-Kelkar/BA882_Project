
-- target/target-gold-schema-setup.sql
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.stockout_daily (
  store_id                INTEGER,
  product_id              INTEGER,
  ds                      DATE,         -- record_date

  -- product & store attrs (denorm for BI/ML)
  title                   VARCHAR,
  brand                   VARCHAR,
  category                VARCHAR,
  location_name           VARCHAR,
  city                    VARCHAR,
  state                   VARCHAR,
  postal_code             VARCHAR,

  -- price & review snapshot
  price                   DOUBLE,
  listed_price            DOUBLE,
  current_retail_min      DOUBLE,
  current_retail_max      DOUBLE,
  reg_retail_min          DOUBLE,
  reg_retail_max          DOUBLE,
  rating                  DOUBLE,
  number_of_reviews       DOUBLE,

  -- inventory & fulfillment snapshot
  available_to_promise_qty DOUBLE,
  shipping_availability_status VARCHAR,
  is_out_of_stock_in_all_stores BOOLEAN,
  is_sold_out              BOOLEAN,
  shipping_method          VARCHAR,
  is_two_day_shipping      BOOLEAN,
  is_base_shipping_method  BOOLEAN,
  min_delivery_date        DATE,
  max_delivery_date        DATE,
  cutoff_time              TIMESTAMP,

  -- trends
  google_trend_score      DOUBLE,

  -- derived rolling features (7d windows)
  inv_7d_avg              DOUBLE,
  inv_7d_min              DOUBLE,
  inv_7d_max              DOUBLE,
  inv_7d_std              DOUBLE,
  price_change_7d         DOUBLE,
  rating_change_7d        DOUBLE,
  reviews_change_7d       DOUBLE,
  trend_7d_avg            DOUBLE,

  -- current day flags
  is_oos_today            BOOLEAN,

  -- >>> derived prediction target (future-looking)
  stockout_next_7d        BOOLEAN,

  -- metadata
  silver_load_ts          TIMESTAMP,
  gold_load_ts            TIMESTAMP DEFAULT NOW(),

  PRIMARY KEY (store_id, product_id, ds)
);