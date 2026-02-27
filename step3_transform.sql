-- ============================================================
-- E-Commerce Data Pipeline — Star Schema + Analytics Views
-- ============================================================
-- Replace YOUR_PROJECT_ID with your actual GCP project ID
-- Run each block IN ORDER in the BigQuery console
-- ============================================================


-- ============================================================
-- 0. Create the data warehouse dataset (run once)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT_ID.ecommerce_dwh`
  OPTIONS (location = 'EU');


-- ============================================================
-- 1. dim_customers
-- ============================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.ecommerce_dwh.dim_customers` AS
SELECT
  customer_id,
  name                          AS customer_name,
  email,
  city,
  CAST(loyalty_member AS BOOL)  AS is_loyalty_member,
  CURRENT_TIMESTAMP()           AS loaded_at
FROM `YOUR_PROJECT_ID.ecommerce_raw.customers`
WHERE customer_id IS NOT NULL;


-- ============================================================
-- 2. dim_products
-- ============================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.ecommerce_dwh.dim_products` AS
SELECT
  product_id,
  name                          AS product_name,
  category,
  CAST(price AS FLOAT64)        AS unit_price,
  channel,
  CURRENT_TIMESTAMP()           AS loaded_at
FROM `YOUR_PROJECT_ID.ecommerce_raw.products`;


-- ============================================================
-- 3. dim_date
-- ============================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.ecommerce_dwh.dim_date` AS
WITH all_dates AS (
  SELECT DATE(order_date)       AS date_value FROM `YOUR_PROJECT_ID.ecommerce_raw.shopify_orders`
  UNION DISTINCT
  SELECT DATE(transaction_date) AS date_value FROM `YOUR_PROJECT_ID.ecommerce_raw.pos_transactions`
)
SELECT
  date_value                                              AS sale_date,
  EXTRACT(YEAR        FROM date_value)                    AS year,
  EXTRACT(MONTH       FROM date_value)                    AS month,
  FORMAT_DATE('%B',        date_value)                    AS month_name,
  EXTRACT(WEEK        FROM date_value)                    AS week_number,
  EXTRACT(DAYOFWEEK   FROM date_value)                    AS day_of_week,
  FORMAT_DATE('%A',        date_value)                    AS day_name,
  CASE WHEN EXTRACT(DAYOFWEEK FROM date_value) IN (1,7)
       THEN TRUE ELSE FALSE END                           AS is_weekend
FROM all_dates
ORDER BY sale_date;


-- ============================================================
-- 4. dim_channels
-- ============================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.ecommerce_dwh.dim_channels` AS
SELECT 'shopify' AS channel_id, 'Online (Shopify)' AS channel_name, 'online'  AS channel_type UNION ALL
SELECT 'pos'     AS channel_id, 'In-Store (POS)'   AS channel_name, 'instore' AS channel_type;


-- ============================================================
-- 5. fact_sales  ← CORE TABLE
-- Unified sales across both channels, partitioned by date
-- ============================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.ecommerce_dwh.fact_sales`
PARTITION BY sale_date
AS

-- Online orders (Shopify)
SELECT
  o.order_id                                          AS transaction_id,
  'shopify'                                           AS channel_id,
  o.customer_id,
  i.product_id,
  DATE(o.order_date)                                  AS sale_date,
  EXTRACT(HOUR FROM TIMESTAMP(o.order_date))          AS sale_hour,
  i.quantity,
  i.unit_price,
  i.line_total                                        AS revenue,
  o.status                                            AS transaction_status,
  o.shipping_city                                     AS location
FROM `YOUR_PROJECT_ID.ecommerce_raw.shopify_orders`   o
JOIN `YOUR_PROJECT_ID.ecommerce_raw.order_items`      i ON o.order_id = i.order_id
WHERE o.source = 'shopify'

UNION ALL

-- In-store transactions (POS)
SELECT
  t.transaction_id,
  'pos'                                               AS channel_id,
  t.customer_id,
  i.product_id,
  DATE(t.transaction_date)                            AS sale_date,
  EXTRACT(HOUR FROM TIMESTAMP(t.transaction_date))    AS sale_hour,
  i.quantity,
  i.unit_price,
  i.line_total                                        AS revenue,
  'completed'                                         AS transaction_status,
  t.store_location                                    AS location
FROM `YOUR_PROJECT_ID.ecommerce_raw.pos_transactions` t
JOIN `YOUR_PROJECT_ID.ecommerce_raw.order_items`      i ON t.transaction_id = i.order_id
WHERE t.source = 'pos';


-- ============================================================
-- 6. Analytics views  (connect these to Looker Studio)
-- ============================================================

-- Revenue by channel and month
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.ecommerce_dwh.v_revenue_by_channel` AS
SELECT
  c.channel_name,
  d.year,
  d.month,
  d.month_name,
  COUNT(DISTINCT f.transaction_id)  AS num_transactions,
  SUM(f.revenue)                    AS total_revenue,
  ROUND(AVG(f.revenue), 2)          AS avg_basket_size
FROM `YOUR_PROJECT_ID.ecommerce_dwh.fact_sales`   f
JOIN `YOUR_PROJECT_ID.ecommerce_dwh.dim_channels` c ON f.channel_id = c.channel_id
JOIN `YOUR_PROJECT_ID.ecommerce_dwh.dim_date`     d ON f.sale_date  = d.sale_date
WHERE f.transaction_status = 'completed'
GROUP BY c.channel_name, d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- Top products by revenue
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.ecommerce_dwh.v_top_products` AS
SELECT
  p.product_name,
  p.category,
  SUM(f.quantity)           AS units_sold,
  SUM(f.revenue)            AS total_revenue,
  ROUND(AVG(f.unit_price), 2) AS avg_price
FROM `YOUR_PROJECT_ID.ecommerce_dwh.fact_sales`    f
JOIN `YOUR_PROJECT_ID.ecommerce_dwh.dim_products`  p ON f.product_id = p.product_id
WHERE f.transaction_status = 'completed'
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC;


-- Sales by day of week (peak day analysis)
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.ecommerce_dwh.v_sales_by_weekday` AS
SELECT
  d.day_name,
  d.day_of_week,
  d.is_weekend,
  COUNT(DISTINCT f.transaction_id)  AS num_transactions,
  SUM(f.revenue)                    AS total_revenue
FROM `YOUR_PROJECT_ID.ecommerce_dwh.fact_sales` f
JOIN `YOUR_PROJECT_ID.ecommerce_dwh.dim_date`   d ON f.sale_date = d.sale_date
WHERE f.transaction_status = 'completed'
GROUP BY d.day_name, d.day_of_week, d.is_weekend
ORDER BY d.day_of_week;


-- Customer loyalty vs anonymous spend
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.ecommerce_dwh.v_customer_segments` AS
SELECT
  CASE WHEN f.customer_id = 'ANONYMOUS' THEN 'Anonymous'
       WHEN c.is_loyalty_member THEN 'Loyalty Member'
       ELSE 'Registered'
  END                               AS customer_segment,
  COUNT(DISTINCT f.transaction_id)  AS num_transactions,
  ROUND(SUM(f.revenue), 2)          AS total_revenue,
  ROUND(AVG(f.revenue), 2)          AS avg_spend_per_transaction
FROM `YOUR_PROJECT_ID.ecommerce_dwh.fact_sales`    f
LEFT JOIN `YOUR_PROJECT_ID.ecommerce_dwh.dim_customers` c ON f.customer_id = c.customer_id
WHERE f.transaction_status = 'completed'
GROUP BY customer_segment;


-- ============================================================
-- Quick checks — run these to verify everything loaded
-- ============================================================
-- SELECT COUNT(*) FROM `YOUR_PROJECT_ID.ecommerce_dwh.fact_sales`;
-- SELECT channel_id, COUNT(*) as rows, ROUND(SUM(revenue),2) as revenue FROM `YOUR_PROJECT_ID.ecommerce_dwh.fact_sales` GROUP BY channel_id;
-- SELECT MIN(sale_date) AS first_sale, MAX(sale_date) AS last_sale FROM `YOUR_PROJECT_ID.ecommerce_dwh.fact_sales`;
