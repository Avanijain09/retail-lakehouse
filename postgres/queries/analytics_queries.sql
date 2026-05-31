-- ============================================================
-- Analytics Queries — Gold Schema
-- These are the queries Tableau will generate implicitly.
-- Also useful for manual analysis and debugging.
-- ============================================================
-- ─── QUERY 1: Monthly revenue trend (all categories combined) ─
-- Business question: Is overall revenue growing month-over-month?
SELECT purchase_month,
    SUM(total_revenue) AS total_revenue,
    SUM(total_freight) AS total_freight,
    SUM(total_gmv) AS total_gmv,
    SUM(total_items_sold) AS total_items_sold,
    SUM(total_orders) AS total_orders,
    ROUND(
        SUM(total_freight)::numeric / NULLIF(SUM(total_gmv), 0) * 100,
        2
    ) AS freight_pct_of_gmv
FROM gold.category_revenue_kpi
GROUP BY purchase_month
ORDER BY purchase_month;
-- ─── QUERY 2: Top 10 categories by total GMV ──────────────────
-- Business question: Which categories drive the most revenue?
SELECT product_category_name,
    SUM(total_gmv) AS total_gmv,
    SUM(total_revenue) AS total_revenue,
    SUM(total_freight) AS total_freight,
    SUM(total_items_sold) AS total_items_sold,
    SUM(total_orders) AS total_orders,
    ROUND(
        SUM(total_freight)::numeric / NULLIF(SUM(total_gmv), 0) * 100,
        2
    ) AS freight_pct_of_gmv
FROM gold.category_revenue_kpi
GROUP BY product_category_name
ORDER BY total_gmv DESC
LIMIT 10;
-- ─── QUERY 3: Category performance by month (pivot-ready) ─────
-- Business question: How has each category trended over time?
SELECT purchase_month,
    product_category_name,
    total_revenue,
    total_items_sold,
    total_orders,
    avg_item_price,
    freight_pct_of_gmv
FROM gold.category_revenue_kpi
WHERE product_category_name NOT IN ('unknown') -- exclude uncategorised
ORDER BY purchase_month,
    total_revenue DESC;
-- ─── QUERY 4: Top 20 sellers by total GMV ─────────────────────
-- Business question: Who are our top-performing sellers?
SELECT seller_id,
    MAX(seller_city) AS seller_city,
    MAX(seller_state) AS seller_state,
    SUM(total_gmv) AS total_gmv,
    SUM(total_revenue) AS total_revenue,
    SUM(total_items_sold) AS total_items_sold,
    SUM(total_orders) AS total_orders,
    ROUND(
        SUM(total_revenue)::numeric / NULLIF(SUM(total_items_sold), 0),
        2
    ) AS avg_item_price_all_time
FROM gold.seller_performance_kpi
GROUP BY seller_id
ORDER BY total_gmv DESC
LIMIT 20;
-- ─── QUERY 5: Seller performance by state (regional ranking) ──
-- Business question: Which states have the strongest sellers?
SELECT seller_state,
    COUNT(DISTINCT seller_id) AS num_sellers,
    SUM(total_gmv) AS total_gmv,
    SUM(total_orders) AS total_orders,
    ROUND(
        SUM(total_gmv)::numeric / NULLIF(COUNT(DISTINCT seller_id), 0),
        2
    ) AS avg_gmv_per_seller
FROM gold.seller_performance_kpi
WHERE seller_state IS NOT NULL
GROUP BY seller_state
ORDER BY total_gmv DESC;
-- ─── QUERY 6: State-level delivery quality heatmap ────────────
-- Business question: Which states have the worst delivery performance?
SELECT customer_state,
    SUM(total_orders) AS total_orders,
    SUM(delivered_orders) AS delivered_orders,
    SUM(cancelled_orders) AS cancelled_orders,
    SUM(on_time_orders) AS on_time_orders,
    ROUND(AVG(avg_delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(avg_estimated_days), 2) AS avg_estimated_days,
    ROUND(
        SUM(on_time_orders)::numeric / NULLIF(SUM(delivered_orders), 0) * 100,
        2
    ) AS on_time_rate_pct,
    ROUND(
        SUM(delivered_orders)::numeric / NULLIF(SUM(total_orders), 0) * 100,
        2
    ) AS delivery_rate_pct,
    ROUND(
        SUM(cancelled_orders)::numeric / NULLIF(SUM(total_orders), 0) * 100,
        2
    ) AS cancellation_rate_pct
FROM gold.delivery_performance_kpi
GROUP BY customer_state
ORDER BY on_time_rate_pct DESC NULLS LAST;
-- ─── QUERY 7: Monthly delivery trend (all states combined) ────
-- Business question: Is delivery performance improving over time?
SELECT purchase_month,
    SUM(total_orders) AS total_orders,
    SUM(delivered_orders) AS delivered_orders,
    SUM(on_time_orders) AS on_time_orders,
    ROUND(AVG(avg_delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(avg_estimated_days), 2) AS avg_estimated_days,
    ROUND(AVG(avg_delay_days), 2) AS avg_delay_days,
    ROUND(
        SUM(on_time_orders)::numeric / NULLIF(SUM(delivered_orders), 0) * 100,
        2
    ) AS on_time_rate_pct
FROM gold.delivery_performance_kpi
GROUP BY purchase_month
ORDER BY purchase_month;
-- ─── QUERY 8: Daily order volume trend (last 30 days) ─────────
-- Business question: Any unusual drops or spikes in order volume?
-- Tableau uses this for the main KPI trend line.
SELECT purchase_date,
    SUM(total_orders) AS total_orders,
    SUM(total_unique_customers) AS total_customers,
    SUM(delivered_count) AS delivered,
    SUM(cancelled_count) AS cancelled,
    ROUND(
        SUM(cancelled_count)::numeric / NULLIF(SUM(total_orders), 0) * 100,
        2
    ) AS cancellation_rate_pct,
    ROUND(
        SUM(on_time_count)::numeric / NULLIF(SUM(delivered_count), 0) * 100,
        2
    ) AS on_time_rate_pct
FROM gold.daily_orders_kpi
WHERE purchase_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY purchase_date
ORDER BY purchase_date;
-- ─── QUERY 9: Pipeline monitoring — audit table ────────────────
-- Business question: Did yesterday's pipeline run succeed?
SELECT run_date,
    table_name,
    layer,
    input_rows,
    output_rows,
    dropped_rows,
    ROUND(
        output_rows::numeric / NULLIF(input_rows, 0) * 100,
        1
    ) AS retention_pct,
    status,
    completed_at,
    error_message
FROM audit.pipeline_runs
WHERE run_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY run_date DESC,
    layer,
    table_name;
-- ─── QUERY 10: Quick sanity check — all tables have today's data
-- Run this after load_gold_to_postgres.py to confirm all 4 tables loaded.
SELECT 'daily_orders_kpi' AS table_name,
    COUNT(*) AS row_count,
    MAX(_load_timestamp) AS last_loaded
FROM gold.daily_orders_kpi
WHERE _process_date = CURRENT_DATE
UNION ALL
SELECT 'category_revenue_kpi',
    COUNT(*),
    MAX(_load_timestamp)
FROM gold.category_revenue_kpi
WHERE _process_date = CURRENT_DATE
UNION ALL
SELECT 'seller_performance_kpi',
    COUNT(*),
    MAX(_load_timestamp)
FROM gold.seller_performance_kpi
WHERE _process_date = CURRENT_DATE
UNION ALL
SELECT 'delivery_performance_kpi',
    COUNT(*),
    MAX(_load_timestamp)
FROM gold.delivery_performance_kpi
WHERE _process_date = CURRENT_DATE
ORDER BY table_name;