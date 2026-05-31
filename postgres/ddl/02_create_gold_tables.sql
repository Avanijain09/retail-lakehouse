-- ============================================================
-- Gold Layer — DDL for all four KPI mart tables
-- Column types are mapped exactly from PySpark Gold output:
--   CHAR(7)        ← purchase_month (yyyy-MM string)
--   DATE           ← purchase_date (DateType in PySpark)
--   NUMERIC(15,2)  ← monetary values (price, freight, gmv)
--   NUMERIC(8,2)   ← time durations (delivery days, hours)
--   NUMERIC(5,2)   ← percentages (0-100 range)
--   INTEGER        ← counts
--   CHAR(2)        ← Brazilian state codes (SP, RJ, MG ...)
-- ============================================================
-- ─────────────────────────────────────────────────────────────
-- TABLE 1: daily_orders_kpi
-- Source: silver/orders + silver/customers
-- Grain : 1 row = 1 purchase_date × 1 customer_state
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.daily_orders_kpi (
    id BIGSERIAL PRIMARY KEY,
    purchase_date DATE NOT NULL,
    customer_state CHAR(2) NOT NULL,
    -- Volume
    total_orders INTEGER NOT NULL DEFAULT 0,
    total_unique_customers INTEGER NOT NULL DEFAULT 0,
    -- Status breakdown
    delivered_count INTEGER NOT NULL DEFAULT 0,
    cancelled_count INTEGER NOT NULL DEFAULT 0,
    shipped_count INTEGER NOT NULL DEFAULT 0,
    processing_count INTEGER NOT NULL DEFAULT 0,
    invoiced_count INTEGER NOT NULL DEFAULT 0,
    approved_count INTEGER NOT NULL DEFAULT 0,
    -- Time metrics (null = not applicable for this status/state combination)
    avg_approval_time_hrs NUMERIC(8, 2),
    avg_delivery_days NUMERIC(8, 2),
    avg_estimated_days NUMERIC(8, 2),
    -- Quality metrics
    on_time_count INTEGER NOT NULL DEFAULT 0,
    delivery_rate_pct NUMERIC(5, 2),
    cancellation_rate_pct NUMERIC(5, 2),
    on_time_rate_pct NUMERIC(5, 2),
    -- Pipeline audit
    _process_date DATE NOT NULL,
    _load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.daily_orders_kpi IS 'Daily order volume and delivery quality by customer state. Grain: purchase_date × customer_state.';
COMMENT ON COLUMN gold.daily_orders_kpi.purchase_date IS 'Date extracted from order_purchase_timestamp in Silver.';
COMMENT ON COLUMN gold.daily_orders_kpi._process_date IS 'Pipeline run date that produced this row — used for idempotent loads.';
-- ─────────────────────────────────────────────────────────────
-- TABLE 2: category_revenue_kpi
-- Source: silver/order_items + silver/orders + silver/products
-- Grain : 1 row = 1 purchase_month × 1 product_category_name
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.category_revenue_kpi (
    id BIGSERIAL PRIMARY KEY,
    purchase_month CHAR(7) NOT NULL,
    -- yyyy-MM
    product_category_name VARCHAR(100) NOT NULL,
    -- lowercase Olist category
    -- Revenue split
    total_revenue NUMERIC(15, 2) NOT NULL DEFAULT 0,
    -- SUM(price)
    total_freight NUMERIC(15, 2) NOT NULL DEFAULT 0,
    -- SUM(freight_value)
    total_gmv NUMERIC(15, 2) NOT NULL DEFAULT 0,
    -- SUM(total_item_value)
    -- Volume
    total_items_sold INTEGER NOT NULL DEFAULT 0,
    -- each row = 1 unit (no qty col in Olist)
    total_orders INTEGER NOT NULL DEFAULT 0,
    -- Derived
    avg_item_price NUMERIC(10, 2),
    freight_pct_of_gmv NUMERIC(5, 2),
    -- Pipeline audit
    _process_date DATE NOT NULL,
    _load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.category_revenue_kpi IS 'Monthly revenue and freight by product category. CANCELED and UNAVAILABLE orders excluded.';
COMMENT ON COLUMN gold.category_revenue_kpi.total_revenue IS 'SUM of price (product revenue only, excludes freight).';
COMMENT ON COLUMN gold.category_revenue_kpi.total_gmv IS 'Gross merchandise value = price + freight_value per item.';
-- ─────────────────────────────────────────────────────────────
-- TABLE 3: seller_performance_kpi
-- Source: silver/order_items + silver/orders + silver/stores
-- Grain : 1 row = 1 purchase_month × 1 seller_id
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.seller_performance_kpi (
    id BIGSERIAL PRIMARY KEY,
    purchase_month CHAR(7) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    -- Olist md5 hash
    -- Seller geography (from silver/stores — may be null if seller removed)
    seller_city VARCHAR(100),
    seller_state CHAR(2),
    -- Revenue
    total_revenue NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_freight NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_gmv NUMERIC(15, 2) NOT NULL DEFAULT 0,
    -- Volume
    total_items_sold INTEGER NOT NULL DEFAULT 0,
    total_orders INTEGER NOT NULL DEFAULT 0,
    -- Derived
    avg_item_price NUMERIC(10, 2),
    avg_freight_per_item NUMERIC(10, 2),
    freight_pct_of_gmv NUMERIC(5, 2),
    -- Pipeline audit
    _process_date DATE NOT NULL,
    _load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.seller_performance_kpi IS 'Monthly seller revenue and volume. Maps to stores dimension. CANCELED/UNAVAILABLE orders excluded.';
-- ─────────────────────────────────────────────────────────────
-- TABLE 4: delivery_performance_kpi
-- Source: silver/orders + silver/customers
-- Grain : 1 row = 1 purchase_month × 1 customer_state
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.delivery_performance_kpi (
    id BIGSERIAL PRIMARY KEY,
    purchase_month CHAR(7) NOT NULL,
    customer_state CHAR(2) NOT NULL,
    -- Volume breakdown
    total_orders INTEGER NOT NULL DEFAULT 0,
    delivered_orders INTEGER NOT NULL DEFAULT 0,
    cancelled_orders INTEGER NOT NULL DEFAULT 0,
    on_time_orders INTEGER NOT NULL DEFAULT 0,
    -- Duration metrics (null = no delivered orders for this group)
    avg_delivery_days NUMERIC(8, 2),
    avg_estimated_days NUMERIC(8, 2),
    avg_delay_days NUMERIC(8, 2),
    -- avg days late (late orders only)
    -- Rate metrics
    delivery_rate_pct NUMERIC(5, 2),
    cancellation_rate_pct NUMERIC(5, 2),
    on_time_rate_pct NUMERIC(5, 2),
    -- Pipeline audit
    _process_date DATE NOT NULL,
    _load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE gold.delivery_performance_kpi IS 'Monthly delivery quality by customer state. All order statuses included (cancellation and delivery rates are KPIs).';
COMMENT ON COLUMN gold.delivery_performance_kpi.avg_delay_days IS 'Average days late for LATE deliveries only (null = all deliveries on time or no delivered orders).';