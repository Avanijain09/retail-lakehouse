-- ============================================================
-- Gold Layer — Performance Indexes
-- Run after 03_create_gold_tables.sql.
-- Indexes cover the most common Tableau filter patterns.
-- Each index creation is safe to rerun (IF NOT EXISTS).
-- ============================================================
-- ─── daily_orders_kpi ────────────────────────────────────────
-- Primary access pattern: date range queries
CREATE INDEX IF NOT EXISTS idx_doki_purchase_date ON gold.daily_orders_kpi (purchase_date);
-- State filter (Tableau geographic filter)
CREATE INDEX IF NOT EXISTS idx_doki_customer_state ON gold.daily_orders_kpi (customer_state);
-- Most common combined filter: "show trend for this state"
CREATE INDEX IF NOT EXISTS idx_doki_date_state ON gold.daily_orders_kpi (purchase_date, customer_state);
-- Idempotent load operations (DELETE WHERE _process_date = ...)
CREATE INDEX IF NOT EXISTS idx_doki_process_date ON gold.daily_orders_kpi (_process_date);
-- ─── category_revenue_kpi ────────────────────────────────────
-- Time-series queries: "revenue trend by month"
CREATE INDEX IF NOT EXISTS idx_caki_purchase_month ON gold.category_revenue_kpi (purchase_month);
-- Category filter: "show all months for Electronics"
CREATE INDEX IF NOT EXISTS idx_caki_category ON gold.category_revenue_kpi (product_category_name);
-- Combined: "Electronics revenue trend over time"
CREATE INDEX IF NOT EXISTS idx_caki_month_category ON gold.category_revenue_kpi (purchase_month, product_category_name);
-- Idempotent load
CREATE INDEX IF NOT EXISTS idx_caki_process_date ON gold.category_revenue_kpi (_process_date);
-- ─── seller_performance_kpi ──────────────────────────────────
-- Month filter
CREATE INDEX IF NOT EXISTS idx_spki_purchase_month ON gold.seller_performance_kpi (purchase_month);
-- Seller lookup (exact seller profile queries)
CREATE INDEX IF NOT EXISTS idx_spki_seller_id ON gold.seller_performance_kpi (seller_id);
-- Regional analysis: "top sellers in SP"
CREATE INDEX IF NOT EXISTS idx_spki_seller_state ON gold.seller_performance_kpi (seller_state);
-- Combined: seller time-series
CREATE INDEX IF NOT EXISTS idx_spki_month_seller ON gold.seller_performance_kpi (purchase_month, seller_id);
-- Idempotent load
CREATE INDEX IF NOT EXISTS idx_spki_process_date ON gold.seller_performance_kpi (_process_date);
-- ─── delivery_performance_kpi ────────────────────────────────
-- Month filter
CREATE INDEX IF NOT EXISTS idx_dpki_purchase_month ON gold.delivery_performance_kpi (purchase_month);
-- State filter (geographic delivery heatmap in Tableau)
CREATE INDEX IF NOT EXISTS idx_dpki_customer_state ON gold.delivery_performance_kpi (customer_state);
-- Combined: state delivery trend over time
CREATE INDEX IF NOT EXISTS idx_dpki_month_state ON gold.delivery_performance_kpi (purchase_month, customer_state);
-- Idempotent load
CREATE INDEX IF NOT EXISTS idx_dpki_process_date ON gold.delivery_performance_kpi (_process_date);
-- ─── Verify all indexes created ──────────────────────────────
SELECT schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'gold'
ORDER BY tablename,
    indexname;