-- postgres/ddl/create_schemas.sql
-- Run this ONCE when setting up fresh environment.
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS bronze_meta;
-- Audit table: har pipeline run ka log
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    run_date DATE NOT NULL,
    table_name VARCHAR(100),
    layer VARCHAR(20),
    -- bronze/silver/gold
    input_rows INTEGER,
    output_rows INTEGER,
    dropped_rows INTEGER,
    status VARCHAR(20),
    -- success/failed/warning
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);
-- Quality check results
CREATE TABLE IF NOT EXISTS audit.quality_results (
    check_id BIGSERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    table_name VARCHAR(100),
    check_name VARCHAR(200),
    passed BOOLEAN,
    value NUMERIC,
    threshold NUMERIC,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);