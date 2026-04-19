-- ============================================================================
-- PostgreSQL Schema Definitions for Gold KPI Tables
-- Database: etl_analytics
-- Purpose: Store aggregated KPIs from Medallion Gold layer
-- ============================================================================

-- Drop tables if they exist (for clean re-initialization)
DROP TABLE IF EXISTS kpi_daily_revenue CASCADE;
DROP TABLE IF EXISTS kpi_user_retention CASCADE;
DROP TABLE IF EXISTS kpi_inventory_turnover CASCADE;
DROP TABLE IF EXISTS kpi_iot_anomaly_summary CASCADE;

-- ============================================================================
-- 1. Daily Revenue KPI
-- ============================================================================
CREATE TABLE kpi_daily_revenue (
    revenue_date DATE NOT NULL,
    total_revenue NUMERIC(12, 2) NOT NULL,
    total_transactions BIGINT NOT NULL,
    avg_transaction_value NUMERIC(10, 2),
    unique_customers BIGINT,
    total_fees NUMERIC(10, 2),
    total_events BIGINT,
    unique_users BIGINT,
    conversion_rate_pct NUMERIC(5, 2),
    aggregated_at TIMESTAMP NOT NULL,
    dt DATE NOT NULL,
    PRIMARY KEY (revenue_date, dt)
);

CREATE INDEX idx_daily_revenue_dt ON kpi_daily_revenue(dt);
CREATE INDEX idx_daily_revenue_date ON kpi_daily_revenue(revenue_date);

COMMENT ON TABLE kpi_daily_revenue IS 'Daily revenue metrics with conversion rate from transactions and events';
COMMENT ON COLUMN kpi_daily_revenue.revenue_date IS 'Date of revenue calculation';
COMMENT ON COLUMN kpi_daily_revenue.conversion_rate_pct IS 'Percentage of events that resulted in transactions';

-- ============================================================================
-- 2. User Retention KPI (Cohort Analysis)
-- ============================================================================
CREATE TABLE kpi_user_retention (
    cohort_date DATE NOT NULL,
    days_since_cohort INT NOT NULL,
    cohort_size BIGINT NOT NULL,
    active_users BIGINT NOT NULL,
    retention_rate_pct NUMERIC(5, 2) NOT NULL,
    total_events BIGINT,
    aggregated_at TIMESTAMP NOT NULL,
    dt DATE NOT NULL,
    PRIMARY KEY (cohort_date, days_since_cohort, dt)
);

CREATE INDEX idx_user_retention_dt ON kpi_user_retention(dt);
CREATE INDEX idx_user_retention_cohort ON kpi_user_retention(cohort_date);
CREATE INDEX idx_user_retention_days ON kpi_user_retention(days_since_cohort);

COMMENT ON TABLE kpi_user_retention IS 'User retention cohort analysis tracking user activity over time';
COMMENT ON COLUMN kpi_user_retention.cohort_date IS 'Date when users first appeared (cohort start date)';
COMMENT ON COLUMN kpi_user_retention.days_since_cohort IS 'Days elapsed since cohort start';
COMMENT ON COLUMN kpi_user_retention.retention_rate_pct IS 'Percentage of cohort still active';

-- ============================================================================
-- 3. Inventory Turnover KPI
-- ============================================================================
CREATE TABLE kpi_inventory_turnover (
    category VARCHAR(100) NOT NULL,
    product_count BIGINT NOT NULL,
    total_stock_units BIGINT,
    total_units_sold BIGINT,
    total_revenue NUMERIC(12, 2),
    avg_turnover_ratio NUMERIC(8, 4),
    total_stock_value NUMERIC(12, 2),
    products_need_reorder BIGINT,
    products_out_of_stock BIGINT,
    reorder_rate_pct NUMERIC(5, 2),
    stock_out_rate_pct NUMERIC(5, 2),
    aggregated_at TIMESTAMP NOT NULL,
    dt DATE NOT NULL,
    PRIMARY KEY (category, dt)
);

CREATE INDEX idx_inventory_turnover_dt ON kpi_inventory_turnover(dt);
CREATE INDEX idx_inventory_turnover_category ON kpi_inventory_turnover(category);

COMMENT ON TABLE kpi_inventory_turnover IS 'Inventory turnover metrics by product category';
COMMENT ON COLUMN kpi_inventory_turnover.avg_turnover_ratio IS 'Average inventory turnover ratio (sales/stock)';
COMMENT ON COLUMN kpi_inventory_turnover.reorder_rate_pct IS 'Percentage of products needing reorder';

-- ============================================================================
-- 4. IoT Anomaly Summary KPI
-- ============================================================================
CREATE TABLE kpi_iot_anomaly_summary (
    sensor_type VARCHAR(50) NOT NULL,
    anomaly_date DATE NOT NULL,
    total_readings BIGINT NOT NULL,
    anomaly_count BIGINT NOT NULL,
    avg_sensor_value NUMERIC(10, 4),
    min_sensor_value NUMERIC(10, 4),
    max_sensor_value NUMERIC(10, 4),
    avg_z_score NUMERIC(8, 4),
    max_abs_z_score NUMERIC(8, 4),
    anomaly_rate_pct NUMERIC(5, 2),
    aggregated_at TIMESTAMP NOT NULL,
    dt DATE NOT NULL,
    PRIMARY KEY (sensor_type, anomaly_date, dt)
);

CREATE INDEX idx_iot_anomaly_dt ON kpi_iot_anomaly_summary(dt);
CREATE INDEX idx_iot_anomaly_date ON kpi_iot_anomaly_summary(anomaly_date);
CREATE INDEX idx_iot_anomaly_type ON kpi_iot_anomaly_summary(sensor_type);

COMMENT ON TABLE kpi_iot_anomaly_summary IS 'IoT sensor anomaly detection using 3-sigma rule';
COMMENT ON COLUMN kpi_iot_anomaly_summary.avg_z_score IS 'Average z-score across all readings';
COMMENT ON COLUMN kpi_iot_anomaly_summary.anomaly_rate_pct IS 'Percentage of readings flagged as anomalies';

-- ============================================================================
-- Grant permissions (if needed for specific users)
-- ============================================================================
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl_user;
