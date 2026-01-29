-- PostgreSQL Verification Queries
-- Run these queries to verify your data warehouse is working correctly

-- 1. CHECK ALL TABLES
SELECT tablename, 
       pg_size_pretty(pg_total_relation_size('retail_dwh.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'retail_dwh'
ORDER BY tablename;

-- Expected: 34 tables
-- Should see: dim_date, dim_product, dim_customer, fct_retail_sales, 
--             26 partitions (fct_retail_sales_2009_12 through fct_retail_sales_2011_12),
--             stg_retail_sales, dq_metrics, dq_quarantine_sales, meta_etl_batch_log


-- 2. CHECK MATERIALIZED VIEWS
SELECT matviewname,
       pg_size_pretty(pg_total_relation_size('retail_dwh.'||matviewname)) as size
FROM pg_matviews 
WHERE schemaname = 'retail_dwh'
ORDER BY matviewname;

-- Expected: 6 materialized views
-- mv_monthly_sales_summary
-- mv_top_products
-- mv_customer_segments
-- mv_daily_sales_trend
-- mv_country_performance
-- mv_product_category_analysis


-- 3. CHECK ROW COUNTS
SELECT 
    'dim_date' as table_name, COUNT(*) as row_count 
FROM retail_dwh.dim_date
UNION ALL
SELECT 'dim_product', COUNT(*) FROM retail_dwh.dim_product
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM retail_dwh.dim_customer
UNION ALL
SELECT 'fct_retail_sales', COUNT(*) FROM retail_dwh.fct_retail_sales
UNION ALL
SELECT 'stg_retail_sales', COUNT(*) FROM retail_dwh.stg_retail_sales
ORDER BY table_name;

-- Expected:
-- dim_date: 373 rows
-- dim_product: ~3,811 rows
-- dim_customer: ~4,339 rows
-- fct_retail_sales: ~534,756 rows
-- stg_retail_sales: ~536,629 rows


-- 4. CHECK PARTITIONS
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'retail_dwh' 
  AND tablename LIKE 'fct_retail_sales_%'
ORDER BY tablename;

-- Expected: 26 partitions (2009-12 through 2011-12)


-- 5. VERIFY FACT TABLE DATA DISTRIBUTION
SELECT 
    DATE_TRUNC('month', invoice_timestamp) as month,
    COUNT(*) as transactions,
    SUM(line_total) as total_revenue,
    AVG(line_total) as avg_transaction
FROM retail_dwh.fct_retail_sales
GROUP BY DATE_TRUNC('month', invoice_timestamp)
ORDER BY month;

-- Expected: 13 months of data (Dec 2009 - Dec 2011)


-- 6. TEST MATERIALIZED VIEW: Monthly Sales Summary
SELECT * FROM retail_dwh.mv_monthly_sales_summary
ORDER BY year, month;

-- Expected: 13 rows with monthly aggregates


-- 7. TEST MATERIALIZED VIEW: Top 10 Products
SELECT 
    stock_code,
    description,
    total_quantity_sold,
    total_revenue,
    avg_unit_price
FROM retail_dwh.mv_top_products
ORDER BY total_revenue DESC
LIMIT 10;

-- Expected: Top 10 best-selling products by revenue


-- 8. TEST MATERIALIZED VIEW: Customer Segments
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    AVG(recency_days) as avg_recency,
    AVG(frequency_count) as avg_frequency,
    AVG(monetary_value) as avg_monetary
FROM retail_dwh.mv_customer_segments
GROUP BY customer_segment
ORDER BY customer_segment;

-- Expected: 5 segments (Champions, Loyal, Potential, At Risk, Lost)


-- 9. CHECK DATA QUALITY METRICS
SELECT 
    rule_category,
    rule_name,
    rows_processed,
    rows_passed,
    rows_quarantined,
    rows_dropped,
    execution_timestamp
FROM retail_dwh.dq_metrics
ORDER BY execution_timestamp DESC
LIMIT 20;

-- Expected: Recent data quality metrics from last ETL run


-- 10. CHECK QUARANTINED RECORDS
SELECT 
    dq_reason,
    COUNT(*) as record_count
FROM retail_dwh.dq_quarantine_sales
GROUP BY dq_reason
ORDER BY record_count DESC;

-- Expected: Breakdown of why records were quarantined


-- 11. CHECK ETL BATCH HISTORY
SELECT 
    batch_id,
    batch_start,
    batch_end,
    status,
    rows_extracted,
    rows_loaded,
    rows_quarantined
FROM retail_dwh.meta_etl_batch_log
ORDER BY batch_start DESC
LIMIT 5;

-- Expected: Recent batch execution history


-- 12. VERIFY REFERENTIAL INTEGRITY (No Orphaned Records)
SELECT 
    'Orphaned Products' as check_type,
    COUNT(*) as orphaned_count
FROM retail_dwh.fct_retail_sales f
LEFT JOIN retail_dwh.dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL

UNION ALL

SELECT 
    'Orphaned Customers',
    COUNT(*)
FROM retail_dwh.fct_retail_sales f
LEFT JOIN retail_dwh.dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL

UNION ALL

SELECT 
    'Orphaned Dates',
    COUNT(*)
FROM retail_dwh.fct_retail_sales f
LEFT JOIN retail_dwh.dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;

-- Expected: All counts should be 0 (no orphaned records)


-- 13. TEST QUERY PERFORMANCE WITH PARTITION PRUNING
EXPLAIN ANALYZE
SELECT 
    COUNT(*) as transactions,
    SUM(line_total) as revenue
FROM retail_dwh.fct_retail_sales
WHERE invoice_timestamp BETWEEN '2010-01-01' AND '2010-03-31';

-- Look for: "Partitions scanned" - should only scan 3 partitions (Jan, Feb, Mar 2010)


-- 14. SAMPLE DATA FROM FACT TABLE
SELECT 
    f.invoice_no,
    f.invoice_timestamp,
    p.description as product,
    c.country,
    f.quantity,
    f.unit_price,
    f.line_total,
    f.is_valid_sale
FROM retail_dwh.fct_retail_sales f
JOIN retail_dwh.dim_product p ON f.product_key = p.product_key
JOIN retail_dwh.dim_customer c ON f.customer_key = c.customer_key
JOIN retail_dwh.dim_date d ON f.date_key = d.date_key
ORDER BY f.invoice_timestamp DESC
LIMIT 10;

-- Expected: Recent transactions with full dimension details


-- 15. COUNTRY PERFORMANCE ANALYSIS
SELECT * FROM retail_dwh.mv_country_performance
ORDER BY total_revenue DESC
LIMIT 10;

-- Expected: Top 10 countries by revenue


-- QUICK HEALTH CHECK (Run this first)
SELECT 
    'Tables' as metric, COUNT(*)::text as value
FROM pg_tables WHERE schemaname = 'retail_dwh'
UNION ALL
SELECT 'Materialized Views', COUNT(*)::text
FROM pg_matviews WHERE schemaname = 'retail_dwh'
UNION ALL
SELECT 'Fact Records', COUNT(*)::text
FROM retail_dwh.fct_retail_sales
UNION ALL
SELECT 'Partitions', COUNT(*)::text
FROM pg_tables 
WHERE schemaname = 'retail_dwh' AND tablename LIKE 'fct_retail_sales_%';

-- Expected:
-- Tables: 34
-- Materialized Views: 6
-- Fact Records: ~534,756
-- Partitions: 26
