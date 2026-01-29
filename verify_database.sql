-- PostgreSQL Verification Queries
-- Run these queries after the pipeline completes to verify everything works

-- ============================================
-- 1. CHECK ALL TABLES
-- ============================================
SELECT tablename, schemaname
FROM pg_tables 
WHERE schemaname = 'retail_dwh'
ORDER BY tablename;
-- Expected: 34 tables (3 dims, 1 fact, 26 partitions, 4 system tables)

-- ============================================
-- 2. CHECK MATERIALIZED VIEWS
-- ============================================
SELECT matviewname, schemaname
FROM pg_matviews 
WHERE schemaname = 'retail_dwh'
ORDER BY matviewname;
-- Expected: 6 materialized views

-- ============================================
-- 3. CHECK ROW COUNTS
-- ============================================
SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM retail_dwh.dim_date
UNION ALL
SELECT 'dim_product', COUNT(*) FROM retail_dwh.dim_product
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM retail_dwh.dim_customer
UNION ALL
SELECT 'fct_retail_sales', COUNT(*) FROM retail_dwh.fct_retail_sales
UNION ALL
SELECT 'stg_retail_sales', COUNT(*) FROM retail_dwh.stg_retail_sales;
-- Expected:
-- dim_date: 373
-- dim_product: 3,811
-- dim_customer: 4,339
-- fct_retail_sales: 534,756
-- stg_retail_sales: 536,629

-- ============================================
-- 4. CHECK PARTITIONS
-- ============================================
SELECT tablename
FROM pg_tables
WHERE schemaname = 'retail_dwh'
  AND tablename LIKE 'fct_retail_sales_%'
ORDER BY tablename;
-- Expected: 26 monthly partitions (2009-12 to 2011-12)

-- ============================================
-- 5. VERIFY DATA QUALITY
-- ============================================
-- Check for orphaned records (should be 0)
SELECT COUNT(*) as orphaned_products
FROM retail_dwh.fct_retail_sales f
WHERE NOT EXISTS (
    SELECT 1 FROM retail_dwh.dim_product p
    WHERE p.product_key = f.product_key
);
-- Expected: 0

SELECT COUNT(*) as orphaned_customers
FROM retail_dwh.fct_retail_sales f
WHERE NOT EXISTS (
    SELECT 1 FROM retail_dwh.dim_customer c
    WHERE c.customer_key = f.customer_key
);
-- Expected: 0

SELECT COUNT(*) as orphaned_dates
FROM retail_dwh.fct_retail_sales f
WHERE NOT EXISTS (
    SELECT 1 FROM retail_dwh.dim_date d
    WHERE d.date_key = f.date_key
);
-- Expected: 0

-- ============================================
-- 6. CHECK MATERIALIZED VIEW DATA
-- ============================================
SELECT 'mv_monthly_sales_summary' as view_name, COUNT(*) as row_count 
FROM retail_dwh.mv_monthly_sales_summary
UNION ALL
SELECT 'mv_top_products', COUNT(*) FROM retail_dwh.mv_top_products
UNION ALL
SELECT 'mv_customer_segments', COUNT(*) FROM retail_dwh.mv_customer_segments
UNION ALL
SELECT 'mv_daily_sales_trend', COUNT(*) FROM retail_dwh.mv_daily_sales_trend
UNION ALL
SELECT 'mv_country_performance', COUNT(*) FROM retail_dwh.mv_country_performance
UNION ALL
SELECT 'mv_product_category_analysis', COUNT(*) FROM retail_dwh.mv_product_category_analysis;
-- Expected:
-- mv_monthly_sales_summary: 13
-- mv_top_products: 1,000
-- mv_customer_segments: 4,337
-- mv_daily_sales_trend: 304
-- mv_country_performance: 38
-- mv_product_category_analysis: 42

-- ============================================
-- 7. DATA QUALITY METRICS
-- ============================================
SELECT * FROM retail_dwh.dq_metrics 
ORDER BY execution_timestamp DESC 
LIMIT 10;
-- Shows recent data quality checks

SELECT dq_reason, COUNT(*) as count
FROM retail_dwh.dq_quarantine_sales
GROUP BY dq_reason
ORDER BY count DESC;
-- Shows quarantined records by reason

-- ============================================
-- 8. BATCH EXECUTION HISTORY
-- ============================================
SELECT batch_id, batch_start, batch_end, status, 
       rows_extracted, rows_loaded, rows_quarantined
FROM retail_dwh.meta_etl_batch_log
ORDER BY batch_start DESC
LIMIT 5;
-- Shows recent ETL batch executions

-- ============================================
-- 9. SAMPLE QUERIES - MONTHLY SALES
-- ============================================
SELECT year, month_name, total_revenue, total_orders
FROM retail_dwh.mv_monthly_sales_summary
ORDER BY year, month;
-- Shows monthly sales summary

-- ============================================
-- 10. SAMPLE QUERIES - TOP 10 PRODUCTS
-- ============================================
SELECT product_rank, stock_code, description, 
       total_quantity_sold, total_revenue
FROM retail_dwh.mv_top_products
WHERE product_rank <= 10
ORDER BY product_rank;
-- Shows top 10 selling products

-- ============================================
-- 11. SAMPLE QUERIES - CUSTOMER SEGMENTS
-- ============================================
SELECT customer_segment, COUNT(*) as customer_count,
       AVG(total_orders) as avg_orders,
       AVG(total_revenue) as avg_revenue
FROM retail_dwh.mv_customer_segments
GROUP BY customer_segment
ORDER BY customer_count DESC;
-- Shows customer segmentation summary

-- ============================================
-- 12. PARTITION VERIFICATION
-- ============================================
SELECT 
    c.relname as partition_name,
    pg_size_pretty(pg_total_relation_size(c.oid)) as size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'retail_dwh'
  AND c.relkind = 'r'
  AND c.relname LIKE 'fct_retail_sales_%'
ORDER BY c.relname;
-- Shows partition sizes

-- ============================================
-- 13. CHECK FOR NULLS IN CRITICAL COLUMNS
-- ============================================
SELECT 
    COUNT(*) FILTER (WHERE invoice_no IS NULL) as null_invoice,
    COUNT(*) FILTER (WHERE stock_code IS NULL) as null_stock_code,
    COUNT(*) FILTER (WHERE quantity IS NULL) as null_quantity,
    COUNT(*) FILTER (WHERE unit_price IS NULL) as null_price
FROM retail_dwh.stg_retail_sales;
-- All should be 0 in staging

-- ============================================
-- 14. VERIFY TABLE IS PARTITIONED
-- ============================================
SELECT 
    c.relname,
    CASE c.relkind
        WHEN 'r' THEN 'regular table'
        WHEN 'p' THEN 'partitioned table'
    END as table_type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'retail_dwh'
  AND c.relname = 'fct_retail_sales';
-- Should show 'partitioned table'

-- ============================================
-- 15. QUICK DATA SANITY CHECK
-- ============================================
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT invoice_no) as unique_invoices,
    MIN(invoice_timestamp) as earliest_date,
    MAX(invoice_timestamp) as latest_date,
    SUM(line_total) as total_revenue,
    AVG(line_total) as avg_transaction_value
FROM retail_dwh.fct_retail_sales;
-- Quick sanity check on fact table data
