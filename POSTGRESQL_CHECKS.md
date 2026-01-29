# PostgreSQL Verification Queries

## Run these queries after the pipeline completes to verify everything works correctly.

---

## 1. Check All Tables

```sql
SELECT tablename, 
       pg_size_pretty(pg_total_relation_size('retail_dwh.' || tablename)) as size
FROM pg_tables 
WHERE schemaname = 'retail_dwh'
ORDER BY tablename;
```

**Expected:** 34 tables (3 staging/DQ, 3 dimensions, 1 fact + 26 partitions, 1 old fact backup)

---

## 2. Check Materialized Views

```sql
SELECT matviewname, 
       pg_size_pretty(pg_total_relation_size('retail_dwh.' || matviewname)) as size
FROM pg_matviews 
WHERE schemaname = 'retail_dwh'
ORDER BY matviewname;
```

**Expected:** 6 materialized views

---

## 3. Verify Row Counts

```sql
SELECT 
    (SELECT COUNT(*) FROM retail_dwh.stg_retail_sales) as staging_rows,
    (SELECT COUNT(*) FROM retail_dwh.dim_date) as dim_date_rows,
    (SELECT COUNT(*) FROM retail_dwh.dim_product) as dim_product_rows,
    (SELECT COUNT(*) FROM retail_dwh.dim_customer) as dim_customer_rows,
    (SELECT COUNT(*) FROM retail_dwh.fct_retail_sales) as fact_rows;
```

**Expected:**
- staging_rows: ~536,629
- dim_date_rows: 373
- dim_product_rows: ~3,811
- dim_customer_rows: ~4,339
- fact_rows: ~534,756

---

## 4. Check Partitions

```sql
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'retail_dwh' 
  AND tablename LIKE 'fct_retail_sales_%'
ORDER BY tablename;
```

**Expected:** 26 monthly partitions (fct_retail_sales_2009_12 through fct_retail_sales_2011_12)

---

## 5. Verify Fact Table is Partitioned

```sql
SELECT 
    c.relname as table_name,
    CASE 
        WHEN c.relkind = 'p' THEN 'Partitioned Table'
        WHEN c.relkind = 'r' THEN 'Regular Table'
    END as table_type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'retail_dwh'
  AND c.relname = 'fct_retail_sales';
```

**Expected:** table_type = 'Partitioned Table'

---

## 6. Test Materialized View - Monthly Sales

```sql
SELECT 
    year,
    month,
    month_name,
    total_orders,
    total_revenue,
    avg_order_value
FROM retail_dwh.mv_monthly_sales_summary
ORDER BY year, month
LIMIT 12;
```

**Expected:** 12+ rows with monthly aggregates

---

## 7. Test Materialized View - Top Products

```sql
SELECT 
    product_description,
    total_quantity,
    total_revenue,
    avg_unit_price,
    times_ordered
FROM retail_dwh.mv_top_products
LIMIT 10;
```

**Expected:** 10 rows with top-selling products

---

## 8. Test Materialized View - Customer Segments

```sql
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    AVG(total_revenue) as avg_revenue,
    AVG(recency_days) as avg_recency
FROM retail_dwh.mv_customer_segments
GROUP BY customer_segment
ORDER BY customer_count DESC;
```

**Expected:** Multiple customer segments with counts

---

## 9. Check Data Quality Metrics

```sql
SELECT 
    rule_category,
    COUNT(*) as rules_executed,
    SUM(rows_processed) as total_rows_processed,
    SUM(rows_quarantined) as total_quarantined
FROM retail_dwh.dq_metrics
GROUP BY rule_category
ORDER BY rule_category;
```

**Expected:** 4 categories (completeness, validity, business_logic, integrity)

---

## 10. Check Quarantined Records

```sql
SELECT 
    dq_reason,
    COUNT(*) as count
FROM retail_dwh.dq_quarantine_sales
GROUP BY dq_reason
ORDER BY count DESC
LIMIT 10;
```

**Expected:** Various DQ failure reasons with counts

---

## 11. Verify Foreign Key Integrity

```sql
-- Check for orphaned product records
SELECT COUNT(*) as orphaned_products
FROM retail_dwh.fct_retail_sales f
LEFT JOIN retail_dwh.dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- Check for orphaned customer records
SELECT COUNT(*) as orphaned_customers
FROM retail_dwh.fct_retail_sales f
LEFT JOIN retail_dwh.dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- Check for orphaned date records
SELECT COUNT(*) as orphaned_dates
FROM retail_dwh.fct_retail_sales f
LEFT JOIN retail_dwh.dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;
```

**Expected:** All counts should be 0

---

## 12. Test Partition Pruning

```sql
EXPLAIN ANALYZE
SELECT 
    COUNT(*),
    SUM(line_total)
FROM retail_dwh.fct_retail_sales
WHERE invoice_timestamp BETWEEN '2010-01-01' AND '2010-01-31';
```

**Expected:** Query plan should show "Partitions scanned: fct_retail_sales_2010_01" (only 1 partition)

---

## 13. Check Batch History

```sql
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
```

**Expected:** Recent batch executions with SUCCESS status

---

## 14. Performance Test - With vs Without Materialized View

```sql
-- Direct query (slower)
SELECT 
    EXTRACT(YEAR FROM dd.date_value) as year,
    EXTRACT(MONTH FROM dd.date_value) as month,
    COUNT(DISTINCT f.invoice_no) as orders,
    SUM(f.line_total) as revenue
FROM retail_dwh.fct_retail_sales f
JOIN retail_dwh.dim_date dd ON f.date_key = dd.date_key
GROUP BY EXTRACT(YEAR FROM dd.date_value), EXTRACT(MONTH FROM dd.date_value)
ORDER BY year, month;

-- Materialized view (much faster)
SELECT year, month, total_orders, total_revenue
FROM retail_dwh.mv_monthly_sales_summary
ORDER BY year, month;
```

**Expected:** Materialized view should be 100-1000x faster

---

## 15. Check Indexes

```sql
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'retail_dwh'
  AND tablename IN ('fct_retail_sales', 'dim_date', 'dim_product', 'dim_customer')
ORDER BY tablename, indexname;
```

**Expected:** Primary keys and foreign key indexes on all tables

---

## Summary Check - Run This First!

```sql
SELECT 
    'Tables' as metric,
    COUNT(*)::text as count
FROM pg_tables 
WHERE schemaname = 'retail_dwh'

UNION ALL

SELECT 
    'Materialized Views',
    COUNT(*)::text
FROM pg_matviews 
WHERE schemaname = 'retail_dwh'

UNION ALL

SELECT 
    'Fact Records',
    (SELECT COUNT(*)::text FROM retail_dwh.fct_retail_sales)

UNION ALL

SELECT 
    'Products',
    (SELECT COUNT(*)::text FROM retail_dwh.dim_product)

UNION ALL

SELECT 
    'Customers',
    (SELECT COUNT(*)::text FROM retail_dwh.dim_customer)

UNION ALL

SELECT 
    'Date Range',
    (SELECT MIN(date_value)::text || ' to ' || MAX(date_value)::text FROM retail_dwh.dim_date);
```

**Expected Results:**
- Tables: 34
- Materialized Views: 6
- Fact Records: ~534,756
- Products: ~3,811
- Customers: ~4,339
- Date Range: 2009-12-01 to 2011-12-09
