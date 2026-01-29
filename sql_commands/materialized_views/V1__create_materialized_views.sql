-- Pre-computed aggregations for fast dashboard queries

DROP MATERIALIZED VIEW IF EXISTS retail_dwh.mv_monthly_sales_summary CASCADE;

CREATE MATERIALIZED VIEW retail_dwh.mv_monthly_sales_summary AS
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT f.invoice_no) as total_orders,
    COUNT(*) as total_line_items,
    SUM(f.quantity) as total_units_sold,
    ROUND(SUM(f.line_total), 2) as total_revenue,
    ROUND(AVG(f.line_total), 2) as avg_line_value,
    ROUND(AVG(f.unit_price), 2) as avg_unit_price,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    COUNT(DISTINCT f.product_key) as unique_products,
    SUM(CASE WHEN f.is_valid_sale THEN f.line_total ELSE 0 END) as valid_sales_revenue,
    SUM(CASE WHEN f.is_cancellation THEN f.line_total ELSE 0 END) as cancelled_revenue,
    SUM(CASE WHEN f.is_return THEN 1 ELSE 0 END) as return_count
FROM retail_dwh.fct_retail_sales f
INNER JOIN retail_dwh.dim_date dd ON f.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

CREATE UNIQUE INDEX idx_mv_monthly_sales_year_month 
    ON retail_dwh.mv_monthly_sales_summary(year, month);

COMMENT ON MATERIALIZED VIEW retail_dwh.mv_monthly_sales_summary IS 
'Monthly sales aggregation for dashboards. Refresh daily.';

DROP MATERIALIZED VIEW IF EXISTS retail_dwh.mv_top_products CASCADE;

CREATE MATERIALIZED VIEW retail_dwh.mv_top_products AS
SELECT 
    p.product_key,
    p.stock_code,
    p.description,
    COUNT(DISTINCT f.invoice_no) as order_count,
    SUM(f.quantity) as total_quantity_sold,
    ROUND(SUM(f.line_total), 2) as total_revenue,
    ROUND(AVG(f.unit_price), 2) as avg_selling_price,
    MIN(f.invoice_timestamp) as first_sale_date,
    MAX(f.invoice_timestamp) as last_sale_date,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM retail_dwh.fct_retail_sales f
INNER JOIN retail_dwh.dim_product p ON f.product_key = p.product_key
WHERE f.is_valid_sale = TRUE
GROUP BY p.product_key, p.stock_code, p.description
ORDER BY total_revenue DESC
LIMIT 1000;

CREATE UNIQUE INDEX idx_mv_top_products_key 
    ON retail_dwh.mv_top_products(product_key);

COMMENT ON MATERIALIZED VIEW retail_dwh.mv_top_products IS 
'Top 1000 products by revenue. Refresh daily.';

DROP MATERIALIZED VIEW IF EXISTS retail_dwh.mv_customer_segments CASCADE;

CREATE MATERIALIZED VIEW retail_dwh.mv_customer_segments AS
SELECT 
    c.customer_key,
    c.customer_id,
    c.country,
    c.is_guest,
    COUNT(DISTINCT f.invoice_no) as total_orders,
    SUM(f.quantity) as total_items_purchased,
    ROUND(SUM(f.line_total), 2) as lifetime_value,
    ROUND(AVG(f.line_total), 2) as avg_order_line_value,
    MIN(f.invoice_timestamp) as first_purchase_date,
    MAX(f.invoice_timestamp) as last_purchase_date,
    (MAX(f.invoice_timestamp::DATE) - MIN(f.invoice_timestamp::DATE)) as customer_lifespan_days,
    COUNT(DISTINCT f.product_key) as unique_products_purchased,
    -- RFM Segmentation
    (CURRENT_DATE - MAX(f.invoice_timestamp)::DATE) as recency_days,
    COUNT(DISTINCT f.invoice_no) as frequency,
    ROUND(SUM(f.line_total), 2) as monetary_value,
    -- Customer Tier
    CASE 
        WHEN SUM(f.line_total) >= 10000 THEN 'VIP'
        WHEN SUM(f.line_total) >= 5000 THEN 'Gold'
        WHEN SUM(f.line_total) >= 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_tier
FROM retail_dwh.fct_retail_sales f
INNER JOIN retail_dwh.dim_customer c ON f.customer_key = c.customer_key
WHERE f.is_valid_sale = TRUE
  AND c.is_guest = FALSE
GROUP BY c.customer_key, c.customer_id, c.country, c.is_guest
ORDER BY lifetime_value DESC;

CREATE UNIQUE INDEX idx_mv_customer_segments_key 
    ON retail_dwh.mv_customer_segments(customer_key);

CREATE INDEX idx_mv_customer_segments_tier 
    ON retail_dwh.mv_customer_segments(customer_tier);

COMMENT ON MATERIALIZED VIEW retail_dwh.mv_customer_segments IS 
'Customer segmentation with RFM analysis. Refresh daily.';

DROP MATERIALIZED VIEW IF EXISTS retail_dwh.mv_daily_sales_trend CASCADE;

CREATE MATERIALIZED VIEW retail_dwh.mv_daily_sales_trend AS
SELECT 
    dd.date_value,
    dd.year,
    dd.month,
    dd.day_of_week,
    dd.is_weekend,
    COUNT(DISTINCT f.invoice_no) as order_count,
    SUM(f.quantity) as units_sold,
    ROUND(SUM(f.line_total), 2) as revenue,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    COUNT(DISTINCT f.product_key) as unique_products,
    ROUND(AVG(SUM(f.line_total)) OVER (
        ORDER BY dd.date_value 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as revenue_7day_ma,
    ROUND(AVG(SUM(f.line_total)) OVER (
        ORDER BY dd.date_value 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) as revenue_30day_ma
FROM retail_dwh.fct_retail_sales f
INNER JOIN retail_dwh.dim_date dd ON f.date_key = dd.date_key
WHERE f.is_valid_sale = TRUE
GROUP BY dd.date_value, dd.year, dd.month, dd.day_of_week, dd.is_weekend
ORDER BY dd.date_value;

CREATE UNIQUE INDEX idx_mv_daily_sales_date 
    ON retail_dwh.mv_daily_sales_trend(date_value);

COMMENT ON MATERIALIZED VIEW retail_dwh.mv_daily_sales_trend IS 
'Daily sales with moving averages. Refresh daily.';

DROP MATERIALIZED VIEW IF EXISTS retail_dwh.mv_country_performance CASCADE;

CREATE MATERIALIZED VIEW retail_dwh.mv_country_performance AS
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_key) as customer_count,
    COUNT(DISTINCT f.invoice_no) as order_count,
    SUM(f.quantity) as units_sold,
    ROUND(SUM(f.line_total), 2) as total_revenue,
    ROUND(AVG(f.line_total), 2) as avg_line_value,
    ROUND(SUM(f.line_total) / COUNT(DISTINCT c.customer_key), 2) as revenue_per_customer,
    MIN(f.invoice_timestamp) as first_order_date,
    MAX(f.invoice_timestamp) as last_order_date,
    COUNT(DISTINCT f.product_key) as unique_products_sold
FROM retail_dwh.fct_retail_sales f
INNER JOIN retail_dwh.dim_customer c ON f.customer_key = c.customer_key
WHERE f.is_valid_sale = TRUE
  AND c.country IS NOT NULL
GROUP BY c.country
ORDER BY total_revenue DESC;

CREATE UNIQUE INDEX idx_mv_country_performance 
    ON retail_dwh.mv_country_performance(country);

COMMENT ON MATERIALIZED VIEW retail_dwh.mv_country_performance IS 
'Revenue and performance by country. Refresh daily.';

DROP MATERIALIZED VIEW IF EXISTS retail_dwh.mv_product_category_analysis CASCADE;

CREATE MATERIALIZED VIEW retail_dwh.mv_product_category_analysis AS
SELECT 
    LEFT(p.stock_code, 2) as category_code,
    COUNT(DISTINCT p.product_key) as product_count,
    SUM(f.quantity) as total_units_sold,
    ROUND(SUM(f.line_total), 2) as total_revenue,
    ROUND(AVG(f.unit_price), 2) as avg_price,
    COUNT(DISTINCT f.invoice_no) as order_count,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM retail_dwh.fct_retail_sales f
INNER JOIN retail_dwh.dim_product p ON f.product_key = p.product_key
WHERE f.is_valid_sale = TRUE
GROUP BY LEFT(p.stock_code, 2)
ORDER BY total_revenue DESC;

CREATE UNIQUE INDEX idx_mv_category_analysis 
    ON retail_dwh.mv_product_category_analysis(category_code);

COMMENT ON MATERIALIZED VIEW retail_dwh.mv_product_category_analysis IS 
'Product category performance. Refresh daily.';



-- Manual refresh:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY retail_dwh.mv_monthly_sales_summary;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY retail_dwh.mv_top_products;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY retail_dwh.mv_customer_segments;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY retail_dwh.mv_daily_sales_trend;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY retail_dwh.mv_country_performance;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY retail_dwh.mv_product_category_analysis;

-- Or
CREATE OR REPLACE FUNCTION retail_dwh.refresh_all_materialized_views()
RETURNS TABLE(view_name TEXT, status TEXT, duration INTERVAL) AS $$
DECLARE
    mv_record RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    FOR mv_record IN 
        SELECT schemaname, matviewname 
        FROM pg_matviews 
        WHERE schemaname = 'retail_dwh'
        ORDER BY matviewname
    LOOP
        start_time := clock_timestamp();
        
        BEGIN
            EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %I.%I', 
                          mv_record.schemaname, 
                          mv_record.matviewname);
            end_time := clock_timestamp();
            
            view_name := mv_record.matviewname;
            status := ' SUCCESS';
            duration := end_time - start_time;
            RETURN NEXT;
            
        EXCEPTION WHEN OTHERS THEN
            end_time := clock_timestamp();
            view_name := mv_record.matviewname;
            status := ' FAILED: ' || SQLERRM;
            duration := end_time - start_time;
            RETURN NEXT;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

