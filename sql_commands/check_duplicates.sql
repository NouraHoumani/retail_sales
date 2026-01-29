
SELECT 
    'SUMMARY' as check_type,
    'stg_retail_sales' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT (invoice_no, stock_code, invoice_date, quantity, unit_price)) as unique_business_keys
FROM retail_dwh.stg_retail_sales

UNION ALL

SELECT 
    'SUMMARY',
    'dim_date',
    COUNT(*),
    COUNT(DISTINCT date_value)
FROM retail_dwh.dim_date

UNION ALL

SELECT 
    'SUMMARY',
    'dim_product',
    COUNT(*),
    COUNT(DISTINCT stock_code)
FROM retail_dwh.dim_product

UNION ALL

SELECT 
    'SUMMARY',
    'dim_customer',
    COUNT(*),
    COUNT(DISTINCT customer_id)
FROM retail_dwh.dim_customer

UNION ALL

SELECT 
    'SUMMARY',
    'fct_retail_sales',
    COUNT(*),
    COUNT(DISTINCT sales_pk_id)
FROM retail_dwh.fct_retail_sales

ORDER BY table_name;