
DROP TABLE IF EXISTS retail_dwh.dim_customerCASCADE; 
CREATE TABLE IF NOT EXISTS retail_dwh.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_pk_id TEXT NOT NULL UNIQUE,
    customer_id BIGINT,  --  Fixed from NUMERIC
    country VARCHAR(100),
    is_guest BOOLEAN DEFAULT FALSE,
    first_purchase_date DATE,
    last_purchase_date DATE,
    total_orders INTEGER,
    total_revenue NUMERIC(12, 2),
    source TEXT NOT NULL DEFAULT 'retail_sales',
    is_active BOOLEAN DEFAULT TRUE,
    etl_modified_on TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id 
    ON retail_dwh.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_pk_id 
    ON retail_dwh.dim_customer(customer_pk_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_country 
    ON retail_dwh.dim_customer(country);
CREATE INDEX IF NOT EXISTS idx_dim_customer_guest 
    ON retail_dwh.dim_customer(is_guest);

COMMENT ON TABLE retail_dwh.dim_customer IS 
'Customer dimension: one row per customer + one row for all guests';

INSERT INTO retail_dwh.dim_customer (
    customer_pk_id,
    customer_id,
    country,
    is_guest,
    first_purchase_date,
    last_purchase_date,
    total_orders,
    total_revenue,
    source,
    is_active,
    etl_modified_on
)
SELECT
    MD5(COALESCE(customer_id::TEXT, 'NULL') || '|retail_sales') as customer_pk_id,
    customer_id,
    MAX(country) as country,
    FALSE as is_guest,
    MIN(invoice_date::DATE) as first_purchase_date,
    MAX(invoice_date::DATE) as last_purchase_date,
    COUNT(DISTINCT invoice_no) as total_orders,
    SUM(line_total) as total_revenue,
    'retail_sales' as source,
    TRUE as is_active,
    CURRENT_TIMESTAMP as etl_modified_on
FROM retail_dwh.stg_retail_sales
WHERE customer_id IS NOT NULL
  AND is_valid_sale = TRUE
GROUP BY customer_id
ON CONFLICT (customer_pk_id) DO UPDATE SET
    country = EXCLUDED.country,
    last_purchase_date = EXCLUDED.last_purchase_date,
    total_orders = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue,
    etl_modified_on = CURRENT_TIMESTAMP;

INSERT INTO retail_dwh.dim_customer (
    customer_pk_id,
    customer_id,
    country,
    is_guest,
    first_purchase_date,
    last_purchase_date,
    total_orders,
    total_revenue,
    source,
    is_active,
    etl_modified_on
)
SELECT
    MD5('GUEST|retail_sales') as customer_pk_id,
    NULL as customer_id,
    'MULTIPLE' as country,
    TRUE as is_guest,
    MIN(invoice_date::DATE) as first_purchase_date,
    MAX(invoice_date::DATE) as last_purchase_date,
    COUNT(DISTINCT invoice_no) as total_orders,
    SUM(line_total) as total_revenue,
    'retail_sales' as source,
    TRUE as is_active,
    CURRENT_TIMESTAMP as etl_modified_on
FROM retail_dwh.stg_retail_sales
WHERE customer_id IS NULL
  AND is_valid_sale = TRUE
ON CONFLICT (customer_pk_id) DO UPDATE SET
    last_purchase_date = EXCLUDED.last_purchase_date,
    total_orders = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue,
    etl_modified_on = CURRENT_TIMESTAMP;

-- Verify
-- SELECT 
--     'dim_customer' as table_name,
--     COUNT(*) as row_count,
--     SUM(CASE WHEN is_guest THEN 1 ELSE 0 END) as guest_records,
--     SUM(CASE WHEN NOT is_guest THEN 1 ELSE 0 END) as registered_customers,
--     ROUND(SUM(total_revenue), 2) as total_revenue
-- FROM retail_dwh.dim_customer;