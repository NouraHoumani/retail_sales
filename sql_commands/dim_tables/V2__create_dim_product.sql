
DROP TABLE IF EXISTS retail_dwh.dim_product CASCADE;
CREATE TABLE IF NOT EXISTS retail_dwh.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_pk_id TEXT NOT NULL UNIQUE,
    stock_code VARCHAR(50) NOT NULL UNIQUE,
    description TEXT NOT NULL,
    avg_unit_price NUMERIC(10, 2),
    min_unit_price NUMERIC(10, 2),
    max_unit_price NUMERIC(10, 2),
    first_seen_date DATE,
    last_seen_date DATE,
    total_transactions INTEGER,
    source TEXT NOT NULL DEFAULT 'retail_sales',
    is_active BOOLEAN DEFAULT TRUE,
    etl_modified_on TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_product_stock_code 
    ON retail_dwh.dim_product(stock_code);
CREATE INDEX IF NOT EXISTS idx_dim_product_pk_id 
    ON retail_dwh.dim_product(product_pk_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_active 
    ON retail_dwh.dim_product(is_active);

COMMENT ON TABLE retail_dwh.dim_product IS 
'Product dimension (SCD Type 1): one row per unique product';

INSERT INTO retail_dwh.dim_product (
    product_pk_id,
    stock_code,
    description,
    avg_unit_price,
    min_unit_price,
    max_unit_price,
    first_seen_date,
    last_seen_date,
    total_transactions,
    source,
    is_active,
    etl_modified_on
)
SELECT
    MD5(LOWER(TRIM(stock_code)) || '|retail_sales') as product_pk_id,
    stock_code,
    MAX(description) as description,
    ROUND(AVG(unit_price), 2) as avg_unit_price,
    MIN(unit_price) as min_unit_price,
    MAX(unit_price) as max_unit_price,
    MIN(invoice_date::DATE) as first_seen_date,
    MAX(invoice_date::DATE) as last_seen_date,
    COUNT(*) as total_transactions,
    'retail_sales' as source,
    TRUE as is_active,
    CURRENT_TIMESTAMP as etl_modified_on
FROM retail_dwh.stg_retail_sales
WHERE is_valid_sale = TRUE
GROUP BY stock_code
ON CONFLICT (stock_code) DO UPDATE SET
    description = EXCLUDED.description,
    avg_unit_price = EXCLUDED.avg_unit_price,
    min_unit_price = EXCLUDED.min_unit_price,
    max_unit_price = EXCLUDED.max_unit_price,
    last_seen_date = EXCLUDED.last_seen_date,
    total_transactions = EXCLUDED.total_transactions,
    etl_modified_on = CURRENT_TIMESTAMP;


-- SELECT 
--     'dim_product' as table_name,
--     COUNT(*) as row_count,
--     COUNT(DISTINCT stock_code) as unique_products,
--     MIN(first_seen_date) as earliest_product,
--     MAX(last_seen_date) as latest_product
-- FROM retail_dwh.dim_product;