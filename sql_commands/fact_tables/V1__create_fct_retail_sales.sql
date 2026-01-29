DROP TABLE IF EXISTS retail_dwh.fct_retail_sales  CASCADE;
CREATE TABLE retail_dwh.fct_retail_sales (
    sales_key SERIAL PRIMARY KEY,
    sales_pk_id TEXT NOT NULL UNIQUE,
    
    product_key INTEGER NOT NULL REFERENCES retail_dwh.dim_product(product_key),
    customer_key INTEGER NOT NULL REFERENCES retail_dwh.dim_customer(customer_key),
    date_key INTEGER NOT NULL REFERENCES retail_dwh.dim_date(date_key),
    
    invoice_no VARCHAR(50) NOT NULL,
    invoice_timestamp TIMESTAMP NOT NULL,
    
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    line_total NUMERIC(12, 2) NOT NULL,
    
    is_cancellation BOOLEAN DEFAULT FALSE,
    is_valid_sale BOOLEAN DEFAULT FALSE,
    is_return BOOLEAN DEFAULT FALSE,
    is_guest_purchase BOOLEAN DEFAULT FALSE,
    
    source TEXT NOT NULL DEFAULT 'retail_sales',
    batch_id VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    etl_modified_on TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fct_sales_product 
    ON retail_dwh.fct_retail_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_customer 
    ON retail_dwh.fct_retail_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_date 
    ON retail_dwh.fct_retail_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_invoice 
    ON retail_dwh.fct_retail_sales(invoice_no);
CREATE INDEX IF NOT EXISTS idx_fct_sales_timestamp 
    ON retail_dwh.fct_retail_sales(invoice_timestamp);
CREATE INDEX IF NOT EXISTS idx_fct_sales_valid 
    ON retail_dwh.fct_retail_sales(is_valid_sale);
CREATE INDEX IF NOT EXISTS idx_fct_sales_pk_id 
    ON retail_dwh.fct_retail_sales(sales_pk_id);

COMMENT ON TABLE retail_dwh.fct_retail_sales IS 
'Sales fact table: one row per invoice line item';

INSERT INTO retail_dwh.fct_retail_sales (
    sales_pk_id,
    product_key,
    customer_key,
    date_key,
    invoice_no,
    invoice_timestamp,
    quantity,
    unit_price,
    line_total,
    is_cancellation,
    is_valid_sale,
    is_return,
    is_guest_purchase,
    source,
    batch_id,
    loaded_at,
    etl_modified_on
)
SELECT
    MD5(
        LOWER(TRIM(stg.invoice_no)) ||
        '|' || LOWER(TRIM(stg.stock_code)) ||
        '|' || TO_CHAR(stg.invoice_date, 'YYYY-MM-DD HH24:MI:SS') ||
        '|' || stg.quantity::TEXT ||
        '|' || stg.unit_price::TEXT
    ) as sales_pk_id,
    dp.product_key,
    COALESCE(
        dc.customer_key, 
        (SELECT customer_key FROM retail_dwh.dim_customer WHERE is_guest = TRUE LIMIT 1)
    ) as customer_key,
    dd.date_key,
    stg.invoice_no,
    stg.invoice_date as invoice_timestamp,
    stg.quantity,
    stg.unit_price,
    stg.line_total,
    stg.is_cancellation,
    stg.is_valid_sale,
    stg.is_return,
    stg.is_guest_purchase,
    'retail_sales' as source,
    stg.batch_id,
    stg.loaded_at,
    CURRENT_TIMESTAMP as etl_modified_on
FROM retail_dwh.stg_retail_sales stg
INNER JOIN retail_dwh.dim_product dp 
    ON LOWER(TRIM(stg.stock_code)) = LOWER(TRIM(dp.stock_code))
LEFT JOIN retail_dwh.dim_customer dc 
    ON stg.customer_id = dc.customer_id
INNER JOIN retail_dwh.dim_date dd 
    ON stg.invoice_date::DATE = dd.date_value
ON CONFLICT (sales_pk_id) DO NOTHING;

-- SELECT 
--     'fct_retail_sales' as table_name,
--     COUNT(*) as row_count,
--     COUNT(DISTINCT sales_pk_id) as unique_sales,
--     SUM(CASE WHEN is_valid_sale THEN 1 ELSE 0 END) as valid_sales,
--     ROUND(SUM(line_total), 2) as total_revenue,
--     ROUND(SUM(CASE WHEN is_valid_sale THEN line_total ELSE 0 END), 2) as valid_revenue
-- FROM retail_dwh.fct_retail_sales;