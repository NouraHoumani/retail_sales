
DROP TABLE IF EXISTS retail_dwh.dim_date CASCADE; 
CREATE TABLE IF NOT EXISTS retail_dwh.dim_date (
    date_key INTEGER PRIMARY KEY,
    date_value DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    etl_modified_on TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_date_value 
    ON retail_dwh.dim_date(date_value);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month 
    ON retail_dwh.dim_date(year, month);

COMMENT ON TABLE retail_dwh.dim_date IS 
'Date dimension: dynamically generated from staging data date range';

INSERT INTO retail_dwh.dim_date (
    date_key,
    date_value,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    week_of_year,
    is_weekend,
    etl_modified_on
)
SELECT
    TO_CHAR(date_value, 'YYYYMMDD')::INTEGER as date_key,
    date_value,
    EXTRACT(YEAR FROM date_value)::INTEGER as year,
    EXTRACT(QUARTER FROM date_value)::INTEGER as quarter,
    EXTRACT(MONTH FROM date_value)::INTEGER as month,
    TRIM(TO_CHAR(date_value, 'Month')) as month_name,
    EXTRACT(DAY FROM date_value)::INTEGER as day,
    EXTRACT(DOW FROM date_value)::INTEGER as day_of_week,
    TRIM(TO_CHAR(date_value, 'Day')) as day_name,
    EXTRACT(WEEK FROM date_value)::INTEGER as week_of_year,
    CASE WHEN EXTRACT(DOW FROM date_value) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
    CURRENT_TIMESTAMP as etl_modified_on
FROM (
    SELECT generate_series(
        (SELECT MIN(invoice_date::DATE) FROM retail_dwh.stg_retail_sales),
        (SELECT MAX(invoice_date::DATE) FROM retail_dwh.stg_retail_sales),
        '1 day'::INTERVAL
    )::DATE as date_value
) dates;

-- SELECT 
--     'dim_date' as table_name,
--     COUNT(*) as row_count,
--     MIN(date_value) as min_date,
--     MAX(date_value) as max_date
-- FROM retail_dwh.dim_date;