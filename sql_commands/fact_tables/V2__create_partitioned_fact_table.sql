-- Partitions the fact table by month for better query performance
-- Run this AFTER initial data load, or migrate existing data
-- IDEMPOTENT: Safe to re-run

BEGIN;


DO $$
BEGIN
   
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'retail_dwh'
        AND c.relname = 'fct_retail_sales'
        AND c.relkind = 'p'  -- 'p' means partitioned table
    ) THEN
        RAISE NOTICE 'Table fct_retail_sales is already partitioned. Skipping migration.';
        RETURN;
    END IF;
END $$;

DROP TABLE IF EXISTS retail_dwh.fct_retail_sales_old CASCADE;

ALTER TABLE IF EXISTS retail_dwh.fct_retail_sales 
    RENAME TO fct_retail_sales_old;

CREATE TABLE retail_dwh.fct_retail_sales (
    sales_key SERIAL,
    sales_pk_id TEXT NOT NULL,
    
    product_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    
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
    etl_modified_on TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (sales_key, invoice_timestamp)
) PARTITION BY RANGE (invoice_timestamp);


CREATE TABLE retail_dwh.fct_retail_sales_2009_12 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2009-12-01') TO ('2010-01-01');

-- 2010 Partitions
CREATE TABLE retail_dwh.fct_retail_sales_2010_01 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-01-01') TO ('2010-02-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_02 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-02-01') TO ('2010-03-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_03 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-03-01') TO ('2010-04-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_04 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-04-01') TO ('2010-05-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_05 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-05-01') TO ('2010-06-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_06 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-06-01') TO ('2010-07-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_07 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-07-01') TO ('2010-08-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_08 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-08-01') TO ('2010-09-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_09 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-09-01') TO ('2010-10-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_10 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-10-01') TO ('2010-11-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_11 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-11-01') TO ('2010-12-01');
CREATE TABLE retail_dwh.fct_retail_sales_2010_12 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2010-12-01') TO ('2011-01-01');

-- 2011 Partitions
CREATE TABLE retail_dwh.fct_retail_sales_2011_01 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-01-01') TO ('2011-02-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_02 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-02-01') TO ('2011-03-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_03 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-03-01') TO ('2011-04-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_04 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-04-01') TO ('2011-05-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_05 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-05-01') TO ('2011-06-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_06 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-06-01') TO ('2011-07-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_07 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-07-01') TO ('2011-08-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_08 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-08-01') TO ('2011-09-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_09 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-09-01') TO ('2011-10-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_10 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-10-01') TO ('2011-11-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_11 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-11-01') TO ('2011-12-01');
CREATE TABLE retail_dwh.fct_retail_sales_2011_12 PARTITION OF retail_dwh.fct_retail_sales
    FOR VALUES FROM ('2011-12-01') TO ('2012-01-01');


DO $$
DECLARE
    partition_name TEXT;
BEGIN
    FOR partition_name IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'retail_dwh' 
        AND tablename LIKE 'fct_retail_sales_20%'
    LOOP
        EXECUTE format('ALTER TABLE retail_dwh.%I ADD CONSTRAINT %I UNIQUE (sales_pk_id)', 
                      partition_name, 
                      partition_name || '_sales_pk_id_key');
    END LOOP;
END $$;

ALTER TABLE retail_dwh.fct_retail_sales 
    ADD CONSTRAINT fct_sales_product_fk 
    FOREIGN KEY (product_key) REFERENCES retail_dwh.dim_product(product_key);

ALTER TABLE retail_dwh.fct_retail_sales 
    ADD CONSTRAINT fct_sales_customer_fk 
    FOREIGN KEY (customer_key) REFERENCES retail_dwh.dim_customer(customer_key);

ALTER TABLE retail_dwh.fct_retail_sales 
    ADD CONSTRAINT fct_sales_date_fk 
    FOREIGN KEY (date_key) REFERENCES retail_dwh.dim_date(date_key);

CREATE INDEX IF NOT EXISTS idx_fct_sales_product 
    ON retail_dwh.fct_retail_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_customer 
    ON retail_dwh.fct_retail_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_date 
    ON retail_dwh.fct_retail_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_invoice 
    ON retail_dwh.fct_retail_sales(invoice_no);
CREATE INDEX IF NOT EXISTS idx_fct_sales_valid 
    ON retail_dwh.fct_retail_sales(is_valid_sale);
CREATE INDEX IF NOT EXISTS idx_fct_sales_pk_id 
    ON retail_dwh.fct_retail_sales(sales_pk_id);

INSERT INTO retail_dwh.fct_retail_sales
SELECT * FROM retail_dwh.fct_retail_sales_old;

DO $$
DECLARE
    old_count INTEGER;
    new_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO old_count FROM retail_dwh.fct_retail_sales_old;
    SELECT COUNT(*) INTO new_count FROM retail_dwh.fct_retail_sales;
    
    RAISE NOTICE 'Old table rows: %', old_count;
    RAISE NOTICE 'New table rows: %', new_count;
    
    IF old_count = new_count THEN
        RAISE NOTICE ' Data migration successful!';
    ELSE
        RAISE WARNING ' Row count mismatch! Review before dropping old table.';
    END IF;
END $$;


-- DROP TABLE retail_dwh.fct_retail_sales_old;

COMMIT;

SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'retail_dwh' 
  AND tablename LIKE 'fct_retail_sales%'
ORDER BY tablename;

