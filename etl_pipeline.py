import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path
from datetime import datetime, timezone
import sys
import argparse

sys.path.append(str(Path(__file__).parent))

from handlers.db_manager import create_database_connection, run_sql_query, close_db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

class DataQualityTracker:
    
    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        self.metrics = []
        self.quarantine_records = []
    
    def add_metric(self, rule_name: str, category: str, rows_processed: int, 
                   rows_passed: int, rows_quarantined: int, rows_dropped: int, notes: str = ''):
        self.metrics.append({
            'batch_id': self.batch_id,
            'rule_name': rule_name,
            'rule_category': category,
            'rows_processed': rows_processed,
            'rows_passed': rows_passed,
            'rows_quarantined': rows_quarantined,
            'rows_dropped': rows_dropped,
            'execution_timestamp': datetime.now(timezone.utc),
            'notes': notes
        })
    
    def quarantine_rows(self, df_quarantine: pd.DataFrame, rule_name: str, reason: str):
        
        for _, row in df_quarantine.iterrows():
            
            row_dict = row.to_dict()
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
            
            self.quarantine_records.append({
                'batch_id': self.batch_id,
                'rule_name': rule_name,
                'dq_reason': reason,
                'quarantined_at': datetime.now(timezone.utc),
                'original_invoice_no': row.get('InvoiceNo'),
                'original_stock_code': row.get('StockCode'),
                'original_description': row.get('Description'),
                'original_quantity': row.get('Quantity'),
                'original_invoice_date': str(row.get('InvoiceDate')),
                'original_unit_price': row.get('UnitPrice'),
                'original_customer_id': row.get('CustomerID'),
                'original_country': row.get('Country'),
                'raw_row_json': json.dumps(row_dict, default=str)
            })
    
    def get_quarantine_df(self) -> pd.DataFrame:
        if self.quarantine_records:
            return pd.DataFrame(self.quarantine_records)
        return pd.DataFrame()
    
    def get_metrics_df(self) -> pd.DataFrame:
        if self.metrics:
            return pd.DataFrame(self.metrics)
        return pd.DataFrame()

def create_retail_dwh_schema(connection):
    
    
    log.info("Setting up database schemas...")
    
    setup_sql = """
    -- Drop old schemas if they exist
    DROP SCHEMA IF EXISTS raw_schema CASCADE;
    DROP SCHEMA IF EXISTS etl_schema CASCADE;
    DROP SCHEMA IF EXISTS staging_schema CASCADE;
    
    -- Create our single schema
    CREATE SCHEMA IF NOT EXISTS retail_dwh;
    
    COMMENT ON SCHEMA retail_dwh IS 
    'Retail sales data warehouse. 
    Prefixes: stg_ (staging), dq_ (data quality), meta_ (metadata), dim_ (dimensions), fct_ (facts)';
    """
    
    success = run_sql_query(connection, setup_sql)
    if success:
        log.info(" Schema retail_dwh created successfully")
    return success

def create_staging_table(connection):
    
    
    log.info("Creating staging table: stg_retail_sales")
    
    create_sql = """
    DROP TABLE IF EXISTS retail_dwh.stg_retail_sales CASCADE;
    
    CREATE TABLE retail_dwh.stg_retail_sales (
       
        invoice_no VARCHAR(50) NOT NULL,
        stock_code VARCHAR(50) NOT NULL,
        description TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        invoice_date TIMESTAMP NOT NULL,
        unit_price NUMERIC(10, 2) NOT NULL,
        customer_id BIGINT,
        country VARCHAR(100) NOT NULL,
        
        line_total NUMERIC(12, 2) NOT NULL,
        
        is_cancellation BOOLEAN DEFAULT FALSE,
        is_adjustment BOOLEAN DEFAULT FALSE,
        is_guest_purchase BOOLEAN DEFAULT FALSE,
        is_valid_sale BOOLEAN DEFAULT FALSE,
        is_return BOOLEAN DEFAULT FALSE,
        
        loaded_at TIMESTAMP NOT NULL,
        batch_id VARCHAR(50) NOT NULL,
        source_file VARCHAR(255)
    );
    
    -- Indexes for performance
    CREATE INDEX idx_stg_retail_invoice_date ON retail_dwh.stg_retail_sales(invoice_date);
    CREATE INDEX idx_stg_retail_invoice_no ON retail_dwh.stg_retail_sales(invoice_no);
    CREATE INDEX idx_stg_retail_stock_code ON retail_dwh.stg_retail_sales(stock_code);
    CREATE INDEX idx_stg_retail_customer_id ON retail_dwh.stg_retail_sales(customer_id) 
        WHERE customer_id IS NOT NULL;
    CREATE INDEX idx_stg_retail_batch_id ON retail_dwh.stg_retail_sales(batch_id);
    
    COMMENT ON TABLE retail_dwh.stg_retail_sales IS 
    'Staging table: cleaned, typed retail sales with row-level flags. 
    Core transformation done in Python ETL before loading.';
    """
    
    success = run_sql_query(connection, create_sql)
    if success:
        log.info(" Staging table created")
    return success

def create_dq_quarantine_table(connection):
    
    
    log.info("Creating DQ table: dq_quarantine_sales")
    
    create_sql = """
    DROP TABLE IF EXISTS retail_dwh.dq_quarantine_sales CASCADE;
    
    CREATE TABLE retail_dwh.dq_quarantine_sales (
        quarantine_id SERIAL PRIMARY KEY,
        batch_id VARCHAR(50) NOT NULL,
        rule_name VARCHAR(100) NOT NULL,
        dq_reason TEXT NOT NULL,
        quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        -- Original row data
        original_invoice_no VARCHAR(50),
        original_stock_code VARCHAR(50),
        original_description TEXT,
        original_quantity INTEGER,
        original_invoice_date VARCHAR(255),
        original_unit_price NUMERIC(10, 2),
        original_customer_id NUMERIC(10, 2),
        original_country VARCHAR(100),
        raw_row_json JSONB
    );
    
    CREATE INDEX idx_dq_quarantine_batch ON retail_dwh.dq_quarantine_sales(batch_id);
    CREATE INDEX idx_dq_quarantine_rule ON retail_dwh.dq_quarantine_sales(rule_name);
    CREATE INDEX idx_dq_quarantine_date ON retail_dwh.dq_quarantine_sales(quarantined_at);
    
    COMMENT ON TABLE retail_dwh.dq_quarantine_sales IS 
    'Data quality quarantine: rejected rows with audit trail for investigation.';
    """
    
    success = run_sql_query(connection, create_sql)
    if success:
        log.info(" Quarantine table created")
    return success

def create_dq_metrics_table(connection):
    
    
    log.info("Creating DQ table: dq_metrics")
    
    create_sql = """
    DROP TABLE IF EXISTS retail_dwh.dq_metrics CASCADE;
    
    CREATE TABLE retail_dwh.dq_metrics (
        metric_id SERIAL PRIMARY KEY,
        batch_id VARCHAR(50) NOT NULL,
        rule_name VARCHAR(100) NOT NULL,
        rule_category VARCHAR(50),
        rows_processed INTEGER NOT NULL,
        rows_passed INTEGER NOT NULL,
        rows_quarantined INTEGER NOT NULL,
        rows_dropped INTEGER NOT NULL,
        execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        notes TEXT
    );
    
    CREATE INDEX idx_dq_metrics_batch ON retail_dwh.dq_metrics(batch_id);
    CREATE INDEX idx_dq_metrics_rule ON retail_dwh.dq_metrics(rule_name);
    CREATE INDEX idx_dq_metrics_timestamp ON retail_dwh.dq_metrics(execution_timestamp);
    
    COMMENT ON TABLE retail_dwh.dq_metrics IS 
    'Data quality monitoring: tracks rule execution and pass/fail rates per batch.';
    """
    
    success = run_sql_query(connection, create_sql)
    if success:
        log.info("Metrics table created")
    return success

def create_meta_batch_log_table(connection):
    
    
    log.info("Creating metadata table: meta_etl_batch_log")
    
    create_sql = """
    DROP TABLE IF EXISTS retail_dwh.meta_etl_batch_log CASCADE;
    
    CREATE TABLE retail_dwh.meta_etl_batch_log (
        batch_id VARCHAR(50) PRIMARY KEY,
        batch_start TIMESTAMP NOT NULL,
        batch_end TIMESTAMP,
        status VARCHAR(20) NOT NULL,
        rows_extracted INTEGER,
        rows_loaded_staging INTEGER,
        rows_quarantined INTEGER,
        error_message TEXT,
        execution_duration_seconds INTEGER
    );
    
    CREATE INDEX idx_meta_batch_start ON retail_dwh.meta_etl_batch_log(batch_start);
    CREATE INDEX idx_meta_batch_status ON retail_dwh.meta_etl_batch_log(status);
    
    COMMENT ON TABLE retail_dwh.meta_etl_batch_log IS 
    'ETL batch execution log: tracks pipeline runs for monitoring and audit.';
    """
    
    success = run_sql_query(connection, create_sql)
    if success:
        log.info(" Batch log table created")
    return success

def extract_data(csv_path: str = 'data/raw/online_retail.csv') -> pd.DataFrame:
    """Extract data from CSV"""
    log.info("="*70)
    log.info("STEP 1: EXTRACT")
    log.info("="*70)
    
    csv_file = Path(csv_path)
    if not csv_file.exists():
        log.error(f"CSV file not found: {csv_path}")
        return None
    
    try:
        log.info(f"Reading CSV: {csv_path}")
        df = pd.read_csv(csv_file, encoding='ISO-8859-1')
        log.info(f" Extracted {len(df):,} rows × {len(df.columns)} columns")
        return df
    except Exception as e:
        log.error(f" Extraction failed: {e}")
        return None

def transform_data(df: pd.DataFrame, dq_tracker: DataQualityTracker) -> pd.DataFrame:
    """Complete transformation with all business rules"""
    
    log.info("\n" + "="*70)
    log.info("STEP 2: TRANSFORM")
    log.info("="*70)
    
    initial_count = len(df)
    log.info(f"Initial rows: {initial_count:,}\n")
    

    log.info("--- 2.1: Type Casting ---")
    
    df['invoice_date'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
    log.info(" Parsed invoice_date to TIMESTAMP")
    
    before = len(df)
    df = df[df['invoice_date'].notna()]
    removed = before - len(df)
    if removed > 0:
        log.info(f" Removed unparseable dates: {removed:,}")
        dq_tracker.add_metric('remove_unparseable_dates', 'data_integrity', before, len(df), 0, removed,
                            'DQ004: Unparseable InvoiceDate')
    
    df['quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
    df['unit_price'] = pd.to_numeric(df['UnitPrice'], errors='coerce').fillna(0.0)
    df['customer_id'] = pd.to_numeric(df['CustomerID'], errors='coerce').astype('Int64')
    log.info(" Converted numeric columns")
    
    
    log.info("\n--- 2.2: Calculations ---")
    
    df['line_total'] = df['quantity'] * df['unit_price']
    log.info(" Calculated line_total = quantity × unit_price")
    
    
    log.info("\n--- 2.3: Anomaly Detection (Quarantine) ---")
    
    
    suspicious_price_mask = (df['unit_price'] > 10000) & (df['quantity'].abs() < 100)
    if suspicious_price_mask.sum() > 0:
        df_quar = df[suspicious_price_mask].copy()
        dq_tracker.quarantine_rows(df_quar, 'quarantine_suspicious_unit_price',
                                   'AN001: Unit price >$10K with low quantity')
        df = df[~suspicious_price_mask]
        log.info(f" Quarantined {suspicious_price_mask.sum():,} rows (suspicious unit price)")
        dq_tracker.add_metric('quarantine_suspicious_unit_price', 'anomaly_detection',
                            len(df) + suspicious_price_mask.sum(), len(df), suspicious_price_mask.sum(), 0,
                            'AN001: Potential pricing errors')
    
    # AN002: Zero quantity
    zero_qty_mask = df['quantity'] == 0
    if zero_qty_mask.sum() > 0:
        df_quar = df[zero_qty_mask].copy()
        dq_tracker.quarantine_rows(df_quar, 'quarantine_zero_quantity', 'AN002: Quantity = 0')
        df = df[~zero_qty_mask]
        log.info(f" Quarantined {zero_qty_mask.sum():,} rows (zero quantity)")
        dq_tracker.add_metric('quarantine_zero_quantity', 'anomaly_detection',
                            len(df) + zero_qty_mask.sum(), len(df), zero_qty_mask.sum(), 0,
                            'AN002: Invalid zero quantity')
    
    # AN003: Negative unit price
    neg_price_mask = df['unit_price'] < 0
    if neg_price_mask.sum() > 0:
        df_quar = df[neg_price_mask].copy()
        dq_tracker.quarantine_rows(df_quar, 'quarantine_negative_unit_price', 'AN003: Negative unit price')
        df = df[~neg_price_mask]
        log.info(f"Quarantined {neg_price_mask.sum():,} rows (negative unit price)")
        dq_tracker.add_metric('quarantine_negative_unit_price', 'anomaly_detection',
                            len(df) + neg_price_mask.sum(), len(df), neg_price_mask.sum(), 0,
                            'AN003: Invalid negative price')
    
   
    log.info("\n--- 2.4: Data Cleaning ---")
    
    before = len(df)
    df = df.dropna(how='all')
    removed = before - len(df)
    if removed > 0:
        log.info(f" Removed all-null rows: {removed:,}")
        dq_tracker.add_metric('remove_all_null_rows', 'data_integrity', before, len(df), 0, removed,
                            'DQ001: Completely empty rows')
    
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed > 0:
        log.info(f"Removed duplicates: {removed:,}")
        dq_tracker.add_metric('remove_exact_duplicates', 'data_integrity', before, len(df), 0, removed,
                            'DQ002: Exact duplicate rows')
    
    before = len(df)
    critical_fields = ['InvoiceNo', 'StockCode', 'invoice_date', 'quantity', 'unit_price']
    df = df.dropna(subset=critical_fields)
    removed = before - len(df)
    if removed > 0:
        log.info(f" Removed missing critical fields: {removed:,}")
        log.info(f"   Note: customer_id is NOT critical (NULL = guest)")
        dq_tracker.add_metric('remove_missing_critical', 'data_integrity', before, len(df), 0, removed,
                            'DQ003: Missing InvoiceNo/StockCode/Date/Qty/Price')
    
    
    log.info("\n--- 2.5: Standardization ---")
    
    df['invoice_no'] = df['InvoiceNo'].astype(str).str.strip().str.upper()
    df['stock_code'] = df['StockCode'].astype(str).str.strip().str.upper()
    df['description'] = df['Description'].fillna('UNKNOWN PRODUCT').str.strip()
    df['country'] = df['Country'].fillna('UNKNOWN').str.strip().str.title()
    log.info(" Standardized text fields (TRIM, UPPER, Title Case)")
    
    
    log.info("\n--- 2.6: Business Logic Flags ---")
    
    df['is_cancellation'] = df['invoice_no'].str.startswith('C', na=False)
    log.info(f" Flagged cancellations: {df['is_cancellation'].sum():,}")
    
    df['is_adjustment'] = (
        (df['quantity'] < 0) & 
        (~df['is_cancellation']) & 
        (df['unit_price'] == 0) & 
        (df['customer_id'].isna())
    )
    log.info(f" Flagged adjustments: {df['is_adjustment'].sum():,}")
    
    df['is_guest_purchase'] = df['customer_id'].isna()
    log.info(f" Flagged guest purchases: {df['is_guest_purchase'].sum():,}")
    
    df['is_valid_sale'] = (
        (df['quantity'] > 0) & 
        (df['unit_price'] > 0) & 
        (~df['is_cancellation'])
    )
    log.info(f" Flagged valid sales: {df['is_valid_sale'].sum():,}")
    
    df['is_return'] = (df['quantity'] < 0) & (~df['is_adjustment'])
    log.info(f" Flagged returns: {df['is_return'].sum():,}")
    
   
    log.info("\n--- 2.7: ETL Metadata ---")
    
    current_time = datetime.now(timezone.utc)
    df['loaded_at'] = current_time
    df['batch_id'] = dq_tracker.batch_id
    df['source_file'] = 'online_retail.csv'
    log.info(f" Added ETL metadata (batch: {dq_tracker.batch_id})")
    
    
    final_count = len(df)
    log.info(f"\n{'='*70}")
    log.info(" TRANSFORMATION COMPLETE")
    log.info(f"{'='*70}")
    log.info(f"   Initial rows:      {initial_count:,}")
    log.info(f"   Final rows:        {final_count:,}")
    log.info(f"   Rows removed:      {initial_count - final_count:,}")
    log.info(f"   Quarantined:       {len(dq_tracker.get_quarantine_df()):,}")
    log.info(f"   Retention rate:    {(final_count/initial_count*100):.2f}%")
    
    return df

def load_to_staging(df: pd.DataFrame, connection) -> bool:
    """Load cleaned data to stg_retail_sales"""
    
    log.info("\n" + "="*70)
    log.info("STEP 3: LOAD TO STAGING")
    log.info("="*70)
    
    
    column_mapping = {
        'invoice_no': 'invoice_no',
        'stock_code': 'stock_code',
        'description': 'description',
        'quantity': 'quantity',
        'invoice_date': 'invoice_date',
        'unit_price': 'unit_price',
        'customer_id': 'customer_id',
        'country': 'country',
        'line_total': 'line_total',
        'is_cancellation': 'is_cancellation',
        'is_adjustment': 'is_adjustment',
        'is_guest_purchase': 'is_guest_purchase',
        'is_valid_sale': 'is_valid_sale',
        'is_return': 'is_return',
        'loaded_at': 'loaded_at',
        'batch_id': 'batch_id',
        'source_file': 'source_file'
    }
    
    df_load = df[column_mapping.keys()]
    
    log.info(f"Loading {len(df_load):,} rows to stg_retail_sales...")
    
    chunk_size = 5000
    total_chunks = (len(df_load) + chunk_size - 1) // chunk_size
    
    for i in range(total_chunks):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, len(df_load))
        chunk_df = df_load.iloc[start_idx:end_idx]
        
        column_names = list(chunk_df.columns)
        columns_clause = ', '.join(column_names)
        
        values_list = []
        for _, row in chunk_df.iterrows():
            values = []
            for val in row:
                if pd.isna(val):
                    values.append("NULL")
                elif isinstance(val, (bool, np.bool_)):
                    values.append("TRUE" if val else "FALSE")
                elif isinstance(val, str):
                    clean_val = val.replace("'", "''")
                    values.append(f"'{clean_val}'")
                elif isinstance(val, (pd.Timestamp, datetime)):
                    values.append(f"'{val}'")
                else:
                    values.append(f"{val}")
            
            values_list.append(f"({', '.join(values)})")
        
        values_clause = ',\n'.join(values_list)
        
        insert_sql = f"""
INSERT INTO retail_dwh.stg_retail_sales ({columns_clause})
VALUES
{values_clause};
"""
        
        success = run_sql_query(connection, insert_sql)
        if not success:
            log.error(f" Failed to load chunk {i+1}/{total_chunks}")
            return False
        
        log.info(f" Loaded chunk {i+1}/{total_chunks} ({end_idx:,}/{len(df_load):,} rows)")
    
    log.info(f" Successfully loaded {len(df_load):,} rows to stg_retail_sales")
    return True

def load_quarantine(dq_tracker: DataQualityTracker, connection) -> bool:
    """Load quarantined rows to dq_quarantine_sales"""
    
    log.info("\n--- Loading Quarantine Data ---")
    
    df_quar = dq_tracker.get_quarantine_df()
    
    if df_quar.empty:
        log.info(" No rows quarantined")
        return True
    
    log.info(f"Loading {len(df_quar):,} quarantined rows...")
    
    for _, row in df_quar.iterrows():
        insert_sql = f"""
INSERT INTO retail_dwh.dq_quarantine_sales (
    batch_id, rule_name, dq_reason, quarantined_at,
    original_invoice_no, original_stock_code, original_description,
    original_quantity, original_invoice_date, original_unit_price,
    original_customer_id, original_country, raw_row_json
) VALUES (
    '{row['batch_id']}',
    '{row['rule_name']}',
    '{row['dq_reason'].replace("'", "''")}',
    '{row['quarantined_at']}',
    {f"'{row['original_invoice_no']}'" if pd.notna(row['original_invoice_no']) else 'NULL'},
    {f"'{row['original_stock_code']}'" if pd.notna(row['original_stock_code']) else 'NULL'},
    {f"'{str(row['original_description']).replace("'", "''")}'" if pd.notna(row['original_description']) else 'NULL'},
    {row['original_quantity'] if pd.notna(row['original_quantity']) else 'NULL'},
    {f"'{row['original_invoice_date']}'" if pd.notna(row['original_invoice_date']) else 'NULL'},
    {row['original_unit_price'] if pd.notna(row['original_unit_price']) else 'NULL'},
    {row['original_customer_id'] if pd.notna(row['original_customer_id']) else 'NULL'},
    {f"'{row['original_country']}'" if pd.notna(row['original_country']) else 'NULL'},
    '{row['raw_row_json'].replace("'", "''")}'::jsonb
);
"""
        
        success = run_sql_query(connection, insert_sql)
        if not success:
            log.error(" Failed to load quarantine data")
            return False
    
    log.info(f" Loaded {len(df_quar):,} rows to dq_quarantine_sales")
    return True

def load_dq_metrics(dq_tracker: DataQualityTracker, connection) -> bool:
    """Load data quality metrics to dq_metrics"""
    
    log.info("\n--- Loading DQ Metrics ---")
    
    df_metrics = dq_tracker.get_metrics_df()
    
    if df_metrics.empty:
        log.info(" No metrics to load")
        return True
    
    log.info(f"Loading {len(df_metrics):,} DQ metrics...")
    
    for _, row in df_metrics.iterrows():
        insert_sql = f"""
INSERT INTO retail_dwh.dq_metrics (
    batch_id, rule_name, rule_category, rows_processed, rows_passed,
    rows_quarantined, rows_dropped, execution_timestamp, notes
) VALUES (
    '{row['batch_id']}',
    '{row['rule_name']}',
    '{row['rule_category']}',
    {row['rows_processed']},
    {row['rows_passed']},
    {row['rows_quarantined']},
    {row['rows_dropped']},
    '{row['execution_timestamp']}',
    '{row['notes'].replace("'", "''")}'
);
"""
        
        success = run_sql_query(connection, insert_sql)
        if not success:
            log.error(" Failed to load DQ metrics")
            return False
    
    log.info(f" Loaded {len(df_metrics):,} DQ metrics")
    return True

def log_batch_execution(batch_id: str, batch_start: datetime, status: str, 
                       rows_extracted: int, rows_loaded: int, rows_quarantined: int,
                       error_msg: str, connection) -> bool:
    """Log batch execution to meta_etl_batch_log"""
    
    batch_end = datetime.now(timezone.utc)
    duration = int((batch_end - batch_start).total_seconds())
    
    insert_sql = f"""
INSERT INTO retail_dwh.meta_etl_batch_log (
    batch_id, batch_start, batch_end, status,
    rows_extracted, rows_loaded_staging, rows_quarantined,
    error_message, execution_duration_seconds
) VALUES (
    '{batch_id}',
    '{batch_start}',
    '{batch_end}',
    '{status}',
    {rows_extracted},
    {rows_loaded},
    {rows_quarantined},
    {f"'{error_msg.replace("'", "''")}'" if error_msg else 'NULL'},
    {duration}
);
"""
    
    return run_sql_query(connection, insert_sql)

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Retail Sales ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python etl_pipeline.py --mode full          # Full refresh (default)
  python etl_pipeline.py --mode incremental   # Incremental load
        """
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['full', 'incremental'],
        default='full',
        help='Loading mode: full (reload all) or incremental (new records only)'
    )
    
    return parser.parse_args()

def get_last_successful_batch_timestamp(connection):
    
    
    query = """
    SELECT MAX(stg.invoice_date) as max_invoice_date
    FROM retail_dwh.stg_retail_sales stg
    INNER JOIN retail_dwh.meta_etl_batch_log log 
        ON stg.batch_id = log.batch_id
    WHERE log.status = 'SUCCESS';
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0]:
            log.info(f" Last batch max date: {result[0]}")
            return result[0]
        else:
            log.info(" No previous batches found (first run)")
            return None
            
    except Exception as e:
        log.warning(f"  Could not get last batch timestamp: {e}")
        return None

def extract_data_incremental(csv_path: str, last_batch_date) -> pd.DataFrame:
    """
    Extract only new data since last batch.
    Note: For static CSV, this returns 0 rows after first load.
    Included to demonstrate incremental pattern for production scenarios.
    """
    
    log.info("="*70)
    log.info("STEP 1: EXTRACT (INCREMENTAL MODE)")
    log.info("="*70)
    
    csv_file = Path(csv_path)
    if not csv_file.exists():
        log.error(f"CSV file not found: {csv_path}")
        return None
    
    try:
        log.info(f"Reading CSV: {csv_path}")
        df = pd.read_csv(csv_file, encoding='ISO-8859-1')
        log.info(f" Extracted {len(df):,} rows from source")
        
        if last_batch_date is None:
            log.info(" First run - loading all data")
            return df
        
        
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        df_new = df[df['InvoiceDate'] > last_batch_date].copy()
        
        log.info(f" Filtered to {len(df_new):,} new rows (InvoiceDate > {last_batch_date})")
        
        if len(df_new) == 0:
            log.warning("  No new records since last batch")
            log.info("   (Expected for static CSV - demonstrates incremental pattern)")
        
        return df_new
        
    except Exception as e:
        log.error(f" Extraction failed: {e}")
        return None

def run_etl_pipeline(csv_path: str = 'data/raw/online_retail.csv', mode: str = 'full'):
    """Run complete ETL pipeline"""
    
    batch_start = datetime.now(timezone.utc)
    batch_id = batch_start.strftime('%Y%m%d_%H%M%S')
    
    log.info("\n" + "="*70)
    log.info("RETAIL SALES ETL PIPELINE")
    log.info("="*70)
    log.info(f"Batch ID: {batch_id}")
    log.info(f"Mode: {mode.upper()}")
    log.info(f"Started: {batch_start}")
    log.info("="*70 + "\n")
    
    connection = None
    dq_tracker = DataQualityTracker(batch_id)
    
    try:
       
        connection = create_database_connection()
        
        
        create_retail_dwh_schema(connection)
        create_staging_table(connection)
        create_dq_quarantine_table(connection)
        create_dq_metrics_table(connection)
        create_meta_batch_log_table(connection)
        
        
        
                
        if mode == 'incremental':
            last_batch_date = get_last_successful_batch_timestamp(connection)
            df = extract_data_incremental(csv_path, last_batch_date)
        else:
            df = extract_data(csv_path)
        
        if df is None:
            raise Exception("Extraction failed")
        
        
        if len(df) == 0:
            log.warning("  No data to process (0 rows extracted)")
            log.info("   Logging empty batch and exiting gracefully...")
            
            log_batch_execution(batch_id, batch_start, 'SUCCESS_NO_DATA', 
                              0, 0, 0, 'No new data to process', connection)
            return True
        
        rows_extracted = len(df)
        
        
        df_clean = transform_data(df, dq_tracker)
        
        
        success_staging = load_to_staging(df_clean, connection)
        success_quarantine = load_quarantine(dq_tracker, connection)
        success_metrics = load_dq_metrics(dq_tracker, connection)
        
        if not (success_staging and success_quarantine and success_metrics):
            raise Exception("Loading failed")
        
        
        log_batch_execution(batch_id, batch_start, 'SUCCESS', 
                          rows_extracted, len(df_clean), len(dq_tracker.get_quarantine_df()),
                          None, connection)
        
        
        log.info("\n" + "="*70)
        log.info(" ETL PIPELINE COMPLETED SUCCESSFULLY")
        log.info("="*70)
        log.info(f"\n SUMMARY:")
        log.info(f"   Batch ID:           {batch_id}")
        log.info(f"   Mode:               {mode.upper()}")
        log.info(f"   Rows extracted:     {rows_extracted:,}")
        log.info(f"   Rows loaded:        {len(df_clean):,}")
        log.info(f"   Rows quarantined:   {len(dq_tracker.get_quarantine_df()):,}")
        log.info(f"   Valid sales:        {df_clean['is_valid_sale'].sum():,}")
        log.info(f"   Cancellations:      {df_clean['is_cancellation'].sum():,}")
        log.info(f"   Duration:           {int((datetime.now(timezone.utc) - batch_start).total_seconds())}s")
        
        total_revenue = df_clean[df_clean['is_valid_sale']]['line_total'].sum()
        log.info(f"   Total revenue:      ${total_revenue:,.2f}")
        
 
        return True
    
    except Exception as e:
        log.error(f"\n ETL PIPELINE FAILED: {e}")
        import traceback
        log.error(traceback.format_exc())
        
        if connection:
            log_batch_execution(batch_id, batch_start, 'FAILED', 
                              0, 0, 0, str(e), connection)
        
        return False
    
    finally:
        if connection:
            close_db_connection(connection)

if __name__ == "__main__":
    args = parse_arguments()
    run_etl_pipeline(mode=args.mode)