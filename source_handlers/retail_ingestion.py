import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).parent.parent))

from handlers.db_manager import create_database_connection, run_sql_query, close_db_connection
from handlers.data_processor import read_csv_file, prepare_dataframe, add_tracking_columns

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

def infer_sql_type(dtype, column_name=None):
    dtype_str = str(dtype)
    
    if 'int' in dtype_str:
        return 'INTEGER'
    elif 'float' in dtype_str:
        return 'NUMERIC(10, 2)'
    elif 'datetime' in dtype_str:
        return 'TIMESTAMP'
    elif 'object' in dtype_str:
        if column_name and 'description' in column_name.lower():
            return 'TEXT'
        else:
            return 'VARCHAR(255)'
    else:
        return 'TEXT'

def generate_create_table_sql(df: pd.DataFrame, schema: str, table: str):
    columns_sql = []
    
    for col in df.columns:
        sql_col_name = col.lower().replace(' ', '_').replace('-', '_')
        sql_type = infer_sql_type(df[col].dtype, col)
        columns_sql.append(f"{sql_col_name} {sql_type}")
    
    columns_sql.append("loaded_at TIMESTAMP")
    columns_sql.append("batch_id VARCHAR(50)")
    
    columns_clause = ',\n        '.join(columns_sql)
    
    create_table_sql = f"""
    DROP TABLE IF EXISTS {schema}.{table} CASCADE;
    
    CREATE TABLE {schema}.{table} (
        {columns_clause}
    );
    """
    
    return create_table_sql

def create_raw_schema(connection):
    create_schema_sql = """
    CREATE SCHEMA IF NOT EXISTS raw_schema;
    """
    
    log.info("Creating RAW schema...")
    success = run_sql_query(connection, create_schema_sql)
    
    if success:
        log.info("RAW schema created successfully")
    return success

def create_raw_table_dynamic(connection, df: pd.DataFrame, schema='raw_schema', table='online_retail'):
    log.info(f"Generating CREATE TABLE SQL for {schema}.{table}...")
    
    create_table_sql = generate_create_table_sql(df, schema, table)
    
    log.info(f"Generated SQL:\n{create_table_sql}")
    
    log.info(f"Creating RAW table: {schema}.{table}")
    success = run_sql_query(connection, create_table_sql)
    
    if success:
        log.info("RAW table created successfully")
    return success

def load_csv_to_raw(csv_path: str, connection, schema='raw_schema', table='online_retail'):
    log.info(f"Starting ingestion from: {csv_path}")
    
    df = read_csv_file(csv_path, encoding='ISO-8859-1')
    
    if df is None:
        log.error("Failed to read CSV file")
        return False
    
    log.info(f"Original data: {len(df):,} rows, {len(df.columns)} columns")
    log.info(f"Columns detected: {df.columns.tolist()}")
    
    df = prepare_dataframe(df)
    
    df = add_tracking_columns(df)
    
    create_raw_table_dynamic(connection, df.drop(columns=['loaded_at', 'batch_id']), schema, table)
    
    log.info(f"Inserting data into {schema}.{table}...")
    
    chunk_size = 5000
    total_chunks = (len(df) + chunk_size - 1) // chunk_size
    
    for i in range(total_chunks):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, len(df))
        chunk_df = df.iloc[start_idx:end_idx]
        
        column_names = [col.lower().replace(' ', '_').replace('-', '_') for col in chunk_df.columns]
        columns_clause = ', '.join(column_names)
        
        values_list = []
        for _, row in chunk_df.iterrows():
            values = []
            for val in row:
                if pd.isna(val):
                    values.append("NULL")
                elif isinstance(val, str):
                    clean_val = val.replace("'", "''")
                    values.append(f"'{clean_val}'")
                elif isinstance(val, (pd.Timestamp, datetime)):
                    values.append(f"'{val}'")
                else:
                    values.append(f"'{val}'")
            
            values_list.append(f"({', '.join(values)})")
        
        values_clause = ',\n'.join(values_list)
        
        insert_sql = f"""
        INSERT INTO {schema}.{table} 
        ({columns_clause})
        VALUES
        {values_clause};
        """
        
        success = run_sql_query(connection, insert_sql)
        
        if not success:
            log.error(f"Failed to insert chunk {i+1}/{total_chunks}")
            return False
        
        log.info(f"Inserted chunk {i+1}/{total_chunks} ({end_idx:,}/{len(df):,} rows)")
    
    log.info(f"Successfully loaded {len(df):,} rows to {schema}.{table}")
    return True

def run_full_ingestion(csv_path: str = 'data/raw/online_retail.csv'):
    log.info("="*60)
    log.info("STARTING FULL DATA INGESTION")
    log.info("="*60)
    
    connection = None
    
    try:
        connection = create_database_connection()
        
        create_raw_schema(connection)
        
        success = load_csv_to_raw(csv_path, connection)
        
        if success:
            log.info("="*60)
            log.info("INGESTION COMPLETED SUCCESSFULLY")
            log.info("="*60)
        else:
            log.error("INGESTION FAILED")
        
        return success
    
    except Exception as e:
        log.error(f"Ingestion failed with error: {e}")
        import traceback
        log.error(traceback.format_exc())
        return False
    
    finally:
        if connection:
            close_db_connection(connection)

if __name__ == "__main__":
    run_full_ingestion()