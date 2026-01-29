import pandas as pd
import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime

log_format = '%(asctime)s | %(levelname)-8s | %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format, datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

def read_csv_file(file_path: str, encoding: str = 'ISO-8859-1') -> Optional[pd.DataFrame]:
    csv_file = Path(file_path)
    
    if not csv_file.exists():
        log.error(f"CSV file not found: {file_path}")
        return None
    
    try:
        log.info(f"Reading CSV: {file_path}")
        
        dataframe = pd.read_csv(csv_file, encoding=encoding)
        
        rows, cols = dataframe.shape
        log.info(f"Loaded successfully. Rows: {rows:,} | Columns: {cols}")
        
        return dataframe
    
    except Exception as e:
        log.error(f"Failed to read CSV: {e}")
        return None

def prepare_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    log.info("Preparing DataFrame...")
    
    initial_row_count = len(dataframe)
    
    dataframe = dataframe.replace(r'^\s*$', None, regex=True)
    dataframe = dataframe.replace('', None)
    
    dataframe = dataframe.dropna(how='all')
    
    dataframe = dataframe.drop_duplicates()
    
    final_row_count = len(dataframe)
    removed = initial_row_count - final_row_count
    
    log.info(f"DataFrame prepared. Removed: {removed} rows | Remaining: {final_row_count:,} rows")
    
    return dataframe

def build_create_table_statement(
    dataframe: pd.DataFrame,
    schema: str,
    table: str,
    primary_key_column: Optional[str] = None
) -> str:
    log.info(f"Building CREATE TABLE for {schema}.{table}")
    
    postgres_types = {
        'int64': 'BIGINT',
        'float64': 'NUMERIC',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN',
        'object': 'TEXT'
    }
    
    column_definitions = []
    
    for col_name, col_type in dataframe.dtypes.items():
        if 'date' in col_name.lower() and 'id' not in col_name.lower():
            pg_type = 'TIMESTAMP'
        else:
            pg_type = postgres_types.get(str(col_type), 'TEXT')
        
        if primary_key_column and col_name == primary_key_column:
            column_definitions.append(f'    "{col_name}" {pg_type} PRIMARY KEY')
        else:
            column_definitions.append(f'    "{col_name}" {pg_type}')
    
    columns_sql = ',\n'.join(column_definitions)
    
    create_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
{columns_sql}
);"""
    
    log.info(f"CREATE TABLE statement generated")
    
    return create_sql

def build_insert_statements(
    dataframe: pd.DataFrame,
    schema: str,
    table: str,
    chunk_size: int = 500
) -> List[str]:
    log.info(f"Building INSERT statements for {len(dataframe):,} rows")
    
    columns = [f'"{col}"' for col in dataframe.columns]
    columns_clause = ', '.join(columns)
    
    insert_statements = []
    
    total_chunks = (len(dataframe) + chunk_size - 1) // chunk_size
    
    for chunk_num in range(total_chunks):
        start_idx = chunk_num * chunk_size
        end_idx = start_idx + chunk_size
        chunk_df = dataframe.iloc[start_idx:end_idx]
        
        value_rows = []
        
        for _, row in chunk_df.iterrows():
            formatted_values = []
            
            for value in row:
                if pd.isna(value):
                    formatted_values.append("NULL")
                elif isinstance(value, str):
                    safe_value = value.replace("'", "''")
                    formatted_values.append(f"'{safe_value}'")
                elif isinstance(value, (pd.Timestamp, datetime)):
                    formatted_values.append(f"'{value}'")
                else:
                    formatted_values.append(f"'{value}'")
            
            value_rows.append(f"    ({', '.join(formatted_values)})")
        
        values_clause = ',\n'.join(value_rows)
        
        insert_sql = f"""INSERT INTO {schema}.{table} ({columns_clause})
VALUES
{values_clause};"""
        
        insert_statements.append(insert_sql)
    
    log.info(f"Generated {len(insert_statements)} INSERT statements")
    
    return insert_statements

def build_upsert_statements(
    dataframe: pd.DataFrame,
    schema: str,
    table: str,
    conflict_columns: List[str],
    chunk_size: int = 500
) -> List[str]:
    log.info(f"Building UPSERT statements for {len(dataframe):,} rows")
    
    columns = [f'"{col}"' for col in dataframe.columns]
    columns_clause = ', '.join(columns)
    
    conflict_clause = ', '.join([f'"{col}"' for col in conflict_columns])
    
    update_columns = [col for col in dataframe.columns if col not in conflict_columns]
    update_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
    
    upsert_statements = []
    
    total_chunks = (len(dataframe) + chunk_size - 1) // chunk_size
    
    for chunk_num in range(total_chunks):
        start_idx = chunk_num * chunk_size
        end_idx = start_idx + chunk_size
        chunk_df = dataframe.iloc[start_idx:end_idx]
        
        value_rows = []
        
        for _, row in chunk_df.iterrows():
            formatted_values = []
            
            for value in row:
                if pd.isna(value):
                    formatted_values.append("NULL")
                elif isinstance(value, str):
                    safe_value = value.replace("'", "''")
                    formatted_values.append(f"'{safe_value}'")
                elif isinstance(value, (pd.Timestamp, datetime)):
                    formatted_values.append(f"'{value}'")
                else:
                    formatted_values.append(f"'{value}'")
            
            value_rows.append(f"    ({', '.join(formatted_values)})")
        
        values_clause = ',\n'.join(value_rows)
        
        upsert_sql = f"""INSERT INTO {schema}.{table} ({columns_clause})
VALUES
{values_clause}
ON CONFLICT ({conflict_clause})
DO UPDATE SET
    {update_clause};"""
        
        upsert_statements.append(upsert_sql)
    
    log.info(f"Generated {len(upsert_statements)} UPSERT statements")
    
    return upsert_statements

def add_tracking_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    current_time = datetime.utcnow()
    batch_id = current_time.strftime('%Y%m%d_%H%M%S')
    
    dataframe['loaded_at'] = current_time
    dataframe['batch_id'] = batch_id
    
    log.info(f"Added tracking columns | Batch ID: {batch_id}")
    
    return dataframe

if __name__ == "__main__":
    print("\n" + "="*60)
    print("Testing Data Processor")
    print("="*60 + "\n")
    
    sample_data = {
        'InvoiceNo': ['INV001', 'INV002', 'INV003'],
        'ProductCode': ['A123', 'B456', 'C789'],
        'Quantity': [10, 5, 8],
        'Price': [15.99, 25.50, 12.75],
        'OrderDate': ['2024-01-15', '2024-01-16', '2024-01-17']
    }
    
    df = pd.DataFrame(sample_data)
    
    print("Sample DataFrame:")
    print(df)
    print()
    
    create_sql = build_create_table_statement(df, 'staging', 'orders', 'InvoiceNo')
    print("Generated CREATE TABLE:")
    print(create_sql)
    print()
    
    insert_sqls = build_insert_statements(df, 'staging', 'orders', chunk_size=2)
    print(f"Generated {len(insert_sqls)} INSERT statements")
    print("First INSERT:")
    print(insert_sqls[0])