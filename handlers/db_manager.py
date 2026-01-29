import psycopg2
import yaml
import logging
from pathlib import Path
from typing import Optional, Any

log_format = '%(asctime)s | %(levelname)-8s | %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format, datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

class DatabaseConfig:
    
    @staticmethod
    def load_from_yaml(config_path: str = 'config/config.yaml') -> dict:
        try:
            config_file = Path(config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")
            
            with open(config_file, 'r') as file:
                config = yaml.safe_load(file)
                db_config = config['database']
            
            secrets_path = Path('config/secrets.yaml')
            if secrets_path.exists():
                with open(secrets_path, 'r') as file:
                    secrets = yaml.safe_load(file)
                    db_config.update(secrets['database'])
            
            return db_config
        
        except Exception as e:
            log.error(f"Failed to load config: {e}")
            raise

        
def create_database_connection():
    db_config = DatabaseConfig.load_from_yaml()
    
    try:
        connection = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            dbname=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        
        log.info("Database connection established successfully")
        return connection
    
    except psycopg2.OperationalError as e:
        log.error(f"Connection failed: {e}")
        raise

def run_sql_query(connection, sql_statement: str, parameters: tuple = None) -> bool:
    cursor = None
    
    try:
        cursor = connection.cursor()
        
        if parameters:
            cursor.execute(sql_statement, parameters)
        else:
            cursor.execute(sql_statement)
        
        connection.commit()
        
        affected_rows = cursor.rowcount
        log.info(f"Query executed | Rows affected: {affected_rows}")
        
        return True
    
    except psycopg2.Error as error:
        log.error(f"Query failed: {error}")
        log.debug(f"Failed SQL (first 150 chars): {sql_statement[:150]}...")
        connection.rollback()
        return False
    
    finally:
        if cursor:
            cursor.close()

def fetch_query_results(connection, sql_query: str, parameters: tuple = None) -> Optional[list]:
    cursor = None
    
    try:
        cursor = connection.cursor()
        
        if parameters:
            cursor.execute(sql_query, parameters)
        else:
            cursor.execute(sql_query)
        
        results = cursor.fetchall()
        log.info(f"Query executed | Rows returned: {len(results)}")
        
        return results
    
    except psycopg2.Error as error:
        log.error(f"Query failed: {error}")
        return None
    
    finally:
        if cursor:
            cursor.close()

def execute_sql_from_file(connection, file_path: str) -> bool:
    sql_file = Path(file_path)
    
    if not sql_file.exists():
        log.error(f"SQL file not found: {file_path}")
        return False
    
    try:
        log.info(f"Executing SQL file: {file_path}")
        
        with open(sql_file, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        return run_sql_query(connection, sql_content)
    
    except Exception as e:
        log.error(f"Failed to execute SQL file: {e}")
        return False

def close_db_connection(connection) -> None:
    if connection and not connection.closed:
        connection.close()
        log.info("Database connection closed")

def verify_connection() -> bool:
    try:
        conn = create_database_connection()
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()[0]
        
        log.info(f"Database verified: {db_version.split(',')[0]}")
        
        cursor.close()
        close_db_connection(conn)
        
        return True
    
    except Exception as e:
        log.error(f"Connection verification failed: {e}")
        return False

if __name__ == "__main__":
    print("\n" + "="*60)
    print("Testing Database Manager")
    print("="*60 + "\n")
    
    verify_connection()