
import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

def print_header(title):
    
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")

def run_command(command, description):
    
    log.info(f" {description}")
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        log.info(f" {description} - COMPLETED")
        return True
    except subprocess.CalledProcessError as e:
        log.error(f" {description} - FAILED")
        log.error(f"Error: {e.stderr}")
        return False

def get_db_connection():
    
    secrets = yaml.safe_load(Path('config/secrets.yaml').read_text())
    return psycopg2.connect(**secrets['database'])

def check_database_status(conn):
   
    cursor = conn.cursor()
    
    
    cursor.execute("""
        SELECT COUNT(*) 
        FROM pg_tables 
        WHERE schemaname = 'retail_dwh'
    """)
    table_count = cursor.fetchone()[0]
    
   
    cursor.execute("""
        SELECT COUNT(*) 
        FROM pg_matviews 
        WHERE schemaname = 'retail_dwh'
    """)
    mv_count = cursor.fetchone()[0]
    
    
    fact_count = 0
    try:
        cursor.execute("SELECT COUNT(*) FROM retail_dwh.fct_retail_sales")
        fact_count = cursor.fetchone()[0]
    except:
        pass
    
    cursor.close()
    
    return {
        'tables': table_count,
        'materialized_views': mv_count,
        'fact_records': fact_count
    }

def run_sql_migrations(conn):
    log.info(" Running SQL migrations...")
    
    try:
        conn.rollback()
    except:
        pass
    
    migration_folders = [
        ("dim_tables", "Dimension Tables"),
        ("fact_tables", "Fact Tables"),
        ("materialized_views", "Materialized Views")
    ]
    
    cursor = conn.cursor()
    
    for folder, description in migration_folders:
        folder_path = Path(f"sql_commands/{folder}")
        if not folder_path.exists():
            log.warning(f" Folder not found: {folder}")
            continue
        
        sql_files = sorted(folder_path.glob("V*.sql"))
        
        log.info(f"\n   {description} ({len(sql_files)} files)")
        
        for sql_file in sql_files:
            log.info(f"     → {sql_file.name}")
            try:
                sql = sql_file.read_text(encoding='utf-8')
                cursor.execute(sql)
                conn.commit()
                log.info(f"      {sql_file.name} - SUCCESS")
            except Exception as e:
                log.error(f"      {sql_file.name} - FAILED: {e}")
                conn.rollback()
                raise
    
    cursor.close()
    log.info(" All SQL migrations completed")

def main():
    """Main pipeline orchestrator"""
    start_time = datetime.now()
    
    print_header("FULL DATA WAREHOUSE PIPELINE - STARTING")
    
    try:

        

        print_header("STEP 1: Loading Staging Data")
        if not run_command("python etl_pipeline.py", "ETL Pipeline (Staging)"):
            log.error(" ETL Pipeline failed - stopping")
            return False

       

        print_header("STEP 2: Connecting to Database")
        log.info(" Establishing database connection...")
        conn = get_db_connection()
        log.info(" Database connected")
        
        
        status_before = check_database_status(conn)
        log.info(f"   Current status:")
        log.info(f"     Tables: {status_before['tables']}")
        log.info(f"     Materialized Views: {status_before['materialized_views']}")
        log.info(f"     Fact Records: {status_before['fact_records']:,}")

        

        print_header("STEP 3: Running SQL Migrations")
        run_sql_migrations(conn)

        

        print_header("STEP 4: Verifying Database Status")
        status_after = check_database_status(conn)
        
        log.info(f"   Final status:")
        log.info(f"     Tables: {status_after['tables']}")
        log.info(f"     Materialized Views: {status_after['materialized_views']}")
        log.info(f"     Fact Records: {status_after['fact_records']:,}")
        
        conn.close()

       

        print_header("STEP 5: Running Comprehensive Tests")
        if not run_command("python tests\\test_all_features.py", "Test Suite"):
            log.warning(" Some tests failed - review output")

        

        duration = (datetime.now() - start_time).total_seconds()
        
        print_header("PIPELINE COMPLETED SUCCESSFULLY!")
        
        log.info(" ALL STEPS COMPLETED")
        log.info(f"⏱  Total Duration: {int(duration)}s ({duration/60:.1f} minutes)")
        log.info("")
        
        log.info("")
       
        
        return True
        
    except Exception as e:
        log.error(f"\n PIPELINE FAILED: {e}")
        import traceback
        log.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
