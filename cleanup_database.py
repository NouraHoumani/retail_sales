
import psycopg2
import yaml
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

def cleanup_database():
    """Drop all tables and materialized views from retail_dwh schema"""
    
    log.info("="*70)
    log.info("  DATABASE CLEANUP - STARTING")
    log.info("="*70)
    
    try:
       
        secrets = yaml.safe_load(Path('config/secrets.yaml').read_text())
        conn = psycopg2.connect(**secrets['database'])
        cursor = conn.cursor()
        
        log.info(" Connected to database")
        
        
        log.info("\n Dropping materialized views...")
        cursor.execute("""
            SELECT matviewname 
            FROM pg_matviews 
            WHERE schemaname = 'retail_dwh'
        """)
        mvs = cursor.fetchall()
        
        for (mv_name,) in mvs:
            log.info(f"  → Dropping {mv_name}")
            cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS retail_dwh.{mv_name} CASCADE")
        
        conn.commit()
        log.info(f" Dropped {len(mvs)} materialized views")
        
        
        log.info("\n Dropping tables...")
        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'retail_dwh'
        """)
        tables = cursor.fetchall()
        
        for (table_name,) in tables:
            log.info(f"  → Dropping {table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS retail_dwh.{table_name} CASCADE")
        
        conn.commit()
        log.info(f" Dropped {len(tables)} tables")
        
        
        cursor.execute("SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'retail_dwh'")
        remaining_tables = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM pg_matviews WHERE schemaname = 'retail_dwh'")
        remaining_mvs = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        log.info("\n" + "="*70)
        log.info("  DATABASE CLEANUP - COMPLETED")
        log.info("="*70)
        log.info(f" Remaining tables: {remaining_tables}")
        log.info(f" Remaining materialized views: {remaining_mvs}")
        log.info("\n Database is clean! Ready for fresh build.")
        log.info("   Run: python run_full_pipeline.py")
        
        return True
        
    except Exception as e:
        log.error(f"\n CLEANUP FAILED: {e}")
        import traceback
        log.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    import sys
    
    
    response = input("\n  WARNING: This will DELETE ALL DATA in retail_dwh schema!\nAre you sure? (type 'yes' to confirm): ")
    
    if response.lower() == 'yes':
        success = cleanup_database()
        sys.exit(0 if success else 1)
    else:
        print(" Cleanup cancelled.")
        sys.exit(1)
