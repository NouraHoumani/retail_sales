import re
import sys
import os
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))
os.chdir(project_root)

from handlers.db_manager import create_database_connection, run_sql_query, close_db_connection

def version_num(filename: str) -> int:
    """Extract version number from V1__name.sql"""
    m = re.match(r"^V(\d+)", filename, re.IGNORECASE)
    if not m:
        raise ValueError(f"Not a versioned migration file: {filename}")
    return int(m.group(1))


def run_migrations():
    
    base_path = Path(__file__).parent
    
    
    folders = ["dim_tables", "fact_tables", "materialized_views"]
    
    connection = create_database_connection()
    
    try:
        print(f"\n{'='*70}")
        print(f"BUILDING STAR SCHEMA")
        print(f"{'='*70}\n")
        
        total_executed = 0
        
        
        print(f" PRE-CLEANUP")
        print(f"{'─'*70}")
        cleanup_sql = """
        -- Drop tables in safe order (fact first, then dims)
        DROP TABLE IF EXISTS retail_dwh.fct_retail_sales CASCADE;
        DROP TABLE IF EXISTS retail_dwh.dim_product CASCADE;
        DROP TABLE IF EXISTS retail_dwh.dim_customer CASCADE;
        DROP TABLE IF EXISTS retail_dwh.dim_date CASCADE;
        """
        print("Dropping existing tables...", end=" ")
        if run_sql_query(connection, cleanup_sql):
            print("\n")
        else:
            print("  (tables may not exist yet)\n")
        
        # STEP 1: Run migrations
        for folder in folders:
            folder_path = base_path / folder
            
            if not folder_path.exists():
                print(f"  Skipping {folder}/ (not found)")
                print(f"   Looking in: {folder_path.absolute()}")
                continue
            
            # Find all V*.sql files
            sql_files = [
                p for p in folder_path.glob("*.sql")
                if re.match(r"^V\d+", p.name, re.IGNORECASE)
            ]
            
            if not sql_files:
                print(f" No migrations in {folder}/")
                continue
            
            # Sort by version
            sql_files_sorted = sorted(sql_files, key=lambda p: version_num(p.name))
            
            print(f"\n {folder.upper()} LAYER ({len(sql_files_sorted)} files)")
            print(f"{'─'*70}")
            
            for i, sql_file in enumerate(sql_files_sorted, 1):
                print(f"[{i}/{len(sql_files_sorted)}] {sql_file.name}...", end=" ")
                
                # Read SQL
                sql = sql_file.read_text(encoding="utf-8")
                
                # Execute
                success = run_sql_query(connection, sql)
                
                if success:
                    print("yes")
                    total_executed += 1
                else:
                    print("no")
                    print(f"\n FAILED at {folder}/{sql_file.name}")
                    return False
        
        print(f"\n{'='*70}")
        print(f" STAR SCHEMA COMPLETE ({total_executed} migrations)")
        print(f"{'='*70}\n")
        
        return True
    
    finally:
        close_db_connection(connection)


if __name__ == "__main__":
    success = run_migrations()
    sys.exit(0 if success else 1)