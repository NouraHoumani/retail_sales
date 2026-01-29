
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from handlers.db_manager import create_database_connection, run_sql_query, close_db_connection
from handlers.cache_manager import CacheManager
import pandas as pd
import time
from datetime import datetime

def print_section(title):
    
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")

def print_result(test_name, passed, message=""):
    
    status = "[PASS]" if passed else "[FAIL]"
    print(f"{status} | {test_name}")
    if message:
        print(f"       {message}")

def test_database_connection():
    print_section("TEST 1: Database Connection")
    
    try:
        connection = create_database_connection()
        if connection:
            print_result("Database connection", True, "Connected successfully")
            close_db_connection(connection)
            return True
        else:
            print_result("Database connection", False, "Failed to connect")
            return False
    except Exception as e:
        print_result("Database connection", False, str(e))
        return False

def test_table_partitioning():
    print_section("TEST 2: Table Partitioning")
    
    connection = create_database_connection()
    if not connection:
        print_result("Partitioning test", False, "No database connection")
        return False
    
    try:
        
        query1 = """
        SELECT COUNT(*) as partition_count
        FROM pg_tables
        WHERE schemaname = 'retail_dwh' 
          AND tablename LIKE 'fct_retail_sales_20%';
        """
        
        df = pd.read_sql(query1, connection)
        partition_count = df['partition_count'].iloc[0]
        
        if partition_count > 0:
            print_result("Partitioned tables exist", True, f"Found {partition_count} partitions")
        else:
            print_result("Partitioned tables exist", False, "No partitions found")
            close_db_connection(connection)
            return False
        
       
        query2 = """
        SELECT COUNT(*) as total_rows
        FROM retail_dwh.fct_retail_sales;
        """
        
        df2 = pd.read_sql(query2, connection)
        total_rows = df2['total_rows'].iloc[0]
        
        print_result("Data in partitions", True, f"{total_rows:,} rows total")
        
        
        query3 = """
        EXPLAIN (FORMAT JSON)
        SELECT COUNT(*) 
        FROM retail_dwh.fct_retail_sales
        WHERE invoice_timestamp >= '2011-01-01' 
          AND invoice_timestamp < '2011-02-01';
        """
        
        df3 = pd.read_sql(query3, connection)
        explain_plan = str(df3.iloc[0, 0])
        
        if 'Partitions' in explain_plan or 'fct_retail_sales_2011' in explain_plan:
            print_result("Partition pruning", True, "Query uses partition pruning")
        else:
            print_result("Partition pruning", False, "No partition pruning detected")
        
        close_db_connection(connection)
        return True
        
    except Exception as e:
        print_result("Partitioning test", False, str(e))
        close_db_connection(connection)
        return False

def test_materialized_views():
    print_section("TEST 3: Materialized Views")
    
    connection = create_database_connection()
    if not connection:
        print_result("MV test", False, "No database connection")
        return False
    
    try:
        
        query1 = """
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = 'retail_dwh'
        ORDER BY matviewname;
        """
        
        df = pd.read_sql(query1, connection)
        mv_count = len(df)
        
        if mv_count > 0:
            print_result("Materialized views exist", True, f"Found {mv_count} MVs")
            for mv in df['matviewname']:
                print(f"       - {mv}")
        else:
            print_result("Materialized views exist", False, "No MVs found")
            close_db_connection(connection)
            return False
        
        
        expected_mvs = [
            'mv_monthly_sales_summary',
            'mv_top_products',
            'mv_customer_segments',
            'mv_daily_sales_trend',
            'mv_country_performance',
            'mv_product_category_analysis'
        ]
        
        for mv_name in expected_mvs:
            query = f"SELECT COUNT(*) as row_count FROM retail_dwh.{mv_name};"
            try:
                df_count = pd.read_sql(query, connection)
                row_count = df_count['row_count'].iloc[0]
                
                if row_count > 0:
                    print_result(f"MV data: {mv_name}", True, f"{row_count:,} rows")
                else:
                    print_result(f"MV data: {mv_name}", False, "Empty")
            except Exception as e:
                print_result(f"MV data: {mv_name}", False, f"Error: {e}")
        
        
        query_refresh = "SELECT * FROM retail_dwh.refresh_all_materialized_views();"
        try:
            df_refresh = pd.read_sql(query_refresh, connection)
            print_result("MV refresh function", True, "Refresh function works")
        except Exception as e:
            print_result("MV refresh function", False, str(e))
        
        close_db_connection(connection)
        return True
        
    except Exception as e:
        print_result("Materialized views test", False, str(e))
        close_db_connection(connection)
        return False

def test_caching_layer():
    print_section("TEST 4: Caching Layer")
    
    try:
        
        cache = CacheManager(use_redis=True)
        print_result("Cache initialization", True, f"Cache type: {cache.cache_type}")
        
        
        test_key = "test_key_12345"
        test_value = {"data": "test", "timestamp": str(datetime.now())}
        
        cache.set(test_key, test_value, ttl=60)
        retrieved = cache.get(test_key)
        
        if retrieved and retrieved == test_value:
            print_result("Cache set/get", True, "Data stored and retrieved correctly")
        else:
            print_result("Cache set/get", False, "Data mismatch")
        
        
        if cache.exists(test_key):
            print_result("Cache exists check", True, "Key exists")
        else:
            print_result("Cache exists check", False, "Key doesn't exist")
        
        
        cache.delete(test_key)
        if not cache.exists(test_key):
            print_result("Cache delete", True, "Key deleted successfully")
        else:
            print_result("Cache delete", False, "Key still exists")
        
        
        stats = cache.get_stats()
        if stats:
            print_result("Cache stats", True, f"Stats retrieved: {stats.get('cache_type', 'N/A')}")
        else:
            print_result("Cache stats", False, "Could not get stats")
        
        return True
        
    except Exception as e:
        print_result("Caching layer test", False, str(e))
        return False

def test_query_performance():
    print_section("TEST 5: Query Performance Comparison")
    
    connection = create_database_connection()
    if not connection:
        print_result("Performance test", False, "No database connection")
        return False
    
    try:
        
        query_mv = "SELECT * FROM retail_dwh.mv_monthly_sales_summary LIMIT 100;"
        
        start_time = time.time()
        df_mv = pd.read_sql(query_mv, connection)
        mv_time = time.time() - start_time
        
        print_result(
            "Materialized view query", 
            mv_time < 1.0, 
            f"Time: {mv_time:.4f}s ({len(df_mv)} rows)"
        )
        
        
        query_partition = """
        SELECT COUNT(*) as count
        FROM retail_dwh.fct_retail_sales
        WHERE invoice_timestamp >= '2011-01-01' 
          AND invoice_timestamp < '2011-02-01';
        """
        
        start_time = time.time()
        df_partition = pd.read_sql(query_partition, connection)
        partition_time = time.time() - start_time
        
        print_result(
            "Partitioned query", 
            partition_time < 2.0, 
            f"Time: {partition_time:.4f}s ({df_partition['count'].iloc[0]:,} rows)"
        )
        
        
        query_fact = "SELECT COUNT(*) as total FROM retail_dwh.fct_retail_sales;"
        
        start_time = time.time()
        df_fact = pd.read_sql(query_fact, connection)
        fact_time = time.time() - start_time
        
        print_result(
            "Full fact table count", 
            fact_time < 5.0, 
            f"Time: {fact_time:.4f}s ({df_fact['total'].iloc[0]:,} rows)"
        )
        
        close_db_connection(connection)
        return True
        
    except Exception as e:
        print_result("Performance test", False, str(e))
        close_db_connection(connection)
        return False

def test_data_integrity():
    print_section("TEST 6: Data Integrity")
    
    connection = create_database_connection()
    if not connection:
        print_result("Integrity test", False, "No database connection")
        return False
    
    try:
        
        query1 = "SELECT COUNT(*) as fact_count FROM retail_dwh.fct_retail_sales;"
        df1 = pd.read_sql(query1, connection)
        fact_count = df1['fact_count'].iloc[0]
        
        print_result("Fact table populated", fact_count > 0, f"{fact_count:,} rows")
        
        
        dims = ['dim_date', 'dim_product', 'dim_customer']
        for dim in dims:
            query = f"SELECT COUNT(*) as count FROM retail_dwh.{dim};"
            df = pd.read_sql(query, connection)
            count = df['count'].iloc[0]
            print_result(f"{dim} populated", count > 0, f"{count:,} rows")
        
        
        query_orphans = """
        SELECT COUNT(*) as orphan_count
        FROM retail_dwh.fct_retail_sales f
        LEFT JOIN retail_dwh.dim_product p ON f.product_key = p.product_key
        WHERE p.product_key IS NULL;
        """
        df_orphans = pd.read_sql(query_orphans, connection)
        orphan_count = df_orphans['orphan_count'].iloc[0]
        
        print_result("No orphaned product records", orphan_count == 0, 
                    f"{orphan_count} orphaned records" if orphan_count > 0 else "All FKs valid")
        
        close_db_connection(connection)
        return True
        
    except Exception as e:
        print_result("Data integrity test", False, str(e))
        close_db_connection(connection)
        return False

def run_all_tests():
    """Run all tests and provide summary"""
    
    print("\n" + "="*70)
    print("  RETAIL SALES DATA WAREHOUSE - TEST SUITE")
    print("  Testing: Partitioning, Materialized Views, Caching")
    print("="*70)
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Table Partitioning", test_table_partitioning),
        ("Materialized Views", test_materialized_views),
        ("Caching Layer", test_caching_layer),
        ("Query Performance", test_query_performance),
        ("Data Integrity", test_data_integrity)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n[ERROR] CRITICAL ERROR in {test_name}: {e}")
            results.append((test_name, False))
    
    
    print_section("TEST SUMMARY")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"{status} | {test_name}")
    
    print(f"\n{'='*70}")
    print(f"OVERALL: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    print(f"{'='*70}\n")
    
    if passed == total:
        print("[SUCCESS] ALL TESTS PASSED!.")
    else:
        print("[WARNING] Some tests failed. Review the output above for details.")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
