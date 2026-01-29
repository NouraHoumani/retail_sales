"""Check for duplicates in all tables"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from handlers.db_manager import create_database_connection, close_db_connection

def check_duplicates():
    connection = create_database_connection()
    
    try:
        cursor = connection.cursor()
        
        print("\n" + "="*70)
        print("DUPLICATE CHECK SUMMARY")
        print("="*70 + "\n")
        
        query = """
        SELECT 
            'stg_retail_sales' as table_name,
            COUNT(*) as total_rows,
            COUNT(DISTINCT (invoice_no, stock_code, invoice_date, quantity, unit_price)) as unique_rows
        FROM retail_dwh.stg_retail_sales
        
        UNION ALL
        
        SELECT 
            'dim_date',
            COUNT(*),
            COUNT(DISTINCT date_value)
        FROM retail_dwh.dim_date
        
        UNION ALL
        
        SELECT 
            'dim_product',
            COUNT(*),
            COUNT(DISTINCT stock_code)
        FROM retail_dwh.dim_product
        
        UNION ALL
        
        SELECT 
            'dim_customer',
            COUNT(*),
            COUNT(DISTINCT customer_id)
        FROM retail_dwh.dim_customer
        
        UNION ALL
        
        SELECT 
            'fct_retail_sales',
            COUNT(*),
            COUNT(DISTINCT sales_pk_id)
        FROM retail_dwh.fct_retail_sales
        
        ORDER BY table_name;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"{'Table':<20} {'Total Rows':<15} {'Unique Rows':<15} {'Duplicates':<15}")
        print("-" * 70)
        
        for row in results:
            table_name = row[0]
            total = row[1]
            unique = row[2]
            duplicates = total - unique
            print(f"{table_name:<20} {total:<15,} {unique:<15,} {duplicates:<15,}")
        
        
        print("\n" + "="*70)
        print("CUSTOMER_ID = 0 CHECK")
        print("="*70 + "\n")
        
        query2 = """
        SELECT 
            customer_key,
            customer_id,
            country,
            is_guest,
            first_purchase_date
        FROM retail_dwh.dim_customer
        WHERE customer_id = 0 OR customer_id IS NULL
        ORDER BY customer_key
        LIMIT 10;
        """
        
        cursor.execute(query2)
        results2 = cursor.fetchall()
        
        if results2:
            print(f"Found {len(results2)} customers with customer_id = 0 or NULL:")
            print(f"{'Key':<10} {'ID':<10} {'Country':<20} {'Guest':<10} {'First Purchase':<20}")
            print("-" * 70)
            for row in results2:
                print(f"{row[0]:<10} {row[1] or 'NULL':<10} {row[2]:<20} {row[3]:<10} {row[4]}")
        else:
            print("No customers with customer_id = 0 or NULL")
        
        s
        print("\n" + "="*70)
        print("STAGING TABLE DUPLICATES")
        print("="*70 + "\n")
        
        query3 = """
        SELECT 
            invoice_no,
            stock_code,
            invoice_date,
            quantity,
            unit_price,
            COUNT(*) as duplicate_count
        FROM retail_dwh.stg_retail_sales
        GROUP BY invoice_no, stock_code, invoice_date, quantity, unit_price
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT 5;
        """
        
        cursor.execute(query3)
        results3 = cursor.fetchall()
        
        if results3:
            print(f"Found duplicate rows in staging:")
            print(f"{'Invoice':<15} {'Stock Code':<15} {'Date':<20} {'Qty':<10} {'Price':<10} {'Count':<10}")
            print("-" * 90)
            for row in results3:
                print(f"{row[0]:<15} {row[1]:<15} {str(row[2]):<20} {row[3]:<10} {row[4]:<10} {row[5]:<10}")
        else:
            print("No duplicates in staging table")
        
        cursor.close()
        
    finally:
        close_db_connection(connection)

if __name__ == "__main__":
    check_duplicates()