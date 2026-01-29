# Retail Sales Data Warehouse - Complete Documentation

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Data Flow](#data-flow)
4. [File Structure](#file-structure)
5. [Database Schema](#database-schema)
6. [ETL Pipeline](#etl-pipeline)
7. [Data Quality](#data-quality)
8. [Configuration](#configuration)
9. [Setup & Installation](#setup--installation)
10. [Usage](#usage)
11. [SQL Migrations](#sql-migrations)
12. [Testing](#testing)
13. [Monitoring](#monitoring)
14. [Performance](#performance)

---

## Project Overview

### Purpose
Production-ready data warehouse for retail sales analytics with automated ETL pipeline, comprehensive data quality validation, and performance optimizations.

### Key Features
- Star schema data warehouse design
- Automated ETL with 17 data quality rules
- Table partitioning (26 monthly partitions)
- Materialized views for fast analytics
- Batch tracking and data lineage
- Caching layer for frequently accessed queries
- Comprehensive test suite

### Technologies
- **Database:** PostgreSQL 15+
- **Language:** Python 3.12
- **Key Libraries:** pandas, psycopg2, pyyaml, schedule, redis
- **Schema:** Star schema (3 dimensions, 1 fact table)

### Data Volume
- **Source Records:** 541,909 rows
- **Valid Records:** 534,756 rows (98.7%)
- **Quarantined Records:** 7,153 rows (1.3%)
- **Products:** 3,811 unique
- **Customers:** 4,339 unique
- **Date Range:** Dec 2009 - Dec 2011

---

## Architecture

### Overall Architecture
```
CSV Data Source
      ↓
ETL Pipeline (etl_pipeline.py)
      ↓
Staging Tables (stg_retail_sales)
      ↓
Data Quality Validation (17 rules)
      ↓
Dimension Tables (dim_date, dim_product, dim_customer)
      ↓
Fact Table (fct_retail_sales) - Partitioned
      ↓
Materialized Views (6 aggregate tables)
      ↓
Analytics & Reporting
```

### Database Schema Type
**Star Schema**
- 3 Dimension Tables
- 1 Fact Table (partitioned into 26 monthly tables)
- 6 Materialized Views (pre-computed aggregates)

### Data Quality Architecture
```
Raw Data → Validation Rules → Valid Data → Warehouse
                ↓
          Invalid Data → Quarantine Table
```

---

## Data Flow

### Step-by-Step Data Flow

1. **Data Ingestion**
   - Source: `data/raw/online_retail.csv`
   - Process: Read CSV with pandas
   - Output: Raw DataFrame

2. **Data Transformation**
   - Type conversions (strings → numeric, datetime)
   - Calculated fields (line_total, is_cancellation, is_valid_sale)
   - Country standardization
   - Guest customer handling

3. **Data Quality Validation**
   - 17 validation rules applied
   - Valid records → staging table
   - Invalid records → quarantine table
   - Metrics logged to dq_metrics table

4. **Staging Load**
   - Table: `stg_retail_sales`
   - Records: 534,756 valid rows
   - Includes all source columns + calculated fields

5. **Dimension Creation**
   - `dim_date`: Extract unique dates, add fiscal attributes
   - `dim_product`: Extract unique products, calculate pricing stats
   - `dim_customer`: Extract unique customers, add segmentation flags

6. **Fact Table Load**
   - Join staging with dimensions to get surrogate keys
   - Create composite business key (MD5 hash)
   - Insert into `fct_retail_sales`

7. **Partitioning**
   - Fact table partitioned by `invoice_timestamp`
   - 26 monthly partitions (2009-12 through 2011-12)
   - Enables partition pruning for faster queries

8. **Materialized Views**
   - 6 pre-computed aggregates created
   - Monthly sales, top products, customer segments, etc.
   - Refresh on demand or scheduled

---

## File Structure

### Root Directory Files

#### `etl_pipeline.py` (Main ETL)
**Purpose:** Core ETL pipeline that loads data from CSV to staging tables

**Key Functions:**
- `run_etl_pipeline(mode='full')` - Main entry point
- `DataQualityTracker` - Tracks DQ metrics and quarantine records
- `create_staging_table()` - Creates staging table if not exists
- `apply_data_quality_rules()` - Applies 17 validation rules
- `load_data_to_staging()` - Inserts valid records to staging

**Configuration:**
- `--mode full` - Full reload (default)
- `--mode incremental` - Incremental load (future enhancement)

**Output:**
- Staging table populated
- Quarantine table populated
- DQ metrics logged
- Batch execution logged

**Run:** `python etl_pipeline.py`

---

#### `run_full_pipeline.py` (Master Orchestrator)
**Purpose:** Runs complete end-to-end pipeline from CSV to analytics

**Execution Steps:**
1. Run ETL pipeline (staging load)
2. Connect to database
3. Run SQL migrations (dimensions → facts → partitions → MVs)
4. Verify database status
5. Run comprehensive tests

**Output:**
- 34 tables created
- 6 materialized views created
- 534,756 fact records loaded
- Test results (6/6 passing)

**Run:** `python run_full_pipeline.py`

**Duration:** 4-5 minutes for full rebuild

---

#### `scheduler.py` (ETL Scheduler)
**Purpose:** Schedules ETL to run automatically at intervals

**Default Schedule:** Daily at 08:00

**Configuration:**
```python
schedule.every().day.at("08:00").do(scheduled_etl_job)
schedule.every().hour.do(scheduled_etl_job)
schedule.every(30).minutes.do(scheduled_etl_job)
```

**Run:** `python scheduler.py` (runs until Ctrl+C)

**Use Case:** Development/testing. For production, use OS schedulers (cron, Task Scheduler) or Airflow.

---

#### `cleanup_database.py` (Database Reset)
**Purpose:** Drops all tables and materialized views for clean rebuild

**What it does:**
- Drops all materialized views in retail_dwh schema
- Drops all tables in retail_dwh schema
- Confirms cleanup complete

**Safety:** Prompts for confirmation before deleting

**Run:** `python cleanup_database.py`

---

#### `requirements.txt` (Dependencies)
**Python packages required:**
```
pandas==2.1.4          # Data manipulation
psycopg2==2.9.9        # PostgreSQL adapter
pyyaml==6.0.1          # YAML configuration
schedule>=1.2.0        # Job scheduling
redis>=5.0.0           # Caching layer
```

**Install:** `pip install -r requirements.txt`

---

### config/ Directory

#### `config.yaml` (Application Configuration)
**Purpose:** Application-level settings

**Not currently used** - Reserved for future enhancements (logging levels, retry logic, etc.)

---

#### `data_quality_rules.yaml` (DQ Rules Configuration)
**Purpose:** Defines all 17 data quality validation rules

**Rule Categories:**
1. **missing_values** - Check for NULL/empty values
2. **format_validation** - Validate data formats
3. **business_logic** - Business rule validation
4. **statistical** - Outlier detection

**Rule Actions:**
- `quarantine` - Move to quarantine table
- `drop` - Remove from pipeline
- `flag` - Mark but keep

**Example Rule:**
```yaml
missing_values:
  - name: check_missing_invoice_no
    column: InvoiceNo
    action: quarantine
    severity: high
```

---

#### `secrets.yaml` (Database Credentials)
**Purpose:** Database connection details

**Structure:**
```yaml
database:
  host: localhost
  port: 5432
  database: retail_dwh
  user: postgres
  password: your_password
```

**Security:** Excluded from git via .gitignore

**Template:** Copy from `secrets.yaml.template`

---

### handlers/ Directory

#### `db_manager.py` (Database Operations)
**Purpose:** Centralized database connection and query execution

**Functions:**
- `create_database_connection()` - Establishes PostgreSQL connection
- `run_sql_query(conn, query)` - Executes SQL query
- `close_db_connection(conn)` - Closes connection

**Usage:**
```python
conn = create_database_connection()
run_sql_query(conn, "SELECT * FROM table")
close_db_connection(conn)
```

---

#### `data_processor.py` (Data Processing Utilities)
**Purpose:** Data transformation and processing functions

**Functions:**
- `clean_string_column()` - Clean and standardize strings
- `parse_dates()` - Parse date columns
- `calculate_line_total()` - Calculate transaction amounts
- `detect_cancellations()` - Identify cancellation records

**Note:** Currently, most logic is in etl_pipeline.py. This file is for future refactoring.

---

#### `cache_manager.py` (Caching Layer)
**Purpose:** Query result caching with Redis/memory backend

**Class:** `CacheManager`

**Methods:**
- `get(key)` - Retrieve from cache
- `set(key, value, ttl)` - Store in cache with TTL
- `delete(key)` - Remove from cache
- `clear(pattern)` - Clear by pattern
- `exists(key)` - Check if key exists
- `get_stats()` - Get cache statistics

**Backends:**
1. **Redis** - Primary (if available)
2. **Memory** - Fallback (Python dict)

**Configuration:**
- Default TTL: 3600 seconds (1 hour)
- Redis: localhost:6379

**Usage:**
```python
cache = CacheManager()
cache.set('query_result', data, ttl=1800)
result = cache.get('query_result')
```

---

### source_handlers/ Directory

#### `retail_ingestion.py` (Data Source Handler)
**Purpose:** Handles data ingestion from CSV source

**Note:** Currently not actively used. Logic is in etl_pipeline.py. Reserved for future multi-source ingestion.

---

### sql_commands/ Directory

#### `run_migrations.py` (Migration Runner)
**Purpose:** Executes SQL migration files in order

**Folders Processed:**
1. `dim_tables/` - Dimension tables (V1, V2, V3)
2. `fact_tables/` - Fact tables (V1, V2)
3. `materialized_views/` - Aggregate views (V1)

**Naming Convention:** `V{number}__{description}.sql`

**Execution Order:** Sorted alphabetically by filename

**Run:** `python sql_commands/run_migrations.py`

---

### sql_commands/dim_tables/

#### `V1__create_dim_date.sql`
**Purpose:** Creates date dimension table

**Table:** `dim_date`

**Columns:**
- `date_key` (PK) - Surrogate key (YYYYMMDD format)
- `full_date` - Actual date
- `year`, `quarter`, `month`, `day` - Date parts
- `day_of_week`, `day_name` - Day attributes
- `month_name` - Month name
- `is_weekend`, `is_holiday` - Flags

**Data:** Generates dates from Dec 1, 2009 to Dec 31, 2011 (373 dates)

**Load Source:** Generated, not from staging

---

#### `V2__create_dim_product.sql`
**Purpose:** Creates product dimension table

**Table:** `dim_product`

**Columns:**
- `product_key` (PK) - Surrogate key (auto-increment)
- `stock_code` - Business key (unique)
- `description` - Product name
- `first_seen_date`, `last_seen_date` - Temporal attributes
- `total_quantity_sold` - Aggregate quantity
- `total_revenue_generated` - Aggregate revenue
- `avg_unit_price`, `min_unit_price`, `max_unit_price` - Pricing stats
- `transaction_count` - Number of transactions

**Data Source:** `stg_retail_sales` (aggregated)

**Load Logic:** 
- Extract unique products
- Calculate statistics
- Handle duplicates with ON CONFLICT DO UPDATE

**Record Count:** ~3,811 products

---

#### `V3__create_dim_customer.sql`
**Purpose:** Creates customer dimension table

**Table:** `dim_customer`

**Columns:**
- `customer_key` (PK) - Surrogate key (auto-increment)
- `customer_pk_id` - Business key (MD5 hash)
- `customer_id` - Original customer ID (nullable for guests)
- `country` - Customer country
- `is_guest` - Guest flag (TRUE if no customer_id)
- `first_purchase_date`, `last_purchase_date` - Temporal
- `total_transactions` - Transaction count
- `total_revenue` - Lifetime value

**Data Source:** `stg_retail_sales` (aggregated)

**Load Logic:**
- Separate inserts for guests and registered customers
- Guests: customer_pk_id = MD5(country + 'GUEST')
- Registered: customer_pk_id = MD5(customer_id)
- Handle duplicates with ON CONFLICT DO UPDATE

**Record Count:** ~4,339 customers

---

### sql_commands/fact_tables/

#### `V1__create_fct_retail_sales.sql`
**Purpose:** Creates and loads main fact table

**Table:** `fct_retail_sales`

**Columns:**
- `sales_key` (PK) - Surrogate key (auto-increment)
- `sales_pk_id` - Business key (MD5 hash)
- **Foreign Keys:**
  - `product_key` → dim_product
  - `customer_key` → dim_customer
  - `date_key` → dim_date
- **Degenerate Dimensions:**
  - `invoice_no` - Invoice number
  - `invoice_timestamp` - Transaction timestamp
- **Measures:**
  - `quantity` - Quantity sold
  - `unit_price` - Price per unit
  - `line_total` - Total amount
- **Flags:**
  - `is_cancellation` - Cancellation indicator
  - `is_valid_sale` - Valid sale flag
  - `is_return` - Return indicator
  - `is_guest_purchase` - Guest purchase flag
- **Lineage:**
  - `source` - Data source identifier
  - `batch_id` - ETL batch ID
  - `loaded_at` - Load timestamp

**Load Logic:**
1. Insert from staging
2. Join with dimensions to get surrogate keys
3. Create MD5 business key from InvoiceNo + StockCode + InvoiceDate
4. Handle duplicates with ON CONFLICT DO NOTHING

**Record Count:** 534,756 transactions

---

#### `V2__create_partitioned_fact_table.sql`
**Purpose:** Converts fact table to partitioned table

**Strategy:** RANGE partitioning by `invoice_timestamp`

**Process:**
1. Rename existing table to `fct_retail_sales_old`
2. Create new partitioned table with same structure
3. Create 26 monthly partitions (2009-12 through 2011-12)
4. Copy data from old table to new partitioned table
5. Verify row counts match
6. Drop old table (optional, currently kept)

**Partitions Created:**
- `fct_retail_sales_2009_12` (Dec 2009)
- `fct_retail_sales_2010_01` through `fct_retail_sales_2010_12`
- `fct_retail_sales_2011_01` through `fct_retail_sales_2011_12`

**Benefits:**
- Partition pruning: Queries scan only relevant partitions
- Faster maintenance: VACUUM per partition
- Easier archival: Drop old partitions
- Better INSERT performance

**Idempotency:** Checks if already partitioned before running

---

### sql_commands/materialized_views/

#### `V1__create_materialized_views.sql`
**Purpose:** Creates 6 pre-computed aggregate tables

---

**1. `mv_monthly_sales_summary`**
- Monthly aggregates: total orders, revenue, customers, products
- Includes avg order value and units per order
- 13 rows (one per month)

**2. `mv_top_products`**
- Top 1000 products by revenue
- Includes quantity sold, revenue, avg price, transaction count
- Useful for product performance dashboards

**3. `mv_customer_segments`**
- RFM (Recency, Frequency, Monetary) analysis
- Segments: Champions, Loyal, Potential, At Risk, Lost
- 4,337 rows (one per customer with valid sales)

**4. `mv_daily_sales_trend`**
- Daily sales aggregates
- Includes 7-day and 30-day moving averages
- 304 rows (one per day)

**5. `mv_country_performance`**
- Country-level aggregates
- Total revenue, customers, orders per country
- 38 rows (one per country)

**6. `mv_product_category_analysis`**
- Category performance (first letter of stock_code)
- Revenue, quantity, avg price per category
- 42 rows (one per category)

**Refresh Function:**
- `refresh_all_materialized_views()` - Refreshes all 6 MVs
- Run: `SELECT retail_dwh.refresh_all_materialized_views();`

**Performance:** 500-1000x faster than querying fact table directly

---

### tests/ Directory

#### `test_all_features.py` (Comprehensive Test Suite)
**Purpose:** Tests all major features and components

**Test Categories:**

1. **Database Connection**
   - Tests PostgreSQL connectivity
   - Verifies retail_dwh schema exists

2. **Table Partitioning**
   - Checks if fact table is partitioned
   - Verifies 26 partitions exist
   - Tests partition-specific data

3. **Materialized Views**
   - Verifies 6 MVs exist
   - Checks each MV has data
   - Tests refresh function

4. **Caching Layer**
   - Tests cache set/get operations
   - Verifies TTL expiration
   - Tests pattern-based clearing

5. **Query Performance**
   - Compares MV query vs fact table query
   - Measures execution time
   - Validates performance improvement

6. **Data Integrity**
   - Checks for orphaned records
   - Validates foreign key relationships
   - Verifies referential integrity

**Run:** `python tests/test_all_features.py`

**Expected:** 6/6 tests passing (100%)

---

## Database Schema

### Schema Name
`retail_dwh` - All tables and views are in this schema

### Tables Overview

| Table Type | Table Name | Rows | Purpose |
|------------|------------|------|---------|
| Dimension | dim_date | 373 | Date dimension |
| Dimension | dim_product | 3,811 | Product master |
| Dimension | dim_customer | 4,339 | Customer master |
| Fact | fct_retail_sales | 534,756 | Sales transactions |
| Partitions | fct_retail_sales_YYYY_MM | Varies | Monthly partitions (26) |
| Staging | stg_retail_sales | 536,629 | Staging area |
| DQ | dq_quarantine_sales | 7,153 | Invalid records |
| DQ | dq_metrics | Varies | DQ metrics log |
| Meta | meta_etl_batch_log | Varies | Batch execution log |
| MV | mv_monthly_sales_summary | 13 | Monthly aggregates |
| MV | mv_top_products | 1,000 | Top products |
| MV | mv_customer_segments | 4,337 | Customer RFM |
| MV | mv_daily_sales_trend | 304 | Daily trends |
| MV | mv_country_performance | 38 | Country stats |
| MV | mv_product_category_analysis | 42 | Category stats |

### Relationships

```
dim_date (date_key) ←------- fct_retail_sales (date_key)
dim_product (product_key) ←- fct_retail_sales (product_key)
dim_customer (customer_key) ← fct_retail_sales (customer_key)
```

### Indexing Strategy

**Primary Keys:**
- All dimension tables: Surrogate key (auto-increment)
- Fact table: Composite (sales_key, invoice_timestamp)

**Foreign Keys:**
- fct_retail_sales: Indexed on product_key, customer_key, date_key

**Unique Constraints:**
- dim_product: stock_code
- dim_customer: customer_pk_id
- fct_retail_sales: sales_pk_id (per partition)

---

## ETL Pipeline

### Pipeline Components

1. **Extraction**
   - Source: CSV file
   - Tool: pandas.read_csv()
   - Output: Raw DataFrame

2. **Transformation**
   - Type conversions
   - Calculated fields
   - Data standardization
   - Business logic application

3. **Validation**
   - 17 data quality rules
   - Quarantine invalid records
   - Log metrics

4. **Loading**
   - Staging table insert
   - Dimension table upserts
   - Fact table inserts
   - Materialized view creation

### ETL Execution Modes

**Full Mode (default):**
- Truncates staging table
- Reloads all data from source
- Recreates dimensions and facts
- Use for: Initial load, full refresh

**Incremental Mode (future):**
- Loads only new/changed records
- Appends to staging
- Updates dimensions
- Inserts new facts
- Use for: Daily updates

### Batch Tracking

Every ETL run creates a unique batch:
- **Batch ID:** Timestamp-based (YYYYMMDD_HHMMSS)
- **Logged to:** `meta_etl_batch_log`
- **Includes:** Start/end time, status, row counts, errors

---

## Data Quality

### Data Quality Rules (17 Total)

**Category 1: Missing Values (8 rules)**
1. Check missing InvoiceNo
2. Check missing StockCode
3. Check missing Description
4. Check missing Quantity
5. Check missing InvoiceDate
6. Check missing UnitPrice
7. Check missing CustomerID (warning only)
8. Check missing Country

**Category 2: Format Validation (3 rules)**
9. Validate InvoiceNo format
10. Validate InvoiceDate format
11. Validate StockCode format

**Category 3: Business Logic (4 rules)**
12. Check negative/zero prices
13. Check extreme quantities
14. Detect cancellations
15. Validate line total calculation

**Category 4: Statistical (2 rules)**
16. Detect price outliers
17. Detect quantity outliers

### Quarantine System

**Table:** `dq_quarantine_sales`

**Columns:**
- Original row data (all source columns)
- `dq_reason` - Why quarantined
- `rule_name` - Which rule failed
- `batch_id` - Which batch
- `quarantined_at` - When quarantined
- `raw_row_json` - Full row as JSON

**Purpose:** Allows investigation and potential recovery of invalid records

---

## Configuration

### Environment Setup

**1. Database Configuration** (`config/secrets.yaml`):
```yaml
database:
  host: localhost
  port: 5432
  database: retail_dwh
  user: postgres
  password: your_password
```

**2. Data Quality Rules** (`config/data_quality_rules.yaml`):
- Customize validation rules
- Add new rules
- Modify actions (quarantine/drop/flag)

**3. Python Dependencies** (`requirements.txt`):
- Install: `pip install -r requirements.txt`

---

## Setup & Installation

### Prerequisites
- Python 3.12+
- PostgreSQL 15+
- Git (for cloning)

### Installation Steps

1. **Clone Repository**
```bash
git clone <repository-url>
cd retail-sales-de
```

2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure Database**
```bash
cp config/secrets.yaml.template config/secrets.yaml
# Edit secrets.yaml with your database credentials
```

4. **Place Data File**
```bash
# Copy your CSV file to:
data/raw/online_retail.csv
```

5. **Run Full Pipeline**
```bash
python run_full_pipeline.py
```

6. **Verify Installation**
```bash
python tests/test_all_features.py
```

---

## Usage

### Common Commands

**Full Pipeline (Recommended):**
```bash
python run_full_pipeline.py
```

**ETL Only:**
```bash
python etl_pipeline.py --mode full
```

**Run Tests:**
```bash
python tests/test_all_features.py
```

**Clean Database:**
```bash
python cleanup_database.py
```

**Start Scheduler:**
```bash
python scheduler.py
```

### Typical Workflows

**Initial Setup:**
1. `python cleanup_database.py` (if needed)
2. `python run_full_pipeline.py`
3. Verify in PostgreSQL with verification_queries.sql

**Daily Operations:**
1. `python scheduler.py` (keeps running)
2. Or schedule with cron/Task Scheduler

**After Schema Changes:**
1. `python cleanup_database.py`
2. `python run_full_pipeline.py`

---

## SQL Migrations

### Migration System

**Runner:** `sql_commands/run_migrations.py`

**Folders:**
1. `dim_tables/` - V1, V2, V3
2. `fact_tables/` - V1, V2
3. `materialized_views/` - V1

**Naming:** `V{number}__{description}.sql`

**Execution:** Alphabetical order within each folder

### Adding New Migrations

1. Create file: `V{next_number}__{description}.sql`
2. Place in appropriate folder
3. Run: `python sql_commands/run_migrations.py`

**Best Practices:**
- Make migrations idempotent (use IF NOT EXISTS)
- Include rollback instructions
- Test on development database first

---

## Testing

### Test Coverage

**6 Test Categories:**
1. Database connectivity
2. Partitioning functionality
3. Materialized views
4. Caching layer
5. Query performance
6. Data integrity

**Expected Results:**
- All 6 tests passing (100%)
- No orphaned records
- Performance improvements verified

### Running Tests

```bash
# Run all tests
python tests/test_all_features.py

# Expected output:
# [PASS] | Database Connection
# [PASS] | Table Partitioning
# [PASS] | Materialized Views
# [PASS] | Caching Layer
# [PASS] | Query Performance
# [PASS] | Data Integrity
# OVERALL: 6/6 tests passed (100.0%)
```

---

## Monitoring

### Batch Execution Monitoring

**Query:**
```sql
SELECT * FROM retail_dwh.meta_etl_batch_log
ORDER BY batch_start DESC LIMIT 10;
```

**Metrics:**
- Batch ID
- Start/end time
- Status (SUCCESS/FAILED)
- Rows extracted, loaded, quarantined
- Error messages

### Data Quality Monitoring

**Query:**
```sql
SELECT * FROM retail_dwh.dq_metrics
ORDER BY execution_timestamp DESC LIMIT 20;
```

**Metrics:**
- Rule name and category
- Rows processed/passed/quarantined/dropped
- Execution timestamp

### Quarantine Analysis

**Query:**
```sql
SELECT dq_reason, COUNT(*) 
FROM retail_dwh.dq_quarantine_sales
GROUP BY dq_reason
ORDER BY COUNT(*) DESC;
```

---

## Performance

### Performance Features

1. **Table Partitioning**
   - 26 monthly partitions
   - Partition pruning enabled
   - 3-5x faster for date-range queries

2. **Materialized Views**
   - 6 pre-computed aggregates
   - 500-1000x faster than fact table queries
   - Refresh on demand

3. **Indexing**
   - Primary keys on all tables
   - Foreign key indexes on fact table
   - Partition-local indexes

4. **Caching**
   - Redis-based caching
   - Configurable TTL
   - Pattern-based clearing

### Performance Benchmarks

**Fact Table Query:**
- Direct query: ~2000ms
- With partition pruning: ~600ms
- Materialized view: ~2ms

**Data Load:**
- 534,756 records: ~90 seconds
- Full pipeline: ~4-5 minutes

---

## Troubleshooting

### Common Issues

**1. "No such file or directory: online_retail.csv"**
- Ensure CSV file is in `data/raw/online_retail.csv`

**2. "Connection refused" (PostgreSQL)**
- Check PostgreSQL is running
- Verify credentials in `config/secrets.yaml`
- Check firewall settings

**3. "Transaction aborted"**
- Usually from previous failed transaction
- Run: `python cleanup_database.py`
- Then: `python run_full_pipeline.py`

**4. "Tests failing"**
- Ensure full pipeline ran successfully
- Check database has all 34 tables and 6 MVs
- Run verification queries

### Debug Mode

Enable detailed logging:
```python
logging.basicConfig(level=logging.DEBUG)
```

---

## Future Enhancements

### Planned Features
1. Incremental ETL mode
2. Change data capture (CDC)
3. Data lineage tracking UI
4. Real-time streaming
5. Advanced ML features
6. Cloud deployment (Azure/AWS)
7. Apache Airflow integration
8. dbt transformation layer

---

## License

MIT License - See LICENSE file

---

## Contact & Support

For issues or questions:
1. Check this documentation
2. Run verification queries
3. Check logs in `meta_etl_batch_log`
4. Review quarantine table for data issues

---

*Last Updated: January 2026*
*Version: 1.0*
