# Retail Sales Data Engineering Project

A production-ready data warehouse for retail sales analytics with automated ETL pipeline, data quality monitoring, and performance optimizations.

## Overview

This project implements an end-to-end data engineering solution for retail sales data, featuring:

- Automated ETL pipeline with data quality validation
- Star schema data warehouse design
- Table partitioning for optimized query performance
- Materialized views for pre-computed analytics
- Data quality monitoring and quarantine system
- Batch tracking and data lineage
- Caching layer for frequently accessed data

## Tech Stack

- **Database**: PostgreSQL 15+
- **Language**: Python 3.12
- **Libraries**: pandas, psycopg2, pyyaml, schedule, redis
- **Architecture**: Star schema with dimension and fact tables

## Project Structure

```
retail-sales-de/
├── config/
│   ├── config.yaml              # Application configuration
│   ├── data_quality_rules.yaml  # Data quality rules
│   └── secrets.yaml             # Database credentials
├── data/
│   └── raw/
│       └── online_retail.csv    # Source data
├── handlers/
│   ├── db_manager.py            # Database operations
│   ├── data_processor.py        # Data processing utilities
│   └── cache_manager.py         # Caching layer
├── sql_commands/
│   ├── dim_tables/              # Dimension table migrations
│   ├── fact_tables/             # Fact table migrations
│   └── materialized_views/      # Aggregate views
├── tests/
│   └── test_all_features.py     # Comprehensive test suite
├── etl_pipeline.py              # Main ETL pipeline
├── run_full_pipeline.py         # Full pipeline orchestrator
├── scheduler.py                 # ETL scheduler
└── requirements.txt             # Python dependencies
```

## Data Warehouse Schema

### Star Schema Design

**Dimensions:**
- `dim_date` - Date dimension with fiscal attributes
- `dim_product` - Product master with pricing statistics
- `dim_customer` - Customer dimension with segmentation

**Facts:**
- `fct_retail_sales` - Partitioned sales transactions (534,756 records)
  - 26 monthly partitions (2009-12 to 2011-12)

**Materialized Views:**
- `mv_monthly_sales_summary` - Monthly aggregates
- `mv_top_products` - Product performance
- `mv_customer_segments` - RFM analysis
- `mv_daily_sales_trend` - Daily trends with moving averages
- `mv_country_performance` - Geographic analysis
- `mv_product_category_analysis` - Category insights

## Setup

### Prerequisites

- Python 3.12+
- PostgreSQL 15+
- Git

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd retail-sales-de
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure database connection in `config/secrets.yaml`:
```yaml
database:
  host: localhost
  port: 5432
  database: your_database
  user: your_user
  password: your_password
```

4. Place source data file in `data/raw/online_retail.csv`

## Usage

### Full Pipeline Execution

Run the complete pipeline (staging, dimensions, facts, materialized views):

```bash
python run_full_pipeline.py
```

This will:
1. Load data into staging tables
2. Execute all SQL migrations
3. Create dimensions and fact tables
4. Build materialized views
5. Run comprehensive tests

### Individual Components

**ETL Pipeline Only:**
```bash
python etl_pipeline.py
```

**Run Tests:**
```bash
python tests/test_all_features.py
```

**Schedule ETL:**
```bash
python scheduler.py
```

**Clean Database:**
```bash
python cleanup_database.py
```

## Features

### Data Quality

- 17 validation rules across 4 categories
- Automatic quarantine of invalid records
- Metrics tracking for each batch
- Referential integrity enforcement

### Performance Optimizations

- **Partitioning**: 26 monthly partitions for faster queries
- **Materialized Views**: Pre-computed aggregates
- **Indexing**: Foreign keys and partition-local indexes
- **Caching**: Redis-based caching with memory fallback

### Data Lineage

- Batch tracking with unique IDs
- Execution metadata logging
- Historical batch analysis
- Source-to-target traceability

## Testing

Comprehensive test suite covering:
- Database connectivity
- Table partitioning
- Materialized views
- Caching layer
- Query performance
- Data integrity

Run tests:
```bash
python tests/test_all_features.py
```

Expected output: 6/6 tests passing (100%)

## Database Queries

### Check Tables
```sql
SELECT * FROM pg_tables WHERE schemaname='retail_dwh';
```

### Check Materialized Views
```sql
SELECT * FROM pg_matviews WHERE schemaname='retail_dwh';
```

### Query Sales Data
```sql
SELECT COUNT(*) FROM retail_dwh.fct_retail_sales;
```

### Monthly Sales Summary
```sql
SELECT * FROM retail_dwh.mv_monthly_sales_summary;
```

### Top Products
```sql
SELECT * FROM retail_dwh.mv_top_products LIMIT 10;
```

## Data Quality Monitoring

### View Metrics
```sql
SELECT * FROM retail_dwh.dq_metrics 
ORDER BY execution_timestamp DESC;
```

### View Quarantined Records
```sql
SELECT dq_reason, COUNT(*) 
FROM retail_dwh.dq_quarantine_sales 
GROUP BY dq_reason;
```

### Batch History
```sql
SELECT * FROM retail_dwh.meta_etl_batch_log 
ORDER BY batch_start DESC;
```

## Configuration

### Data Quality Rules

Edit `config/data_quality_rules.yaml` to customize validation rules:

```yaml
data_quality_rules:
  missing_values:
    - name: check_missing_invoice_no
      column: InvoiceNo
      action: quarantine
```

### ETL Schedule

Edit `scheduler.py` to configure execution frequency:

```python
schedule.every().day.at("08:00").do(scheduled_etl_job)
schedule.every().hour.do(scheduled_etl_job)
schedule.every(30).minutes.do(scheduled_etl_job)
```

## Performance

- **Data Volume**: 541,909 source rows
- **Valid Records**: 534,756 fact records
- **Pipeline Execution**: ~3-4 minutes full rebuild
- **Query Performance**: 500-1000x improvement with materialized views
- **Partitions**: 26 monthly partitions for optimized range queries

## Requirements Met

- Data ingestion from CSV
- Data cleaning and validation (17 rules)
- Data transformation and type conversion
- Star schema data warehouse
- Automated ETL pipeline with scheduling
- Table partitioning and indexing
- Data quality monitoring with alerts
- Data versioning and batch tracking
- Metadata management
- Performance optimization (MVs, caching, partitioning)

## License

MIT License

## Author

Data Engineering Project - 2026
