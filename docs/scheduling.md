# ETL Pipeline Scheduling

## Overview
The ETL pipeline can run automatically on a schedule using standard OS scheduling tools.

## Manual Execution

```bash
python etl_pipeline.py --mode full
python etl_pipeline.py --mode incremental
```

## Python Scheduler

The `scheduler.py` script provides a simple scheduling mechanism:

```bash
python scheduler.py
```

Default schedule: Daily at 08:00

Configure schedule in `scheduler.py`:
```python
schedule.every().day.at("08:00").do(scheduled_etl_job)
schedule.every().hour.do(scheduled_etl_job)
schedule.every(30).minutes.do(scheduled_etl_job)
```

## Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (daily, weekly, etc.)
4. Action: Start a program
   - Program: `python.exe`
   - Arguments: `etl_pipeline.py --mode full`
   - Start in: `C:\path\to\retail-sales-de`

## Linux Cron

Edit crontab:
```bash
crontab -e
```

Add schedule:
```
0 8 * * * cd /path/to/retail-sales-de && python etl_pipeline.py --mode full
```

Schedule formats:
- Daily at 8 AM: `0 8 * * *`
- Every hour: `0 * * * *`
- Every 30 minutes: `*/30 * * * *`

## Production Recommendations

For enterprise deployments, consider:

- **Apache Airflow**: Workflow orchestration with DAGs
- **Azure Data Factory**: Cloud-based ETL orchestration
- **AWS Glue**: Managed ETL service
- **Prefect**: Modern workflow orchestration

## Monitoring

Check batch execution history:
```sql
SELECT * FROM retail_dwh.meta_etl_batch_log 
ORDER BY batch_start DESC;
```

Check data quality metrics:
```sql
SELECT * FROM retail_dwh.dq_metrics 
WHERE execution_timestamp > CURRENT_DATE - 7;
```
