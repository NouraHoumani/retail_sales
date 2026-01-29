import schedule
import time
import logging
from datetime import datetime
from etl_pipeline import run_etl_pipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

def scheduled_etl_job():
    
    log.info("="*70)
    log.info(f"SCHEDULED ETL EXECUTION TRIGGERED")
    log.info("="*70)
    
    try:
        success = run_etl_pipeline()
        if success:
            log.info(" Scheduled ETL completed successfully")
        else:
            log.error(" Scheduled ETL failed")
    except Exception as e:
        log.error(f" Scheduled ETL error: {e}")

def main():
    
    
    log.info("\n" + "="*70)
    log.info("ETL SCHEDULER STARTED")
    log.info("="*70)
    log.info("Schedule: Daily at 08:00")
    log.info("Press Ctrl+C to stop")
    log.info("="*70 + "\n")
    
    
    schedule.every().day.at("08:00").do(scheduled_etl_job)
    
   
    log.info("Running ETL immediately (initial run)...")
    scheduled_etl_job()
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)  

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("\n\n  Scheduler stopped by user")