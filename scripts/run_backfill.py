import sys
import os
import logging
from datetime import datetime
from sqlalchemy import func, select
from sqlalchemy.orm import Session


# 1. Get the path to the current file (run_backfill.py)
current_dir = os.path.dirname(os.path.abspath(__file__))
print(current_dir)
# 2. Get the parent directory (chicago-crime root)
parent_dir = os.path.dirname(current_dir)
print(parent_dir)
# 3. Add the parent directory to sys.path
sys.path.append(parent_dir)
# --- CRITICAL FIX END ---

from app.database import engine,init_db
from app.models import CrimeRecord
from app.services.api_client import fetch_crime_data
from app.services.etl import clean_data, load_data_bulk

# Configure Logging (Will be overridden by main.py if run from there)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def is_database_empty() -> bool:
    """
    Returns True if the CrimeRecord table has 0 rows.
    """
    with Session(engine) as session:
        # 2.0 Syntax: standard count query
        # efficient: SELECT count(id) from crime_records
        count = session.scalar(select(func.count(CrimeRecord.id)))
        return count == 0

def main():
    logger.info("=== STARTING HISTORICAL BACKFILL ===")

    # [Step 1] Init Schema (Safe to run multiple times)
    logger.info("Verifying Schema...")
    init_db()
    
    # [Step 2] Run Safety Check 
    if not is_database_empty():
        logger.error("ABORTING: Data detected in database.")
        raise RuntimeError("Database must be empty to run backfill.")

    # [Step 3] Configuration
    start_date = datetime(2001, 1, 1)
    
    # [Step 4] Extract (API)
    logger.info(f"Fetching backfill data starting from {start_date.date()}...")
    try:
       
        raw_df = fetch_crime_data(start_date=start_date, limit=2_000_000)
    except Exception as e:
        logger.error(f"API Failed: {e}")
        raise

    if raw_df.empty:
        logger.warning("No data returned from API.")
        return

    # [Step 6] Transform (Clean)
    logger.info("Cleaning data...")
    try:
        clean_df = clean_data(raw_df)
    except Exception as e:
        logger.error(f'Cleaning failed! Due to {e}')
    
    # [Step 7] Load (Database)
    if not clean_df.empty:
        logger.info("Saving to Database...")
        load_data_bulk(clean_df)
        logger.info("=== BACKFILL COMPLETE ===")
    else:
        logger.warning("All data filtered out during cleaning.")

if __name__ == "__main__":
    main()