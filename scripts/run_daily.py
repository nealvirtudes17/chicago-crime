import sys
import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import func, select
from sqlalchemy.orm import Session

# --- STEP 0: ENVIRONMENT SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from app.database import engine
from app.models import CrimeRecord 
from app.services.api_client import fetch_crime_data
from app.services.etl import clean_data, load_data_bulk

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_last_crime_date() -> datetime | None:
    """
    Queries the database for the most recent crime timestamp.
    Returns None if the database is empty.
    """
    stmt = select(func.max(CrimeRecord.date))
    with Session(engine) as session:
        return session.scalar(stmt)

def main():
    logger.info("=== STARTING DAILY INCREMENTAL JOB ===")

    try:
        # STEP 1: Determine the Checkpoint
        last_date = get_last_crime_date()

        if last_date:
            # Happy Path: Increment by 1 second
            start_date = last_date + timedelta(seconds=1)
            logger.info(f"Last recorded crime: {last_date}")
            logger.info(f"Fetching data starting from: {start_date}")
        else:
            # STOP: Strict Guardrail
            # We prevent implicit backfills to ensure data integrity
            error_msg = (
                "CRITICAL: Database is empty. Cannot run incremental daily job. "
                "You must run 'run_backfill.py' first to populate historical data."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # STEP 2: Extract (API)
        raw_df = fetch_crime_data(start_date=start_date, limit=500_000)

        if raw_df.empty:
            logger.info("No new data found. Job finished.")
            return

        # STEP 3: Transform (Clean)
        logger.info(f"Cleaning {len(raw_df)} records...")
        clean_df = clean_data(raw_df)

        # STEP 4: Load (Database)
        if not clean_df.empty:
            logger.info("Saving to database...")
            load_data_bulk(clean_df)
            logger.info("=== DAILY JOB SUCCESSFUL ===")
        else:
            logger.warning("Data fetched but all records were filtered during cleaning.")

    except Exception as e:
        logger.error(f"Daily Job Failed: {e}")
        raise

if __name__ == "__main__":
    # 1. Capture BEFORE state
    before_date = get_last_crime_date()
    print(f"\n[DIAGNOSTIC] DB Date BEFORE run: {before_date}")

    # 2. Run the actual ETL process
    main()

    # 3. Capture AFTER state
    after_date = get_last_crime_date()
    print(f"[DIAGNOSTIC] DB Date AFTER run:  {after_date}")