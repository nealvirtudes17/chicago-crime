import argparse
import sys
import logging
from app.database import init_db
# Import the logic functions directly (refactor scripts to export functions)
from scripts.run_backfill import main as run_backfill_task
from scripts.run_daily import main as run_daily_task

# Configure logging for the entry point
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Chicago Crime Data Pipeline CLI")
    
    # Define arguments
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=["init", "backfill", "daily"], 
        required=True,
        help="Select operation mode: 'init' (DB setup), 'backfill' (Historical), or 'daily' (Incremental)"
    )

    args = parser.parse_args()

    try:
        if args.mode == "init":
            logger.info("Initializing Database Schema...")
            init_db()
            logger.info("Database initialized successfully.")

        elif args.mode == "backfill":
            logger.info("Triggering Historical Backfill...")
            run_backfill_task()

        elif args.mode == "daily":
            logger.info("Triggering Daily Incremental Load...")
            run_daily_task()

    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()