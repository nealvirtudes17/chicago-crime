import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import make_url
from app.config import Config

# 1. Setup Logging (Standard for Production)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 2. Engine Configuration
engine = create_engine(
    Config.DB_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False  # Keep False in prod to reduce log noise
)

# 3. Base Model
class Base(DeclarativeBase):
    pass

#Not best practice devops team should create the db ahead and you just connect
def _create_database_if_not_exists():
    """
    Private helper: Connects to MySQL root to create the DB if missing.
    """
    # Parse the URL to get the DB name (e.g., 'chicago_crime')
    url = make_url(Config.DB_URL)
    target_db = url.database
    
    # Validation: If using SQLite, we skip this (it auto-creates)
    if url.drivername.startswith("sqlite"):
        return

    # Create a temporary 'root' connection URL (connect to 'mysql' system db or no db)
    # We use isolation_level="AUTOCOMMIT" because CREATE DATABASE cannot run inside a transaction
    root_url = url.set(database="mysql")
    
    tmp_engine = create_engine(root_url, isolation_level="AUTOCOMMIT")
    
    try:
        with tmp_engine.connect() as conn:
            # Check if database exists strictly before creating
            # Note: "CREATE DATABASE IF NOT EXISTS" is easier, but explicit checks are cleaner for logging
            logger.info(f"Checking for database: {target_db}...")
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {target_db}"))
            logger.info(f"Database '{target_db}' is ready.")
    except Exception as e:
        logger.error(f"Could not bootstrap database: {e}")
        raise
    finally:
        tmp_engine.dispose()

def init_db():
    """
    Initializes the database schema using SQLAlchemy 2.0 strict standards.
    """
    try:

        _create_database_if_not_exists()

        # Step 1: Register Models
        # Must import here so Base.metadata populates with your tables
        import app.models 
        
        # Step 2: Transactional Schema Creation
        # 'engine.begin()' is the 2.0 standard. It automatically:
        #  - Opens a connection
        #  - Begins a transaction
        #  - Commits if successful OR Rolls back if error
        #  - Closes the connection (releasing file locks)
        with engine.begin() as conn:
            Base.metadata.create_all(bind=conn)
            
        logger.info("Database initialized successfully.")

    except SQLAlchemyError as e:
        # Step 3: Specific Error Handling
        # Catches connection errors, syntax errors, or locking issues.
        logger.error(f"Failed to initialize database: {e}")
        raise  # Re-raise so the script (like run_backfill.py) knows to stop.
    except Exception as e:
        # Catch-all for non-DB errors (like ImportError if models.py is broken)
        logger.error(f"Unexpected system error: {e}")
        raise