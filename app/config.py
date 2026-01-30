import os
from pathlib import Path
from dotenv import load_dotenv

# This anchors everything to "chicago-crime/" no matter where scripts run
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"

# STEP 2: Load the .env file
load_dotenv(dotenv_path=ENV_PATH)

class Config:
    """Production configuration loader."""

    # --- DATABASE CONFIGURATION FOR SQLITE AT ROOT---
    
    # 1. Get the raw string from .env (e.g., "sqlite:///chicago_crime.db")
    _raw_url = os.getenv("DATABASE_URL")
    
    if not _raw_url:
        raise ValueError("Critical: DATABASE_URL is missing in .env file.")

    # STEP 3: THE FIX - Intercept Relative SQLite Paths
    if _raw_url.startswith("sqlite:///"):
        # Strip the prefix to get just the filename (e.g. "chicago_crime.db")
        filename = _raw_url.replace("sqlite:///", "")
        
        # If the path is not absolute (doesn't start with / or C:\), 
        # we FORCE it to be at the Project Root.
        if not Path(filename).is_absolute():
            # Create the absolute path: /Users/You/chicago-crime/chicago_crime.db
            abs_path = BASE_DIR / filename
            DB_URL = f"sqlite:///{abs_path}"
        else:
            DB_URL = _raw_url
    else:
        # For Postgres/MySQL, use as-is
        DB_URL = _raw_url

    # --- API Credentials ---
    API_TOKEN: str = os.environ.get("SOCRATA_APP_TOKEN", "")
    API_USER: str = os.environ.get("SOCRATA_USERNAME", "")
    API_PASS: str = os.environ.get("SOCRATA_PASSWORD", "")

    @classmethod
    def validate(cls):
        missing = [k for k, v in {
            "SOCRATA_APP_TOKEN": cls.API_TOKEN,
            "SOCRATA_USERNAME": cls.API_USER,
            "SOCRATA_PASSWORD": cls.API_PASS
        }.items() if not v]
        
        if missing:
            raise EnvironmentError(f"Missing secrets in .env: {', '.join(missing)}")

Config.validate()

# DEBUG: Print this when you run to confirm it's fixed
if __name__ == "__main__":
    print(f"Project Root: {BASE_DIR}")
    print(f"Database URL: {Config.DB_URL}")