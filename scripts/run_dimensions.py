import sys
import os
import pandas as pd
import logging
from sodapy import Socrata
from sqlalchemy import text
from sqlalchemy.orm import Session

# --- SETUP PATHS ---
# Ensures we can import from 'app' regardless of where the script is run
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from app.config import Config
from app.database import engine, init_db
from app.models import CommunityArea, IUCR, Ward, Beat, District

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
# strict 2.0 mapping: Model -> { dataset_id, col_mapping }
# col_mapping format: { 'API_FIELD_NAME': 'YOUR_MODEL_FIELD_NAME' }

DIMENSION_CONFIG = {
    CommunityArea: {
        "dataset_id": "igwz-8jzy",
        "mapping": {
            "area_num_1": "id", 
            "community": "name"
        }
    },
    IUCR: {
        "dataset_id": "c7ck-438e",
        "mapping": {
            "iucr": "id", 
            "primary_description": "primary_desc", 
            "secondary_description": "secondary_desc",
            "index_code": "index_code",
            "active": "is_active"  # Mapping API 'active' -> Model 'is_active'
        }
    },
    Ward: {
        "dataset_id": "k9yb-bpqx",
        "mapping": {
            "ward": "id"
        }
    },
    Beat: {
        "dataset_id": "n9it-hstw",
        "mapping": {
            "beat_num": "beat_num", # PK
            "district": "district", 
            "sector": "sector", 
            "beat": "beat"
        }
    },
    District: {
        "dataset_id": "24zt-jpfn",
        "mapping": {
            "dist_num": "dist_num",   # PK
            "dist_label": "dist_label"
        }
    }
}

def fetch_and_load_dimension(session: Session, client: Socrata, model, config: dict):
    dataset_id = config["dataset_id"]
    mapping = config["mapping"]
    table_name = model.__tablename__

    logger.info(f"--- Processing {table_name} ---")
    
    try:
        # 1. Fetch from API (Dimensions are small, usually < 3000 rows)
        results = client.get(dataset_id, limit=5000)
        df = pd.DataFrame.from_records(results)

        if df.empty:
            logger.warning(f"No data returned for {table_name}")
            return

        # 2. Rename Columns (API Name -> Model Name)
        df = df.rename(columns=mapping)
        
        # 3. Filter Columns (Keep ONLY what is in our Model)
        # This automatically drops 'the_geom' or any other junk columns
        target_cols = list(mapping.values())
        
        # Ensure all target columns exist (fill missing with None)
        for col in target_cols:
            if col not in df.columns:
                df[col] = None
                
        df = df[target_cols]

        # --- NEW: DEDUPLICATION LOGIC ---
        # Automatically find the Primary Key of the model
        pk_column = model.__table__.primary_key.columns.keys()[0] # e.g., 'beat_num' or 'id'
        
        initial_count = len(df)
        # Keep the first occurrence, drop the rest
        df = df.drop_duplicates(subset=[pk_column], keep='first')
        
        if len(df) < initial_count:
            logger.warning(f"⚠️ Dropped {initial_count - len(df)} duplicate rows from {table_name} (Duplicate PKs)")
        # --------------------------------


        # 2. Standardize Districts/Beats to match Crime Records
        if table_name == "dim_districts":
            # Force "1" -> "001" to match standard Crime Data
            # If your crime data is "1", remove the .zfill(3) part.
            df['dist_num'] = df['dist_num'].astype(str).str.split('.').str[0].str.zfill(3)
            
        if table_name == "dim_beats":
            # Force "123" -> "0123" (Beats are often 4 digits in some systems)
            # Usually safe to leave as-is, but strip decimals
            df['beat_num'] = df['beat_num'].astype(str).str.split('.').str[0]
        # ----------------------------

        # 4. Clean Data
        # Convert NaN to None for SQL NULL compatibility
        df = df.where(pd.notnull(df), None)
        
        # Special handling for Booleans (like IUCR is_active)
        if "is_active" in df.columns:
             df["is_active"] = df["is_active"].astype(str).str.lower() == "true"

        # 5. Truncate & Load
        # We delete all existing rows to ensure we match the source of truth
        session.execute(text(f"DELETE FROM {table_name}"))
        
        records = df.to_dict(orient="records")
        session.bulk_insert_mappings(model, records)
        logger.info(f"✅ Loaded {len(records)} rows into {table_name}")

    except Exception as e:
        logger.error(f"❌ Failed to load {table_name}: {e}")
        raise

def main():
    logger.info("=== STARTING DIMENSION ETL ===")
    
    # Initialize Client
    client = Socrata(
        "data.cityofchicago.org",
        Config.API_TOKEN,
        username=Config.API_USER,
        password=Config.API_PASS,
        timeout=600
    )

    # Ensure Tables Exist
    init_db()

    with Session(engine) as session:
        try:
            for model_cls, config in DIMENSION_CONFIG.items():
                fetch_and_load_dimension(session, client, model_cls, config)
            
            session.commit()
            logger.info("=== DIMENSION ETL COMPLETE: SUCCESS ===")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Critical Transaction Failure: {e}")
            sys.exit(1)
        finally:
            client.close()

if __name__ == "__main__":
    main()