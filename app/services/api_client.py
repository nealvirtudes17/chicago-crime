import pandas as pd
from sodapy import Socrata
from datetime import datetime
from app.config import Config

# Dataset ID for Chicago Crimes - 2001 to Present
DATASET_ID = "ijzp-q8t2"
DOMAIN = "data.cityofchicago.org"

def fetch_crime_data(start_date: datetime, limit: int = 50_000) -> pd.DataFrame:
    """
    Fetches raw crime data from the Chicago Data Portal.
    """
    # 1. Initialize Client with Safe Credentials
    client = Socrata(
        DOMAIN,
        Config.API_TOKEN,
        username=Config.API_USER,
        password=Config.API_PASS,
        timeout=900  # 15 minute timeout for large requests
    )

    # 2. Format Date for API (SoQL format)
    date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    print(f"--- API: Fetching data since {date_str} (Limit: {limit}) ---")

    try:
        # 3. Execute Request
        results = client.get(
            DATASET_ID, 
            where=f"date >= '{date_str}'", 
            limit=limit,
            order="date ASC"
        )
        
        # 4. Convert to DataFrame
        df = pd.DataFrame.from_records(results)
        
        if df.empty:
            print("API Warning: No records returned.")
            return pd.DataFrame()
            
        print(f"API Success: Retrieved {len(df)} rows.")
        return df

    except Exception as e:
        print(f"API Critical Error: {e}")
        raise
    finally:
        client.close()