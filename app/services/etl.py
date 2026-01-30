import pandas as pd
import numpy as np
from sqlalchemy import insert
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.database import engine
from app.models import CrimeRecord

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    string_cols = [
        'case_number', 'block', 'iucr', 'primary_type', 
        'description', 'location_description', 'beat', 
        'district', 'community_area', 'fbi_code'
    ]
    
    return (
        df
        .pipe(lambda _df: _df.rename(columns=str.lower))
        .assign(
            # 1. Datetimes
            date=lambda x: pd.to_datetime(x['date'], errors='coerce'),
            updated_on=lambda x: pd.to_datetime(x['updated_on'], errors='coerce'),
            
            # 2. Coordinates: Float32 provides precision while saving memory
            latitude=lambda x: pd.to_numeric(x['latitude'], errors='coerce').astype('Float32'),
            longitude=lambda x: pd.to_numeric(x['longitude'], errors='coerce').astype('Float32'),
            x_coordinate=lambda x: pd.to_numeric(x['x_coordinate'], errors='coerce').astype('Float32'),
            y_coordinate=lambda x: pd.to_numeric(x['y_coordinate'], errors='coerce').astype('Float32'),
            
            # 3. Administrative IDs: Use Nullable Integers to handle missing values
            year=lambda x: pd.to_numeric(x['year'], errors='coerce').astype('Int16'),
            ward=lambda x: pd.to_numeric(x['ward'], errors='coerce').astype('Int16'),
            id=lambda x: pd.to_numeric(x['id'], errors='coerce').astype('Int64'),
            
            # 4. Nullable Booleans
            arrest=lambda x: x['arrest'].astype('boolean'),
            domestic=lambda x: x['domestic'].astype('boolean'),
            
            # 5. Optimized Strings
            **{col: lambda x, c=col: x[c].astype('string') for col in string_cols}
        )
    )

def load_data_bulk(df: pd.DataFrame):
    """
    High-performance Bulk Insert for Backfilling.
    """
    if df.empty:
        print("No data to load.")
        return

    # Convert NaT/NaN to None for SQL compatibility
    clean_df = df.replace({np.nan: None})
    records = clean_df.to_dict(orient="records")

    with Session(engine) as session:
        try:
            print(f"Inserting {len(records)} records...")
            # Core SQLAlchemy 2.0 Insert
            session.execute(insert(CrimeRecord), records)
            session.commit()
            print("Commit successful.")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error loading data: {e}")
            raise