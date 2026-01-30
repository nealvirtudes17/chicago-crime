from datetime import datetime
from typing import Optional
from sqlalchemy import String, Integer, Float, Boolean, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base

class CrimeRecord(Base):
    __tablename__ = "crime_records"

    # 1. Primary Key: We turn OFF autoincrement because we want to use 
    # the existing unique 'id' provided by the Chicago Data Portal.
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)

    # 2. Identifiers
    # Case Number is the business key. Unique and indexed for fast lookups.
    case_number: Mapped[str] = mapped_column(String(20), index=True)
    
    # 3. Timestamps
    # 'date' is a reserved keyword in some SQL dialects, but SQLAlchemy handles escaping.
    date: Mapped[datetime] = mapped_column(DateTime, index=True)
    updated_on: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # 4. Categorical / Text Data
    block: Mapped[Optional[str]] = mapped_column(String(100))
    iucr: Mapped[Optional[str]] = mapped_column(String(10))
    primary_type: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    description: Mapped[Optional[str]] = mapped_column(String(255))
    location_description: Mapped[Optional[str]] = mapped_column(String(100))
    
    # 5. Flags (Checkboxes)
    arrest: Mapped[bool] = mapped_column(Boolean, default=False)
    domestic: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # 6. Jurisdiction Codes
    # These are technically numbers but often treated as codes (Strings) to preserve formatting.
    beat: Mapped[Optional[str]] = mapped_column(String(10))
    district: Mapped[Optional[str]] = mapped_column(String(10))
    ward: Mapped[Optional[int]] = mapped_column(Integer)
    # API schema says "text", so we use String to be safe (even if it looks like a number)
    community_area: Mapped[Optional[str]] = mapped_column(String(10))
    fbi_code: Mapped[Optional[str]] = mapped_column(String(10))
    
    # 7. Spatial Data
    # Used Float for precision. 
    x_coordinate: Mapped[Optional[float]] = mapped_column(Float)
    y_coordinate: Mapped[Optional[float]] = mapped_column(Float)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    
    # 8. Partitioning/Filtering
    year: Mapped[Optional[int]] = mapped_column(Integer, index=True)

    # Note: We omitted the 'location' column (the composite object) 
    # because 'latitude' and 'longitude' already store that data efficiently.