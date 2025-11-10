"""FMCSA carrier data ingestion module."""
# TODO: Implement FMCSA monthly snapshot download
# Expected CSV columns: dot_number, legal_name, dba, address, power_units, status, phone
# Download from: https://ai.fmcsa.dot.gov/SMS/Tools/download.aspx
# Process: download monthly CSV, extract fields, normalize addresses, geocode, merge with entities

import logging
from typing import Optional
import pandas as pd
from pathlib import Path
import duckdb
from src.config import settings

logger = logging.getLogger(__name__)


def ingest_fmcsa(file_path: Optional[str] = None) -> pd.DataFrame:
    """
    Ingest FMCSA carrier data from CSV snapshot.
    
    Args:
        file_path: Path to FMCSA CSV file (uses config if not provided)
    
    Returns:
        Standardized DataFrame with FMCSA carrier data
    """
    file_path = file_path or settings.fmcsa_snapshot_path
    
    if not Path(file_path).exists():
        logger.warning(f"FMCSA file not found: {file_path}, returning empty DataFrame")
        return pd.DataFrame()
    
    logger.info(f"Starting FMCSA ingestion from {file_path}")
    
    # Read CSV
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception as e:
        logger.error(f"Error reading FMCSA file: {e}")
        return pd.DataFrame()
    
    if df.empty:
        logger.warning("FMCSA file is empty")
        return pd.DataFrame()
    
    logger.info(f"Loaded {len(df)} rows from FMCSA file")
    
    # Map columns (fuzzy matching for common variations)
    column_map = {
        "dot_number": ["DOT_NUMBER", "DOT_NUM", "DOT", "dot_number"],
        "legal_name": ["LEGAL_NAME", "LEGAL NAME", "CARRIER_NAME", "legal_name"],
        "dba_name": ["DBA_NAME", "DBA", "DBA NAME", "dba_name"],
        "phone": ["PHONE", "PHONE_NUMBER", "PHONE_NUM", "phone"],
        "address": ["ADDRESS", "STREET", "STREET_ADDRESS", "address"],
        "city": ["CITY", "city"],
        "state": ["STATE", "state"],
        "zip": ["ZIP", "ZIP_CODE", "ZIPCODE", "zip"],
        "power_units": ["POWER_UNITS", "POWER_UNIT", "POWER", "power_units"],
        "drivers": ["DRIVERS", "DRIVER_COUNT", "drivers"],
        "operating_status": ["OPERATING_STATUS", "STATUS", "OPERATING", "operating_status"],
        "hazmat_flag": ["HAZMAT", "HAZMAT_FLAG", "HAZMAT_FLG", "hazmat_flag"]
    }
    
    result_data = []
    for idx, row in df.iterrows():
        # Extract fields with fuzzy matching
        dot_number = None
        for col in column_map.get("dot_number", []):
            if col in df.columns:
                dot_number = row.get(col)
                break
        
        legal_name = None
        for col in column_map.get("legal_name", []):
            if col in df.columns:
                legal_name = row.get(col)
                break
        
        dba_name = None
        for col in column_map.get("dba_name", []):
            if col in df.columns:
                dba_name = row.get(col)
                break
        
        phone = None
        for col in column_map.get("phone", []):
            if col in df.columns:
                phone = row.get(col)
                break
        
        address = None
        for col in column_map.get("address", []):
            if col in df.columns:
                address = row.get(col)
                break
        
        city = None
        for col in column_map.get("city", []):
            if col in df.columns:
                city = row.get(col)
                break
        
        state = None
        for col in column_map.get("state", []):
            if col in df.columns:
                state = row.get(col)
                break
        
        zip_code = None
        for col in column_map.get("zip", []):
            if col in df.columns:
                zip_code = row.get(col)
                break
        
        power_units = None
        for col in column_map.get("power_units", []):
            if col in df.columns:
                power_units = row.get(col)
                break
        
        drivers = None
        for col in column_map.get("drivers", []):
            if col in df.columns:
                drivers = row.get(col)
                break
        
        operating_status = None
        for col in column_map.get("operating_status", []):
            if col in df.columns:
                operating_status = row.get(col)
                break
        
        hazmat_flag = False
        for col in column_map.get("hazmat_flag", []):
            if col in df.columns:
                hazmat_val = row.get(col)
                hazmat_flag = str(hazmat_val).upper() in ["Y", "YES", "TRUE", "1"]
                break
        
        # Clean and normalize
        dot_number = str(dot_number).strip() if pd.notna(dot_number) else None
        legal_name = str(legal_name).strip() if pd.notna(legal_name) else None
        dba_name = str(dba_name).strip() if pd.notna(dba_name) else None
        phone = str(phone).strip() if pd.notna(phone) else None
        address = str(address).strip() if pd.notna(address) else None
        city = str(city).strip() if pd.notna(city) else None
        state = str(state).strip() if pd.notna(state) else None
        zip_code = str(zip_code).strip() if pd.notna(zip_code) else None
        
        # Convert numeric fields
        try:
            power_units = int(power_units) if pd.notna(power_units) else None
        except (ValueError, TypeError):
            power_units = None
        
        try:
            drivers = int(drivers) if pd.notna(drivers) else None
        except (ValueError, TypeError):
            drivers = None
        
        operating_status = str(operating_status).strip() if pd.notna(operating_status) else None
        
        result_data.append({
            "dot_number": dot_number,
            "legal_name": legal_name,
            "dba_name": dba_name,
            "phone": phone,
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "power_units": power_units,
            "drivers": drivers,
            "status": operating_status,
            "hazmat_flag": hazmat_flag,
            "source": "fmcsa"
        })
    
    result_df = pd.DataFrame(result_data)
    logger.info(f"Processed {len(result_df)} FMCSA records")
    
    # Cache to parquet
    settings.cache_fmcsa_dir.mkdir(parents=True, exist_ok=True)
    cache_path = settings.cache_fmcsa_dir / "fmcsa_normalized.parquet"
    result_df.to_parquet(cache_path, index=False)
    logger.info(f"Cached to {cache_path}")
    
    # Persist to DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_fmcsa (
            dot_number VARCHAR,
            legal_name VARCHAR,
            dba_name VARCHAR,
            phone VARCHAR,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip VARCHAR,
            power_units INTEGER,
            drivers INTEGER,
            status VARCHAR,
            hazmat_flag BOOLEAN,
            source VARCHAR
        )
    """)
    
    conn.register('result_df', result_df)
    conn.execute("DROP TABLE IF EXISTS raw_fmcsa")
    conn.execute("CREATE TABLE raw_fmcsa AS SELECT * FROM result_df")
    conn.close()
    
    logger.info(f"Persisted {len(result_df)} rows to DuckDB table raw_fmcsa")
    
    return result_df
