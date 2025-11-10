"""EIA generator inventory ingestion module."""
import logging
from typing import Optional
import pandas as pd
from pathlib import Path
import duckdb
from src.config import settings

logger = logging.getLogger(__name__)

# Diesel fuel type mappings
DIESEL_FUEL_TYPES = ["DFO", "Diesel", "Distillate Fuel Oil", "Distillate", "Diesel Fuel Oil"]


def ingest_eia_generators(file_path: Optional[str] = None) -> pd.DataFrame:
    """
    Ingest EIA Form 860 generator inventory data.
    
    Args:
        file_path: Path to EIA Form 860 CSV (uses config if not provided)
    
    Returns:
        Standardized DataFrame with diesel generator facilities
    """
    file_path = file_path or settings.eia_form860_path
    
    if not Path(file_path).exists():
        logger.warning(f"EIA file not found: {file_path}, returning empty DataFrame")
        return pd.DataFrame()
    
    logger.info(f"Starting EIA generator ingestion from {file_path}")
    
    # Read CSV
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception as e:
        logger.error(f"Error reading EIA file: {e}")
        return pd.DataFrame()
    
    if df.empty:
        logger.warning("EIA file is empty")
        return pd.DataFrame()
    
    logger.info(f"Loaded {len(df)} rows from EIA file")
    
    # Map columns (fuzzy matching)
    column_map = {
        "plant_name": ["Plant Name", "PLANT_NAME", "NAME", "plant_name"],
        "address": ["Address", "ADDRESS", "STREET", "address"],
        "city": ["City", "CITY", "city"],
        "state": ["State", "STATE", "state"],
        "zip": ["Zip", "ZIP", "ZIP_CODE", "zip"],
        "fuel_type": ["Fuel Type", "FUEL_TYPE", "FUEL", "fuel_type"],
        "prime_mover": ["Prime Mover", "PRIME_MOVER", "PRIME_MOVER_TYPE", "prime_mover"],
        "nameplate_mw": ["Nameplate Capacity (MW)", "NAMEPLATE_MW", "CAPACITY_MW", "nameplate_mw"]
    }
    
    result_data = []
    for idx, row in df.iterrows():
        # Extract fields
        plant_name = None
        for col in column_map.get("plant_name", []):
            if col in df.columns:
                plant_name = row.get(col)
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
        
        fuel_type = None
        for col in column_map.get("fuel_type", []):
            if col in df.columns:
                fuel_type = row.get(col)
                break
        
        prime_mover = None
        for col in column_map.get("prime_mover", []):
            if col in df.columns:
                prime_mover = row.get(col)
                break
        
        nameplate_mw = None
        for col in column_map.get("nameplate_mw", []):
            if col in df.columns:
                nameplate_mw = row.get(col)
                break
        
        # Filter: only diesel/distillate fuel types
        fuel_type_str = str(fuel_type).upper() if pd.notna(fuel_type) else ""
        is_diesel = any(diesel_type.upper() in fuel_type_str for diesel_type in DIESEL_FUEL_TYPES)
        
        if not is_diesel:
            continue
        
        # Clean and normalize
        plant_name = str(plant_name).strip() if pd.notna(plant_name) else None
        address = str(address).strip() if pd.notna(address) else None
        city = str(city).strip() if pd.notna(city) else None
        state = str(state).strip() if pd.notna(state) else None
        zip_code = str(zip_code).strip() if pd.notna(zip_code) else None
        fuel_type = str(fuel_type).strip() if pd.notna(fuel_type) else None
        prime_mover = str(prime_mover).strip() if pd.notna(prime_mover) else None
        
        try:
            nameplate_mw = float(nameplate_mw) if pd.notna(nameplate_mw) else None
        except (ValueError, TypeError):
            nameplate_mw = None
        
        result_data.append({
            "plant_name": plant_name,
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "fuel_type": fuel_type,
            "nameplate_mw": nameplate_mw,
            "generator_flag": True,
            "source": "eia"
        })
    
    result_df = pd.DataFrame(result_data)
    logger.info(f"Processed {len(result_df)} diesel generator facilities")
    
    # Cache to parquet
    settings.cache_eia_dir.mkdir(parents=True, exist_ok=True)
    cache_path = settings.cache_eia_dir / "eia_generators.parquet"
    result_df.to_parquet(cache_path, index=False)
    logger.info(f"Cached to {cache_path}")
    
    # Persist to DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_eia (
            plant_name VARCHAR,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip VARCHAR,
            fuel_type VARCHAR,
            nameplate_mw DOUBLE,
            generator_flag BOOLEAN,
            source VARCHAR
        )
    """)
    
    if not result_df.empty:
        conn.register('result_df', result_df)
        conn.execute("DROP TABLE IF EXISTS raw_eia")
        conn.execute("CREATE TABLE raw_eia AS SELECT * FROM result_df")
    
    conn.close()
    
    logger.info(f"Persisted {len(result_df)} rows to DuckDB table raw_eia")
    
    return result_df

