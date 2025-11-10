"""Permit data ingestion module."""
import logging
from typing import List, Optional
import pandas as pd
from pathlib import Path
import duckdb
from datetime import datetime, timedelta
from src.config import settings

logger = logging.getLogger(__name__)

# Permit classification keywords
PERMIT_CLASSES = {
    "tank_install": ["tank", "storage tank", "fuel tank", "oil tank"],
    "tank_replace": ["tank replacement", "replace tank", "tank removal"],
    "generator_install": ["generator", "standby generator", "backup generator"],
    "transfer_switch": ["transfer switch", "ats", "automatic transfer"],
    "fuel_system": ["fuel system", "fuel line", "fuel piping"]
}


def classify_permit(description: str) -> Optional[str]:
    """
    Classify permit type from description.
    
    Args:
        description: Permit description
    
    Returns:
        Permit class or None
    """
    desc_lower = description.lower()
    for permit_class, keywords in PERMIT_CLASSES.items():
        if any(keyword in desc_lower for keyword in keywords):
            return permit_class
    return None


def ingest_permits() -> pd.DataFrame:
    """
    Ingest permit data from configured sources.
    
    Returns:
        Standardized DataFrame with permit data
    """
    logger.info("Starting permits ingestion...")
    
    if not settings.permits_sources:
        logger.warning("No permits sources configured, returning empty DataFrame")
        return pd.DataFrame()
    
    result_data = []
    
    # TODO: Implement portal scraping for each source
    # For now, return empty DataFrame
    logger.warning("Permits ingestion not yet fully implemented")
    
    result_df = pd.DataFrame()
    
    if result_df.empty:
        logger.info("No permits data to process")
        return result_df
    
    # Deduplicate by permit_id
    result_df = result_df.drop_duplicates(subset=["permit_id"], keep="first")
    
    # Cache
    settings.cache_permits_dir.mkdir(parents=True, exist_ok=True)
    cache_path = settings.cache_permits_dir / "permits.parquet"
    result_df.to_parquet(cache_path, index=False)
    
    # Persist to DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_permits (
            permit_id VARCHAR PRIMARY KEY,
            applicant VARCHAR,
            address VARCHAR,
            city VARCHAR,
            zip VARCHAR,
            county VARCHAR,
            permit_type VARCHAR,
            class VARCHAR,
            issue_date DATE,
            contractor VARCHAR,
            source VARCHAR
        )
    """)
    
    if not result_df.empty:
        conn.register('result_df', result_df)
        conn.execute("DROP TABLE IF EXISTS raw_permits")
        conn.execute("CREATE TABLE raw_permits AS SELECT * FROM result_df")
    
    conn.close()
    
    logger.info(f"Persisted {len(result_df)} rows to DuckDB table raw_permits")
    
    return result_df

