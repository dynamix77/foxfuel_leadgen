"""Procurement and bid opportunity ingestion module."""
import logging
from typing import List, Optional
import pandas as pd
from pathlib import Path
import duckdb
import hashlib
from datetime import datetime
from src.config import settings

logger = logging.getLogger(__name__)

# Relevance keywords
RELEVANCE_KEYWORDS = [
    "fuel", "diesel", "heating oil", "generator", "emergency power",
    "fleet fueling", "bulk fuel", "fuel delivery", "fuel supply",
    "diesel fuel", "heating fuel", "backup generator", "standby generator"
]


def classify_relevance(title: str, description: str = "") -> float:
    """
    Classify bid relevance score (0-1).
    
    Args:
        title: Bid title
        description: Bid description
    
    Returns:
        Relevance score 0-1
    """
    text = f"{title} {description}".lower()
    matches = sum(1 for keyword in RELEVANCE_KEYWORDS if keyword.lower() in text)
    return min(matches / len(RELEVANCE_KEYWORDS), 1.0)


def ingest_procurement() -> pd.DataFrame:
    """
    Ingest procurement bids and solicitations from configured sources.
    
    Returns:
        Standardized DataFrame with procurement opportunities
    """
    logger.info("Starting procurement ingestion...")
    
    if not settings.procurement_sources:
        logger.warning("No procurement sources configured, returning empty DataFrame")
        return pd.DataFrame()
    
    result_data = []
    
    # TODO: Implement RSS/HTML parsing for each source
    # For now, return empty DataFrame
    logger.warning("Procurement ingestion not yet fully implemented")
    
    result_df = pd.DataFrame()
    
    if result_df.empty:
        logger.info("No procurement data to process")
        return result_df
    
    # Deduplicate by bid_id
    result_df = result_df.drop_duplicates(subset=["bid_id"], keep="first")
    
    # Cache
    settings.cache_procurement_dir.mkdir(parents=True, exist_ok=True)
    cache_path = settings.cache_procurement_dir / "procurement_bids.parquet"
    result_df.to_parquet(cache_path, index=False)
    
    # Persist to DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_procurement (
            bid_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            buyer VARCHAR,
            url VARCHAR,
            due_date DATE,
            relevance_score DOUBLE,
            keywords VARCHAR,
            county VARCHAR,
            source VARCHAR
        )
    """)
    
    if not result_df.empty:
        conn.register('result_df', result_df)
        conn.execute("DROP TABLE IF EXISTS raw_procurement")
        conn.execute("CREATE TABLE raw_procurement AS SELECT * FROM result_df")
    
    conn.close()
    
    logger.info(f"Persisted {len(result_df)} rows to DuckDB table raw_procurement")
    
    return result_df

