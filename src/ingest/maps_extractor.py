"""Maps Extractor CSV ingestion module."""
import logging
import glob
import re
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd
import duckdb

from src.config import settings
from src.utils.addresses import create_street_key

logger = logging.getLogger(__name__)


def _find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Find first matching column by case-insensitive comparison."""
    normalized = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in normalized:
            return normalized[candidate.lower()]
    return None


def parse_organization_address(address_str: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Parse CodeCanyon OrganizationAddress field.
    
    Format: "Address: 4921 Cottman Ave, Philadelphia, PA 19135"
    or just: "4921 Cottman Ave, Philadelphia, PA 19135"
    
    Returns:
        Tuple of (address, city, state, zip)
    """
    if not address_str or pd.isna(address_str):
        return (None, None, None, None)
    
    address_str = str(address_str).strip()
    
    # Remove "Address: " prefix if present
    if address_str.startswith("Address:"):
        address_str = address_str[9:].strip()
    
    # Try to parse: "Street, City, ST ZIP"
    # Pattern: street address, city, state zip
    pattern = r'^(.+?),\s*([^,]+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$'
    match = re.match(pattern, address_str)
    
    if match:
        street = match.group(1).strip()
        city = match.group(2).strip()
        state = match.group(3).strip()
        zip_code = match.group(4).strip()
        return (street, city, state, zip_code)
    
    # Fallback: try simpler pattern without ZIP
    pattern2 = r'^(.+?),\s*([^,]+?),\s*([A-Z]{2})$'
    match2 = re.match(pattern2, address_str)
    if match2:
        street = match2.group(1).strip()
        city = match2.group(2).strip()
        state = match2.group(3).strip()
        return (street, city, state, None)
    
    # If no pattern matches, return the whole string as address
    return (address_str, None, None, None)


def ingest_maps_extractor(glob_pattern: Optional[str] = None) -> pd.DataFrame:
    """
    Ingest Google Maps Extractor CSV exports.

    Args:
        glob_pattern: Glob pattern for CSV files (defaults to ./data/maps_extractor/*.csv)

    Returns:
        Standardized DataFrame with places data
    """
    glob_pattern = glob_pattern or "./data/maps_extractor/*.csv"
    files = sorted(glob.glob(glob_pattern))

    if not files:
        logger.info(f"No Maps Extractor files found for pattern: {glob_pattern}")
        return pd.DataFrame()

    logger.info(f"Found {len(files)} Maps Extractor files")

    records = []

    for file_path in files:
        logger.info(f"Loading Maps Extractor CSV: {file_path}")
        try:
            df = pd.read_csv(file_path, low_memory=False)
        except Exception as exc:
            logger.error(f"Failed to read {file_path}: {exc}")
            continue

        if df.empty:
            logger.warning(f"{file_path} is empty")
            continue

        # Try CodeCanyon format first (OrganizationName, OrganizationAddress, etc.)
        name_col = _find_column(df, ["organizationname", "name", "place", "business_name"])
        org_address_col = _find_column(df, ["organizationaddress", "fulladdress", "address", "full_address"])
        city_col = _find_column(df, ["city", "municipality", "locality"])
        state_col = _find_column(df, ["state", "region", "province"])
        zip_col = _find_column(df, ["zip", "postal_code", "postcode", "zipcode"])
        lat_col = _find_column(df, ["organizationlatitude", "latitude", "lat"])
        lon_col = _find_column(df, ["organizationlongitude", "longitude", "lon", "lng"])
        category_col = _find_column(df, ["organizationcategory", "categories", "category"])

        for _, row in df.iterrows():
            name = None
            if name_col:
                name = str(row[name_col]).strip() if pd.notna(row[name_col]) else None
            
            # Parse address - try CodeCanyon format first
            address = None
            city = None
            state = None
            zip_code = None
            
            if org_address_col and pd.notna(row[org_address_col]):
                parsed_addr, parsed_city, parsed_state, parsed_zip = parse_organization_address(row[org_address_col])
                address = parsed_addr
                city = parsed_city or (str(row[city_col]).strip() if city_col and pd.notna(row[city_col]) else None)
                state = parsed_state or (str(row[state_col]).strip() if state_col and pd.notna(row[state_col]) else None)
                zip_code = parsed_zip or (str(row[zip_col]).strip() if zip_col and pd.notna(row[zip_col]) else None)
            else:
                # Fallback to separate columns if available
                address = str(row[org_address_col]).strip() if org_address_col and pd.notna(row[org_address_col]) else None
                city = str(row[city_col]).strip() if city_col and pd.notna(row[city_col]) else None
                state = str(row[state_col]).strip() if state_col and pd.notna(row[state_col]) else None
                zip_code = str(row[zip_col]).strip() if zip_col and pd.notna(row[zip_col]) else None

            try:
                latitude = float(row[lat_col]) if lat_col and pd.notna(row[lat_col]) else None
            except (TypeError, ValueError):
                latitude = None

            try:
                longitude = float(row[lon_col]) if lon_col and pd.notna(row[lon_col]) else None
            except (TypeError, ValueError):
                longitude = None

            categories = str(row[category_col]).strip() if category_col and pd.notna(row[category_col]) else None

            name_key = create_street_key(name) if name else None

            records.append(
                {
                    "place_name": name,
                    "address": address,
                    "city": city,
                    "state": state,
                    "zip": zip_code,
                    "latitude": latitude,
                    "longitude": longitude,
                    "categories": categories,
                    "name_key": name_key,
                    "source_file": Path(file_path).name,
                    "source": "maps_extractor",
                }
            )

    if not records:
        logger.warning("No records processed from Maps Extractor files")
        return pd.DataFrame()

    result_df = pd.DataFrame(records)
    logger.info(f"Processed {len(result_df)} Maps Extractor rows")

    settings.cache_maps_extractor_dir.mkdir(parents=True, exist_ok=True)
    cache_path = settings.cache_maps_extractor_dir / "maps_extractor.parquet"
    result_df.to_parquet(cache_path, index=False)
    logger.info(f"Cached Maps Extractor data to {cache_path}")

    conn = duckdb.connect(settings.duckdb_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_maps_extractor (
            place_name VARCHAR,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            categories VARCHAR,
            name_key VARCHAR,
            source_file VARCHAR,
            source VARCHAR
        )
        """
    )

    conn.register("result_df", result_df)
    conn.execute("DROP TABLE IF EXISTS raw_maps_extractor")
    conn.execute("CREATE TABLE raw_maps_extractor AS SELECT * FROM result_df")
    conn.close()
    logger.info(f"Persisted Maps Extractor data to DuckDB table raw_maps_extractor")

    return result_df

