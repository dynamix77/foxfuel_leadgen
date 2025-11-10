"""PA DEP Storage Tank ingestion module."""
import logging
import re
from typing import Optional, Dict
import pandas as pd
import duckdb
from tqdm import tqdm

from src.config import settings
from src.utils.io import read_data_file, write_preview_csv
from src.utils.fuzzy import map_headers
from src.utils.addresses import normalize_address
from src.utils.geocode import geocode_address, set_geocode_qps

logger = logging.getLogger(__name__)

# Header mapping: canonical name -> expected header name
EXPECTED_HEADERS = {
    "facility_name": "PF_NAME",
    "facility_id": "PF_SITE_ID",
    "mailing_name": "MAILING_NAME",
    "address_1": "LOCAD_PF_ADDRESS_1",
    "address_2": "LOCAD_PF_ADDRESS_2",
    "city": "LOCAD_LOCAD_PF_CITY",
    "state": "LOCAD_PF_STATE",
    "zip": "LOCAD_PF_ZIP_CODE",
    "county": "PF_COUNTY_NAME",
    "product_code": "SUBSTANCE_CODE",
    "capacity": "CAPACITY",
    "status_code": "STATUS_CODE"
}

# Product code constants
DIESEL_LIKE_CODES = {"DIESL", "BIDSL", "HO", "KERO"}
NON_DIESEL_CODES = {"GAS", "AVGAS", "JET", "ETHNL", "HZSUB", "OTHER", "USDOL", "NMO", "UNREG", "GSHOL", "NPOIL", "HZPRL"}

# Status code constants
ACTIVE_STATUS = {"C"}  # Treat "T" as not active


def clean_capacity(capacity_str: Optional[str]) -> Optional[float]:
    """
    Extract numeric capacity from string.
    
    Args:
        capacity_str: Capacity string (may contain commas, units, etc.)
    
    Returns:
        Numeric capacity in gallons or None
    """
    if pd.isna(capacity_str) or not capacity_str:
        return None
    
    # Convert to string and remove commas
    s = str(capacity_str).replace(",", "")
    
    # Extract first numeric token
    match = re.search(r'(\d+\.?\d*)', s)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def get_capacity_bucket(capacity_gal: Optional[float]) -> str:
    """
    Get capacity bucket string.
    
    Args:
        capacity_gal: Capacity in gallons
    
    Returns:
        Bucket string: "20K+", "10K-20K", "5K-10K", "1K-5K", "<1K"
    """
    if capacity_gal is None or pd.isna(capacity_gal):
        return "<1K"
    
    if capacity_gal >= 20000:
        return "20K+"
    elif capacity_gal >= 10000:
        return "10K-20K"
    elif capacity_gal >= 5000:
        return "5K-10K"
    elif capacity_gal >= 1000:
        return "1K-5K"
    else:
        return "<1K"


def classify_diesel_like(product_code: Optional[str]) -> bool:
    """
    Classify if product code is diesel-like.
    
    Args:
        product_code: Product code string
    
    Returns:
        True if diesel-like, False otherwise
    """
    if pd.isna(product_code) or not product_code:
        return False
    
    code = str(product_code).strip().upper()
    return code in DIESEL_LIKE_CODES


def classify_active_like(status_code: Optional[str]) -> bool:
    """
    Classify if status code indicates active facility.
    
    Args:
        status_code: Status code string
    
    Returns:
        True if status is "C" (active), False otherwise
    """
    if pd.isna(status_code) or not status_code:
        return False
    
    return str(status_code).strip().upper() in ACTIVE_STATUS


def ingest_pa_tanks(
    file_path: str,
    geocode: bool = True,
    skip_geocode: bool = False,
    geocode_limit: Optional[int] = None,
    geocode_qps: float = 5.0,
    geocode_batch_size: int = 100
) -> pd.DataFrame:
    """
    Ingest PA DEP storage tank data from CSV or XLSX.
    
    Args:
        file_path: Path to input file (CSV or XLSX)
        geocode: Whether to geocode addresses (default: True)
    
    Returns:
        Standardized DataFrame with all required columns
    """
    logger.info(f"Starting PA tanks ingestion from {file_path}")
    
    # Read file
    df = read_data_file(file_path)
    
    if df.empty:
        raise ValueError("Input file is empty")
    
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    
    # Map headers
    actual_headers = list(df.columns)
    header_map = map_headers(EXPECTED_HEADERS, actual_headers)
    
    logger.info(f"Header mapping: {header_map}")
    
    # Check for required headers
    required = ["facility_name", "address_1", "city", "state"]
    missing = [r for r in required if r not in header_map or header_map[r] is None]
    if missing:
        raise ValueError(f"Missing required headers: {missing}")
    
    # Extract columns using mapped headers
    result_data = []
    geocode_count = 0
    cache_hits = 0
    
    # Prepare rows for priority sorting (diesel + larger capacity first)
    rows_list = []
    for idx, row in df.iterrows():
        rows_list.append((idx, row))
    
    # Sort by priority: is_diesel_like DESC, capacity_gal DESC, facility_name
    def get_priority(row_tuple):
        idx, row = row_tuple
        # Extract product code for diesel check
        product_code = None
        if header_map.get("product_code"):
            product_code = row.get(header_map["product_code"])
        is_diesel = 1 if product_code and str(product_code).upper() in DIESEL_LIKE_CODES else 0
        
        # Extract capacity
        capacity_raw = None
        if header_map.get("capacity"):
            capacity_raw = row.get(header_map["capacity"])
        capacity = clean_capacity(capacity_raw) or 0
        
        # Extract facility name for tie-breaker
        facility_name = None
        if header_map.get("facility_name"):
            facility_name = row.get(header_map["facility_name"])
        name_str = str(facility_name).upper() if facility_name else ""
        
        return (is_diesel, capacity, name_str)
    
    if geocode_limit and geocode and not skip_geocode:
        rows_list.sort(key=get_priority, reverse=True)
    
    for idx, row in tqdm(rows_list, total=len(rows_list), desc="Processing rows"):
        # Extract fields
        facility_name = None
        if header_map.get("facility_name"):
            facility_name = row.get(header_map["facility_name"])
        if (not facility_name or pd.isna(facility_name)) and header_map.get("mailing_name"):
            facility_name = row.get(header_map["mailing_name"])
        
        facility_id = None
        if header_map.get("facility_id"):
            facility_id = row.get(header_map["facility_id"])
        
        address_1 = None
        if header_map.get("address_1"):
            address_1 = row.get(header_map["address_1"])
        
        address_2 = None
        if header_map.get("address_2"):
            address_2 = row.get(header_map["address_2"])
        
        city = None
        if header_map.get("city"):
            city = row.get(header_map["city"])
        
        state = None
        if header_map.get("state"):
            state = row.get(header_map["state"])
        
        zip_code = None
        if header_map.get("zip"):
            zip_code = row.get(header_map["zip"])
        
        county = None
        if header_map.get("county"):
            county = row.get(header_map["county"])
        
        product_code = None
        if header_map.get("product_code"):
            product_code = row.get(header_map["product_code"])
        
        capacity_raw = None
        if header_map.get("capacity"):
            capacity_raw = row.get(header_map["capacity"])
        
        status_code = None
        if header_map.get("status_code"):
            status_code = row.get(header_map["status_code"])
        
        # Clean and normalize
        facility_name = str(facility_name).strip() if not pd.isna(facility_name) else None
        address_1 = str(address_1).strip() if not pd.isna(address_1) else None
        address_2 = str(address_2).strip() if address_2 and not pd.isna(address_2) else None
        city = str(city).strip() if not pd.isna(city) else None
        state = str(state).strip() if not pd.isna(state) else None
        zip_code = str(zip_code).strip() if not pd.isna(zip_code) else None
        county = str(county).strip() if not pd.isna(county) else None
        product_code = str(product_code).strip() if not pd.isna(product_code) else None
        status_code = str(status_code).strip() if not pd.isna(status_code) else None
        
        # County filter
        if county and county not in settings.counties:
            continue
        
        # Process capacity
        capacity_gal = clean_capacity(capacity_raw)
        capacity_bucket = get_capacity_bucket(capacity_gal)
        
        # Classifications
        is_diesel_like = classify_diesel_like(product_code)
        is_active_like = classify_active_like(status_code)
        
        # Build address for geocoding
        full_address = normalize_address(
            address_1, address_2, city, state, zip_code, "USA"
        )
        
        # Geocode
        latitude = None
        longitude = None
        geocode_confidence = None
        
        if skip_geocode:
            geocode_confidence = "skipped"
        elif geocode and full_address:
            # Check geocode limit
            if geocode_limit and geocode_count >= geocode_limit:
                geocode_confidence = "limit_reached"
            else:
                lat, lng, conf = geocode_address(full_address, settings.duckdb_path, skip=skip_geocode)
                latitude = lat
                longitude = lng
                geocode_confidence = conf
                if conf in ["cached", "high", "medium", "low"]:
                    if lat and lng:
                        # Check if this was a cache hit or new geocode
                        if conf == "cached":
                            cache_hits += 1
                        else:
                            geocode_count += 1
        
        # Create facility_id if missing
        if not facility_id:
            # Composite key from name + address
            facility_id = f"{facility_name or 'UNKNOWN'}_{address_1 or 'UNKNOWN'}"
        
        result_data.append({
            "facility_id": facility_id,
            "facility_name": facility_name,
            "address": address_1,
            "city": city,
            "state": state,
            "zip": zip_code,
            "county": county,
            "product_code": product_code,
            "capacity_gal": capacity_gal,
            "status_code": status_code,
            "is_diesel_like": is_diesel_like,
            "is_active_like": is_active_like,
            "capacity_bucket": capacity_bucket,
            "latitude": latitude,
            "longitude": longitude,
            "distance_mi": None,
            "sector_primary": None,
            "sector_confidence": None,
            "naics_code": None,
            "maps_category": None,
            "source": "pa_tanks"
        })
    
    result_df = pd.DataFrame(result_data)
    
    # Sort by priority for geocoding: diesel_like DESC, capacity_gal DESC, facility_name
    if geocode_limit and geocode and not skip_geocode:
        result_df = result_df.sort_values(
            by=['is_diesel_like', 'capacity_gal', 'facility_name'],
            ascending=[False, False, True],
            na_position='last'
        )
    
    logger.info(f"Processed {len(result_df)} rows after filtering")
    if geocode and not skip_geocode:
        logger.info(f"Geocoding: {geocode_count} new geocodes, {cache_hits} cache hits")
    
    # Persist to DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_pa_tanks (
            facility_id VARCHAR,
            facility_name VARCHAR,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip VARCHAR,
            county VARCHAR,
            product_code VARCHAR,
            capacity_gal DOUBLE,
            status_code VARCHAR,
            is_diesel_like BOOLEAN,
            is_active_like BOOLEAN,
            capacity_bucket VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            source VARCHAR
        )
    """)
    
    # Drop existing data and insert new
    conn.execute("DROP TABLE IF EXISTS raw_pa_tanks")
    conn.register('result_df', result_df)
    conn.execute("CREATE TABLE raw_pa_tanks AS SELECT * FROM result_df")
    conn.close()
    
    logger.info(f"Persisted {len(result_df)} rows to DuckDB table raw_pa_tanks")
    
    return result_df

