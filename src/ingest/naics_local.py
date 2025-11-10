"""Local NAICS data ingestion module."""
import logging
import re
from typing import Optional, Tuple
import pandas as pd
import duckdb
from tqdm import tqdm

from src.config import settings
from src.utils.io import read_data_file
from src.utils.fuzzy import map_headers
from src.utils.addresses import normalize_address
from src.utils.geocode import geocode_address

logger = logging.getLogger(__name__)

# Header mapping: canonical name -> expected header names (fuzzy matching will try these)
EXPECTED_HEADERS = {
    "business_name": ["COMPANY NAME", "NAME", "BUSINESS_NAME", "COMPANY"],
    "address": ["STREET ADDRESS", "ADDRESS", "ADDRESS 1"],
    "city": ["CITY"],
    "state": ["STATE"],
    "zip": ["ZIP CODE", "ZIP", "ZIP_CODE"],
    "county": ["COUNTY"],
    "naics_code": ["NAICS", "NAICS_CODE"],
    "naics_title": ["NAICS DESCRIPTION", "NAICS_TITLE", "TITLE", "DESCRIPTION"],
    "latitude": ["LAT", "LATITUDE"],
    "longitude": ["LON", "LONG", "LONGITUDE"]
}


def normalize_naics_code(naics_code: Optional[str]) -> Optional[str]:
    """
    Normalize NAICS code to 6 digits with left pad zeroes.
    
    Args:
        naics_code: NAICS code string (may contain punctuation)
    
    Returns:
        Normalized 6-digit NAICS code string or None
    """
    if pd.isna(naics_code) or not naics_code:
        return None
    
    # Remove punctuation and whitespace
    cleaned = re.sub(r'[^\d]', '', str(naics_code))
    
    if not cleaned:
        return None
    
    # Left pad to 6 digits
    normalized = cleaned.zfill(6)
    
    # Truncate if longer than 6 digits
    if len(normalized) > 6:
        normalized = normalized[:6]
    
    return normalized


def classify_sector(naics_code: Optional[str], naics_title: Optional[str]) -> Tuple[str, int, str]:
    """
    Classify business sector from NAICS code and title.
    
    Args:
        naics_code: Normalized 6-digit NAICS code
        naics_title: NAICS title/description
    
    Returns:
        Tuple of (sector_primary, sector_confidence, subsector_notes)
    """
    naics_code = naics_code or ""
    naics_title = (naics_title or "").lower()
    
    # Education (check before Fleet to catch "bus depot" in school context)
    if naics_code.startswith("611"):
        return ("Education", 100, "NAICS prefix match")
    
    keywords_edu = ["school", "district", "university", "college", "campus"]
    if any(kw in naics_title for kw in keywords_edu):
        return ("Education", 70, f"Title keyword: {naics_title[:50]}")
    
    # Fleet and Transportation
    if naics_code.startswith("484") or naics_code.startswith("485") or naics_code.startswith("488"):
        return ("Fleet and Transportation", 100, "NAICS prefix match")
    
    keywords_fleet = ["trucking", "bus", "coach", "logistics", "intermodal", "yard", "terminal"]
    if any(kw in naics_title for kw in keywords_fleet):
        return ("Fleet and Transportation", 70, f"Title keyword: {naics_title[:50]}")
    
    # Construction
    if naics_code.startswith("23"):
        return ("Construction", 100, "NAICS prefix match")
    
    keywords_const = ["construction", "site work", "excavation", "paving", "utility contractor", "heavy civil"]
    if any(kw in naics_title for kw in keywords_const):
        return ("Construction", 70, f"Title keyword: {naics_title[:50]}")
    
    # Healthcare
    if naics_code.startswith("621") or naics_code.startswith("622") or naics_code.startswith("623"):
        return ("Healthcare", 100, "NAICS prefix match")
    
    keywords_health = ["hospital", "medical center", "surgery", "nursing", "long term care"]
    if any(kw in naics_title for kw in keywords_health):
        return ("Healthcare", 70, f"Title keyword: {naics_title[:50]}")
    
    # Utilities and Data Centers
    if naics_code == "518210":
        return ("Utilities and Data Centers", 100, "Exact NAICS match: data center")
    
    if naics_code.startswith("22"):
        return ("Utilities and Data Centers", 100, "NAICS prefix match")
    
    keywords_util = ["utility", "power", "water", "wastewater", "data center", "colocation"]
    if any(kw in naics_title for kw in keywords_util):
        return ("Utilities and Data Centers", 70, f"Title keyword: {naics_title[:50]}")
    
    # Industrial and Manufacturing
    if naics_code.startswith("31") or naics_code.startswith("32") or naics_code.startswith("33"):
        return ("Industrial and Manufacturing", 100, "NAICS prefix match")
    
    keywords_mfg = ["plant", "fabrication", "manufacturing", "processing"]
    if any(kw in naics_title for kw in keywords_mfg):
        return ("Industrial and Manufacturing", 70, f"Title keyword: {naics_title[:50]}")
    
    # Public and Government
    if naics_code.startswith("92"):
        return ("Public and Government", 100, "NAICS prefix match")
    
    keywords_public = ["township", "borough", "county", "municipal", "fire", "police", "public works"]
    if any(kw in naics_title for kw in keywords_public):
        return ("Public and Government", 70, f"Title keyword: {naics_title[:50]}")
    
    # Retail and Commercial Fueling
    if naics_code == "447110" or naics_code == "447190":
        return ("Retail and Commercial Fueling", 100, "Exact NAICS match")
    
    keywords_retail = ["gas station", "convenience", "c store"]
    if any(kw in naics_title for kw in keywords_retail):
        return ("Retail and Commercial Fueling", 70, f"Title keyword: {naics_title[:50]}")
    
    # Partial range prefix match (confidence 50) - only for valid prefixes
    if naics_code and len(naics_code) >= 3:
        prefix = naics_code[:3]
        if prefix in ["484", "485", "488"]:
            return ("Fleet and Transportation", 50, "Partial NAICS prefix match")
        if prefix in ["621", "622", "623"]:
            return ("Healthcare", 50, "Partial NAICS prefix match")
        if prefix == "611":
            return ("Education", 50, "Partial NAICS prefix match")
        if prefix == "518":
            return ("Utilities and Data Centers", 50, "Partial NAICS prefix match")
    
    # Unknown
    return ("Unknown", 0, "No match found")


def ingest_naics_local(file_path: Optional[str] = None, geocode: bool = True, skip_geocode: bool = False) -> pd.DataFrame:
    """
    Ingest local NAICS data from CSV.
    
    Args:
        file_path: Path to NAICS CSV file (uses config if not provided)
        geocode: Whether to geocode addresses
        skip_geocode: If True, skip geocoding entirely
    
    Returns:
        Standardized DataFrame with all required columns
    """
    file_path = file_path or settings.naics_local_path
    
    logger.info(f"Starting NAICS local ingestion from {file_path}")
    
    # Read file
    df = read_data_file(file_path)
    
    if df.empty:
        raise ValueError("Input file is empty")
    
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    
    # Map headers - use fuzzy matching for each expected field
    actual_headers = list(df.columns)
    header_map = {}
    
    for canonical, expected_variants in EXPECTED_HEADERS.items():
        # Try exact match first (case-insensitive, strip whitespace)
        matched = None
        for variant in expected_variants:
            for actual in actual_headers:
                if actual.upper().strip() == variant.upper().strip():
                    matched = actual
                    break
            if matched:
                break
        
        # Try fuzzy match if no exact match
        if not matched:
            from src.utils.fuzzy import find_header_match
            for variant in expected_variants:
                matched = find_header_match(variant, actual_headers, threshold=75.0)
                if matched:
                    break
        
        header_map[canonical] = matched
    
    logger.info(f"Header mapping: {header_map}")
    
    # Check for required headers
    required = ["business_name", "address", "city", "state"]
    missing = [r for r in required if r not in header_map or header_map[r] is None]
    if missing:
        raise ValueError(f"Missing required headers: {missing}")
    
    # Extract and process rows
    result_data = []
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing NAICS rows"):
        # Extract fields
        business_name = None
        if header_map.get("business_name"):
            business_name = row.get(header_map["business_name"])
        
        address = None
        if header_map.get("address"):
            address = row.get(header_map["address"])
        
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
        
        naics_code_raw = None
        if header_map.get("naics_code"):
            naics_code_raw = row.get(header_map["naics_code"])
        
        naics_title = None
        if header_map.get("naics_title"):
            naics_title = row.get(header_map["naics_title"])
        
        latitude = None
        if header_map.get("latitude"):
            lat_val = row.get(header_map["latitude"])
            if pd.notna(lat_val):
                try:
                    latitude = float(lat_val)
                except (ValueError, TypeError):
                    pass
        
        longitude = None
        if header_map.get("longitude"):
            lon_val = row.get(header_map["longitude"])
            if pd.notna(lon_val):
                try:
                    longitude = float(lon_val)
                except (ValueError, TypeError):
                    pass
        
        # Clean and normalize
        business_name = str(business_name).strip() if not pd.isna(business_name) else None
        address = str(address).strip() if not pd.isna(address) else None
        city = str(city).strip() if not pd.isna(city) else None
        state = str(state).strip() if not pd.isna(state) else None
        zip_code = str(zip_code).strip() if not pd.isna(zip_code) else None
        county = str(county).strip() if not pd.isna(county) else None
        naics_title = str(naics_title).strip() if not pd.isna(naics_title) else None
        
        # Normalize NAICS code
        naics_code = normalize_naics_code(naics_code_raw)
        
        # Classify sector
        sector_primary, sector_confidence, subsector_notes = classify_sector(naics_code, naics_title)
        
        # Geocode if missing coordinates
        if (latitude is None or longitude is None) and not skip_geocode and geocode:
            full_address = normalize_address(address, None, city, state, zip_code, "USA")
            if full_address:
                lat, lng, conf = geocode_address(full_address, settings.duckdb_path, skip=skip_geocode)
                if lat and lng:
                    latitude = lat
                    longitude = lng
        
        result_data.append({
            "business_name": business_name,
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "county": county,
            "naics_code": naics_code,
            "naics_title": naics_title,
            "sector_primary": sector_primary,
            "sector_confidence": sector_confidence,
            "subsector_notes": subsector_notes,
            "latitude": latitude,
            "longitude": longitude,
            "source": "naics_local"
        })
    
    result_df = pd.DataFrame(result_data)
    logger.info(f"Processed {len(result_df)} NAICS rows")
    
    # Persist to DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_naics_local (
            business_name VARCHAR,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip VARCHAR,
            county VARCHAR,
            naics_code VARCHAR,
            naics_title VARCHAR,
            sector_primary VARCHAR,
            sector_confidence INTEGER,
            subsector_notes VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            source VARCHAR
        )
    """)
    
    conn.register('result_df', result_df)
    conn.execute("DROP TABLE IF EXISTS raw_naics_local")
    conn.execute("CREATE TABLE raw_naics_local AS SELECT * FROM result_df")
    conn.close()
    
    logger.info(f"Persisted {len(result_df)} rows to DuckDB table raw_naics_local")
    
    return result_df

