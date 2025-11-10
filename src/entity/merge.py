"""Entity merge module for combining signals from multiple sources."""
import logging
import pandas as pd
import duckdb
from typing import Dict, Optional
from rapidfuzz import fuzz
from math import radians, cos, sin, asin, sqrt

from src.config import settings
from src.utils.addresses import create_street_key

logger = logging.getLogger(__name__)


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two points in meters using Haversine formula.
    
    Args:
        lat1, lon1: First point coordinates
        lat2, lon2: Second point coordinates
    
    Returns:
        Distance in meters
    """
    # Convert to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000  # Earth radius in meters
    return c * r


def merge_entities(sources: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merge entities from multiple sources with source attribution.
    
    Args:
        sources: Dict mapping source names to DataFrames
    
    Returns:
        Merged DataFrame with source attribution
    """
    logger.info(f"Merging entities from {len(sources)} sources...")
    
    # Add source column to each DataFrame if not present
    merged_list = []
    for source_name, df in sources.items():
        if 'source' not in df.columns:
            df['source'] = source_name
        merged_list.append(df)
    
    # Combine all sources
    merged_df = pd.concat(merged_list, ignore_index=True)
    
    logger.info(f"Merged {len(merged_df)} total entities")
    return merged_df


def merge_naics_signals(entity_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge NAICS sector signals into entity DataFrame.
    
    Args:
        entity_df: Entity DataFrame with facility_id, facility_name, latitude, longitude
    
    Returns:
        Entity DataFrame with sector_primary, sector_confidence, naics_code columns added
    """
    logger.info("Merging NAICS sector signals into entities...")
    
    # Load NAICS data from DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    try:
        naics_df = conn.execute("SELECT * FROM raw_naics_local").df()
    except Exception:
        logger.warning("No NAICS data found, skipping sector merge")
        conn.close()
        return entity_df
    conn.close()
    
    if naics_df.empty:
        logger.warning("NAICS DataFrame is empty")
        return entity_df
    
    # Create normalized name keys for matching
    entity_df['name_key'] = entity_df['facility_name'].apply(
        lambda x: create_street_key(str(x) if pd.notna(x) else "")
    )
    naics_df['name_key'] = naics_df['business_name'].apply(
        lambda x: create_street_key(str(x) if pd.notna(x) else "")
    )
    
    # Initialize sector columns
    entity_df['sector_primary'] = None
    entity_df['sector_confidence'] = 0
    entity_df['naics_code'] = None
    
    # Match NAICS records to entities
    matches = []
    for idx, entity in entity_df.iterrows():
        if pd.isna(entity.get('latitude')) or pd.isna(entity.get('longitude')):
            continue
        
        entity_lat = entity['latitude']
        entity_lon = entity['longitude']
        entity_name_key = entity['name_key']
        
        best_match = None
        best_confidence = 0
        
        for naics_idx, naics_row in naics_df.iterrows():
            if pd.isna(naics_row.get('latitude')) or pd.isna(naics_row.get('longitude')):
                continue
            
            naics_lat = naics_row['latitude']
            naics_lon = naics_row['longitude']
            
            # Check distance
            distance = haversine_distance(entity_lat, entity_lon, naics_lat, naics_lon)
            if distance > settings.naics_match_radius_meters:
                continue
            
            # Check name similarity
            naics_name_key = naics_row['name_key']
            if entity_name_key and naics_name_key:
                similarity = fuzz.ratio(entity_name_key.upper(), naics_name_key.upper())
                if similarity < settings.naics_name_similarity_min:
                    continue
            else:
                continue
            
            # This is a candidate match
            sector_conf = naics_row.get('sector_confidence', 0)
            if sector_conf > best_confidence:
                best_confidence = sector_conf
                best_match = {
                    'sector_primary': naics_row.get('sector_primary'),
                    'sector_confidence': sector_conf,
                    'naics_code': naics_row.get('naics_code')
                }
        
        if best_match:
            matches.append({
                'entity_idx': idx,
                **best_match
            })
    
    # Apply matches with preference rules
    sector_preference = {
        "Fleet and Transportation": 1,
        "Healthcare": 2,
        "Construction": 3,
        "Utilities and Data Centers": 4,
        "Industrial and Manufacturing": 5,
        "Education": 6,
        "Public and Government": 7,
        "Retail and Commercial Fueling": 8,
        "Unknown": 9
    }
    
    # Group matches by entity and pick best
    entity_matches = {}
    for match in matches:
        entity_idx = match['entity_idx']
        if entity_idx not in entity_matches:
            entity_matches[entity_idx] = match
        else:
            # Prefer higher confidence, then preferred sector
            existing = entity_matches[entity_idx]
            if match['sector_confidence'] > existing['sector_confidence']:
                entity_matches[entity_idx] = match
            elif match['sector_confidence'] == existing['sector_confidence']:
                match_pref = sector_preference.get(match['sector_primary'], 99)
                existing_pref = sector_preference.get(existing['sector_primary'], 99)
                if match_pref < existing_pref:
                    entity_matches[entity_idx] = match
    
    # Apply matches to entity DataFrame
    for entity_idx, match in entity_matches.items():
        entity_df.at[entity_idx, 'sector_primary'] = match['sector_primary']
        entity_df.at[entity_idx, 'sector_confidence'] = match['sector_confidence']
        entity_df.at[entity_idx, 'naics_code'] = match['naics_code']
    
    # Drop temporary name_key column
    entity_df = entity_df.drop(columns=['name_key'], errors='ignore')
    
    matched_count = len(entity_matches)
    logger.info(f"Matched {matched_count} entities with NAICS sector signals")
    
    # Persist signals and sector metadata to DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    conn.execute("ALTER TABLE raw_pa_tanks ADD COLUMN IF NOT EXISTS sector_primary VARCHAR")
    conn.execute("ALTER TABLE raw_pa_tanks ADD COLUMN IF NOT EXISTS sector_confidence INTEGER")
    conn.execute("ALTER TABLE raw_pa_tanks ADD COLUMN IF NOT EXISTS naics_code VARCHAR")

    if entity_matches:
        update_rows = []
        signal_rows = []
        for entity_idx, match in entity_matches.items():
            facility_id = entity_df.at[entity_idx, 'facility_id']
            update_rows.append(
                {
                    "facility_id": str(facility_id),
                    "sector_primary": match["sector_primary"],
                    "sector_confidence": match["sector_confidence"],
                    "naics_code": match["naics_code"],
                }
            )
            signal_rows.append(
                {
                    "signal_id": f"{facility_id}_sector",
                    "entity_id": facility_id,
                    "signal_type": "sector",
                    "signal_value": match["sector_primary"],
                    "source": "naics_local",
                }
            )

        update_df = pd.DataFrame(update_rows)
        conn.register("sector_update_df", update_df)
        conn.execute(
            """
            UPDATE raw_pa_tanks
            SET sector_primary = sector_update_df.sector_primary,
                sector_confidence = sector_update_df.sector_confidence,
                naics_code = sector_update_df.naics_code
            FROM sector_update_df
            WHERE CAST(raw_pa_tanks.facility_id AS VARCHAR) = sector_update_df.facility_id
            """
        )
        logger.info("Updated sector columns on raw_pa_tanks")

        signals_df = pd.DataFrame(signal_rows)
        conn.register("sector_signals_df", signals_df)
        conn.execute(
            """
            INSERT OR REPLACE INTO signals 
            SELECT signal_id, entity_id, signal_type, signal_value, source, CURRENT_TIMESTAMP
            FROM sector_signals_df
            """
        )
        logger.info(f"Persisted {len(signal_rows)} sector signals to DuckDB")

    conn.close()

    return entity_df


def merge_maps_extractor(entity_df: pd.DataFrame, maps_df: pd.DataFrame, distance_threshold_meters: float = 200.0) -> pd.DataFrame:
    """
    Merge Google Maps Extractor data into entity DataFrame.

    Args:
        entity_df: Entity DataFrame with facility_id, facility_name, latitude, longitude
        maps_df: Maps extractor DataFrame
        distance_threshold_meters: Max distance for coordinate matching

    Returns:
        Updated entity DataFrame
    """
    if maps_df.empty:
        logger.info("Maps extractor DataFrame empty, skipping merge")
        return entity_df

    logger.info("Merging Maps Extractor data into entities...")

    # Prepare keys
    entity_df["name_key"] = entity_df["facility_name"].apply(
        lambda x: create_street_key(str(x)) if pd.notna(x) else ""
    )
    maps_df["name_key"] = maps_df["place_name"].apply(
        lambda x: create_street_key(str(x)) if pd.notna(x) else ""
    )

    conn = duckdb.connect(settings.duckdb_path)

    # Ensure columns exist for places data
    conn.execute("ALTER TABLE raw_pa_tanks ADD COLUMN IF NOT EXISTS maps_category VARCHAR")
    conn.execute("ALTER TABLE raw_pa_tanks ADD COLUMN IF NOT EXISTS maps_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

    matches = []

    for idx, entity in entity_df.iterrows():
        entity_name_key = entity["name_key"]
        if not entity_name_key:
            continue

        # Filter candidate maps rows by name key
        candidates = maps_df[maps_df["name_key"] == entity_name_key]
        if candidates.empty:
            continue

        best_match = None
        best_distance = None

        for _, place in candidates.iterrows():
            if pd.notna(entity.get("latitude")) and pd.notna(entity.get("longitude")) and pd.notna(place["latitude"]) and pd.notna(place["longitude"]):
                distance = haversine_distance(
                    entity["latitude"],
                    entity["longitude"],
                    place["latitude"],
                    place["longitude"],
                )
                if distance_threshold_meters is not None and distance > distance_threshold_meters:
                    continue
            else:
                distance = None

            if best_distance is None or (distance is not None and distance < best_distance):
                best_match = place
                best_distance = distance

        if best_match is not None:
            matches.append((idx, best_match))

    logger.info(f"Matched {len(matches)} entities with Maps Extractor data")

    for idx, match in matches:
        entity_df.at[idx, "maps_category"] = match["categories"]
        entity_df.at[idx, "maps_source_file"] = match["source_file"]
        if pd.isna(entity_df.at[idx, "latitude"]) and pd.notna(match["latitude"]):
            entity_df.at[idx, "latitude"] = match["latitude"]
        if pd.isna(entity_df.at[idx, "longitude"]) and pd.notna(match["longitude"]):
            entity_df.at[idx, "longitude"] = match["longitude"]

    if matches:
        # Persist category information back to raw_pa_tanks
        update_df = pd.DataFrame(
            [
                {
                    "facility_id": entity_df.at[idx, "facility_id"],
                    "maps_category": match["categories"],
                    "latitude": match["latitude"],
                    "longitude": match["longitude"],
                }
                for idx, match in matches
            ]
        )
        conn.register("maps_update_df", update_df)
        conn.execute(
            """
            UPDATE raw_pa_tanks
            SET maps_category = maps_update_df.maps_category,
                latitude = COALESCE(raw_pa_tanks.latitude, maps_update_df.latitude),
                longitude = COALESCE(raw_pa_tanks.longitude, maps_update_df.longitude),
                maps_updated_at = CURRENT_TIMESTAMP
            FROM maps_update_df
            WHERE raw_pa_tanks.facility_id = maps_update_df.facility_id
            """
        )

        # Attach signals
        signal_rows = []
        for idx, match in matches:
            facility_id = entity_df.at[idx, "facility_id"]
            category_value = match["categories"] or ""
            signal_rows.append(
                {
                    "signal_id": f"{facility_id}_places",
                    "entity_id": facility_id,
                    "signal_type": "places",
                    "signal_value": category_value,
                    "source": "maps_extractor",
                }
            )

        if signal_rows:
            signals_df = pd.DataFrame(signal_rows)
            conn.register("places_signals_df", signals_df)
            conn.execute(
                """
                INSERT OR REPLACE INTO signals
                SELECT signal_id, entity_id, signal_type, signal_value, source, CURRENT_TIMESTAMP
                FROM places_signals_df
                """
            )
            logger.info(f"Persisted {len(signal_rows)} places signals to DuckDB")

    conn.close()

    entity_df = entity_df.drop(columns=["name_key"], errors="ignore")
    return entity_df

