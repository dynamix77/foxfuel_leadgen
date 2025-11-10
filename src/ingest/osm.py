"""OpenStreetMap Overpass API ingestion module."""
import logging
from typing import List, Dict, Optional
import pandas as pd
import json
from pathlib import Path
import duckdb
import requests
from src.config import settings

logger = logging.getLogger(__name__)


def ingest_osm() -> pd.DataFrame:
    """
    Ingest OSM features for depots, yards, and terminals using Overpass API.
    
    Returns:
        Standardized DataFrame with OSM features
    """
    logger.info("Starting OSM Overpass ingestion...")
    
    # Overpass query for Southeast PA region
    # Bounding box for Southeast PA (approximate)
    bbox = "39.8,-75.6,40.3,-74.8"  # min_lat, min_lon, max_lat, max_lon
    
    overpass_query = f"""
    [out:json][timeout:60];
    (
      node["amenity"="bus_station"]({bbox});
      node["public_transport"="stop_position"]({bbox});
      way["landuse"="industrial"]({bbox});
      way["man_made"="works"]({bbox});
      way["aeroway"="apron"]({bbox});
      way["railway"="yard"]({bbox});
      node["amenity"="parking"]["hgv"="yes"]({bbox});
      relation["landuse"="industrial"]({bbox});
    );
    out center meta;
    """
    
    result_data = []
    
    try:
        response = requests.post(
            settings.overpass_api,
            data=overpass_query,
            timeout=120
        )
        response.raise_for_status()
        data = response.json()
        
        elements = data.get("elements", [])
        logger.info(f"Retrieved {len(elements)} OSM elements")
        
        for element in elements:
            tags = element.get("tags", {})
            
            # Determine flags
            depot_flag = False
            yard_flag = False
            terminal_flag = False
            
            if tags.get("amenity") == "bus_station" or tags.get("public_transport") == "stop_position":
                depot_flag = True
            if tags.get("railway") == "yard" or tags.get("landuse") == "industrial":
                yard_flag = True
            if tags.get("aeroway") == "apron" or tags.get("man_made") == "works":
                terminal_flag = True
            if tags.get("amenity") == "parking" and tags.get("hgv") == "yes":
                terminal_flag = True
            
            if not (depot_flag or yard_flag or terminal_flag):
                continue
            
            # Get coordinates
            if "center" in element:
                lat = element["center"].get("lat")
                lon = element["center"].get("lon")
            elif "lat" in element and "lon" in element:
                lat = element.get("lat")
                lon = element.get("lon")
            else:
                continue
            
            name = tags.get("name", tags.get("operator", "Unknown"))
            address = tags.get("addr:full") or f"{tags.get('addr:street', '')}, {tags.get('addr:city', '')}"
            
            result_data.append({
                "name": name,
                "address": address,
                "lat": lat,
                "lon": lon,
                "osm_type": element.get("type", "unknown"),
                "osm_id": element.get("id"),
                "depot_flag": depot_flag,
                "yard_flag": yard_flag,
                "terminal_flag": terminal_flag,
                "source": "osm"
            })
    
    except Exception as e:
        logger.error(f"Error querying Overpass API: {e}")
        return pd.DataFrame()
    
    result_df = pd.DataFrame(result_data)
    logger.info(f"Processed {len(result_df)} OSM features")
    
    # Cache to JSON
    settings.cache_osm_dir.mkdir(parents=True, exist_ok=True)
    cache_path = settings.cache_osm_dir / "osm_features.json"
    with open(cache_path, "w") as f:
        json.dump(result_data, f, indent=2)
    logger.info(f"Cached to {cache_path}")
    
    # Persist to DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_osm (
            name VARCHAR,
            address VARCHAR,
            lat DOUBLE,
            lon DOUBLE,
            osm_type VARCHAR,
            osm_id BIGINT,
            depot_flag BOOLEAN,
            yard_flag BOOLEAN,
            terminal_flag BOOLEAN,
            source VARCHAR
        )
    """)
    
    if not result_df.empty:
        conn.register('result_df', result_df)
        conn.execute("DROP TABLE IF EXISTS raw_osm")
        conn.execute("CREATE TABLE raw_osm AS SELECT * FROM result_df")
    
    conn.close()
    
    logger.info(f"Persisted {len(result_df)} rows to DuckDB table raw_osm")
    
    return result_df
