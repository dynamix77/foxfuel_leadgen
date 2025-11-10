"""Geocoding utilities with caching."""
import logging
import time
from typing import Optional, Tuple, Dict
import duckdb
import googlemaps
from tenacity import retry, stop_after_attempt, wait_exponential
from src.config import settings

logger = logging.getLogger(__name__)

# Initialize Google Maps client
_gmaps_client: Optional[googlemaps.Client] = None

# Rate limiting globals
_min_request_interval: float = 1.0 / 5.0  # Default 5 QPS
_last_request_time: float = 0.0


def get_gmaps_client() -> googlemaps.Client:
    """Get or create Google Maps client."""
    global _gmaps_client
    if _gmaps_client is None:
        if not settings.google_maps_api_key:
            raise ValueError("GOOGLE_MAPS_API_KEY not set in environment")
        _gmaps_client = googlemaps.Client(key=settings.google_maps_api_key)
    return _gmaps_client


def init_geocode_cache(db_path: str):
    """Initialize geocoding cache table in DuckDB."""
    conn = duckdb.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS geocode_cache (
            address_hash VARCHAR PRIMARY KEY,
            address TEXT,
            latitude DOUBLE,
            longitude DOUBLE,
            confidence VARCHAR,
            cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.close()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def set_geocode_qps(qps: float):
    """Set geocoding QPS limit."""
    global _min_request_interval
    if qps > 0:
        _min_request_interval = 1.0 / qps
    else:
        _min_request_interval = 0.0


def geocode_address(address: str, db_path: Optional[str] = None, skip: bool = False) -> Tuple[Optional[float], Optional[float], str]:
    """
    Geocode an address using Google Maps API with caching.
    
    Args:
        address: Address string to geocode
        db_path: Path to DuckDB database (uses settings if not provided)
        skip: If True, skip geocoding and return skipped status
    
    Returns:
        Tuple of (latitude, longitude, confidence) or (None, None, "failed")
    """
    if skip:
        return None, None, "skipped"
    
    if not address or not address.strip():
        return None, None, "empty"
    
    db_path = db_path or settings.duckdb_path
    init_geocode_cache(db_path)
    
    # Create hash for caching
    import hashlib
    address_hash = hashlib.md5(address.encode()).hexdigest()
    
    # Check cache
    conn = duckdb.connect(db_path)
    cached = conn.execute(
        "SELECT latitude, longitude, confidence FROM geocode_cache WHERE address_hash = ?",
        [address_hash]
    ).fetchone()
    
    if cached:
        conn.close()
        logger.debug(f"Cache hit for address: {address[:50]}...")
        return cached[0], cached[1], cached[2] or "cached"
    
    # Geocode via API with rate limiting
    try:
        global _last_request_time, _min_request_interval
        if _min_request_interval > 0:
            elapsed = time.time() - _last_request_time
            if elapsed < _min_request_interval:
                time.sleep(_min_request_interval - elapsed)
            _last_request_time = time.time()
        
        client = get_gmaps_client()
        result = client.geocode(address)
        
        if result and len(result) > 0:
            location = result[0]['geometry']['location']
            lat = location['lat']
            lng = location['lng']
            
            # Determine confidence
            location_type = result[0]['geometry'].get('location_type', '')
            if location_type == 'ROOFTOP':
                confidence = "high"
            elif location_type in ['RANGE_INTERPOLATED', 'GEOMETRIC_CENTER']:
                confidence = "medium"
            else:
                confidence = "low"
            
            # Cache result
            conn.execute(
                """
                INSERT OR REPLACE INTO geocode_cache 
                (address_hash, address, latitude, longitude, confidence)
                VALUES (?, ?, ?, ?, ?)
                """,
                [address_hash, address, lat, lng, confidence]
            )
            conn.close()
            
            logger.debug(f"Geocoded: {address[:50]}... -> ({lat}, {lng})")
            return lat, lng, confidence
        else:
            conn.close()
            logger.warning(f"No results for address: {address[:50]}...")
            return None, None, "no_results"
    
    except Exception as e:
        conn.close()
        logger.error(f"Geocoding error for {address[:50]}...: {e}")
        return None, None, "error"


def batch_geocode(
    addresses: list,
    db_path: Optional[str] = None
) -> Dict[str, Tuple[Optional[float], Optional[float], str]]:
    """
    Geocode multiple addresses with caching.
    
    Args:
        addresses: List of address strings
        db_path: Path to DuckDB database
    
    Returns:
        Dict mapping address to (lat, lng, confidence)
    """
    results = {}
    for address in addresses:
        results[address] = geocode_address(address, db_path)
    return results

