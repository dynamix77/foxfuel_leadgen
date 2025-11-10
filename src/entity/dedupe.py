"""Entity deduplication module."""
import logging
import pandas as pd
from rapidfuzz import fuzz
try:
    import pygeohash as pgh
except ImportError:
    # Fallback if pygeohash not available
    pgh = None

logger = logging.getLogger(__name__)


def cluster_by_geohash(df: pd.DataFrame, precision: int = 7) -> pd.DataFrame:
    """
    Add geohash column for clustering.
    
    Args:
        df: DataFrame with latitude and longitude columns
        precision: Geohash precision (default: 7)
    
    Returns:
        DataFrame with geohash column
    """
    def get_geohash(row):
        if pgh is None:
            return None
        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
            try:
                return pgh.encode(row['latitude'], row['longitude'], precision=precision)
            except Exception:
                return None
        return None
    
    df['geohash'] = df.apply(get_geohash, axis=1)
    return df


def dedupe_entities(df: pd.DataFrame, similarity_threshold: float = 90.0) -> pd.DataFrame:
    """
    Deduplicate entities by geohash and name similarity.
    
    Args:
        df: DataFrame with entity data
        similarity_threshold: Minimum name similarity score (0-100)
    
    Returns:
        Deduplicated DataFrame keeping most complete record per cluster
    """
    logger.info(f"Deduplicating {len(df)} entities...")
    
    # Add geohash
    df = cluster_by_geohash(df, precision=7)
    
    # Group by geohash
    deduplicated = []
    processed = set()
    
    for geohash, group in df.groupby('geohash'):
        if pd.isna(geohash):
            # Keep all records without geohash
            deduplicated.extend(group.to_dict('records'))
            continue
        
        group_list = group.to_dict('records')
        
        # Cluster by name similarity within geohash
        clusters = []
        for record in group_list:
            name = record.get('facility_name', '')
            if not name:
                clusters.append([record])
                continue
            
            matched = False
            for cluster in clusters:
                cluster_name = cluster[0].get('facility_name', '')
                if cluster_name:
                    similarity = fuzz.ratio(name.upper(), cluster_name.upper())
                    if similarity >= similarity_threshold:
                        cluster.append(record)
                        matched = True
                        break
            
            if not matched:
                clusters.append([record])
        
        # Keep most complete record from each cluster
        for cluster in clusters:
            # Score by number of non-null fields
            best_record = max(cluster, key=lambda r: sum(1 for v in r.values() if pd.notna(v) and v))
            deduplicated.append(best_record)
    
    result_df = pd.DataFrame(deduplicated)
    logger.info(f"Deduplication complete: {len(df)} -> {len(result_df)} entities")
    
    return result_df

