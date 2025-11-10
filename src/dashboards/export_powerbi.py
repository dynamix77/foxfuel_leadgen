"""Power BI export module."""
import logging
import pandas as pd
import duckdb
import json
from datetime import datetime
from pathlib import Path
from src.config import settings

logger = logging.getLogger(__name__)


def export_star_schema():
    """
    Export star schema CSVs for Power BI.
    
    Exports:
    - entity (fact table)
    - signals (dimension)
    - lead_score (dimension)
    - crm_sync (bridge)
    """
    logger.info("Exporting star schema for Power BI...")
    
    conn = duckdb.connect(settings.duckdb_path)
    
    # Entity fact table - join with sector signals and distance
    try:
        # Calculate distance from base address
        base_lat, base_lon = 40.144, -75.128  # Approximate for Willow Grove, PA
        entity_df = conn.execute(f"""
            SELECT 
                e.*,
                COALESCE(s.sector_primary, '') as sector_primary,
                COALESCE(e.sector_confidence, 0) as sector_confidence,
                CASE 
                    WHEN e.latitude IS NOT NULL AND e.longitude IS NOT NULL THEN
                        69.0 * acos(
                            sin(radians({base_lat})) * sin(radians(e.latitude)) +
                            cos(radians({base_lat})) * cos(radians(e.latitude)) *
                            cos(radians(e.longitude - {base_lon}))
                        )
                    ELSE NULL
                END as distance_mi
            FROM raw_pa_tanks e
            LEFT JOIN (
                SELECT entity_id, signal_value as sector_primary
                FROM signals
                WHERE signal_type = 'sector'
            ) s ON e.facility_id = s.entity_id
        """).df()
    except Exception as e:
        logger.warning(f"Error joining sector signals: {e}, using base entity table")
        entity_df = conn.execute("SELECT * FROM raw_pa_tanks").df()
        entity_df['sector_primary'] = None
        entity_df['sector_confidence'] = 0
        entity_df['distance_mi'] = None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    entity_path = settings.out_dir / f"entity_{timestamp}.csv"
    # Select only required columns for export
    export_cols = ['facility_id', 'facility_name', 'address', 'city', 'state', 'zip', 
                   'county', 'latitude', 'longitude', 'sector_primary', 'sector_confidence', 'distance_mi']
    export_cols = [c for c in export_cols if c in entity_df.columns]
    entity_df[export_cols].to_csv(entity_path, index=False, encoding='utf-8')
    logger.info(f"Exported {len(entity_df)} entities to {entity_path}")
    
    # Lead score dimension
    try:
        score_df = conn.execute("""
            SELECT 
                entity_id,
                score,
                tier,
                reason_text as reasons_str,
                reason_codes as reasons_codes
            FROM lead_score
        """).df()
        # Rename tier to band for export
        if 'tier' in score_df.columns:
            score_df = score_df.rename(columns={'tier': 'band'})
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        score_path = settings.out_dir / f"lead_score_{timestamp}.csv"
        score_df.to_csv(score_path, index=False, encoding='utf-8')
        logger.info(f"Exported {len(score_df)} scores to {score_path}")
    except Exception:
        logger.warning("lead_score table not found, skipping")
    
    # Signals dimension - load from DuckDB if available
    try:
        # Get all signal types including sector
        signals_df = conn.execute("""
            SELECT DISTINCT 
                signal_type as signal_id,
                signal_type as signal_name,
                CASE 
                    WHEN signal_type = 'sector' THEN 'sector'
                    WHEN signal_type = 'tank' THEN 'tank'
                    WHEN signal_type = 'status' THEN 'status'
                    WHEN signal_type = 'fleet' THEN 'fleet'
                    ELSE 'infrastructure'
                END as signal_category
            FROM signals
        """).df()
        
        # Also include sector signal entries per matched entity
        sector_signals_df = conn.execute("""
            SELECT 
                CONCAT(entity_id, '_sector') as signal_id,
                'Sector' as signal_name,
                'sector' as signal_category,
                entity_id,
                signal_value as sector_value
            FROM signals
            WHERE signal_type = 'sector'
        """).df()
        
        # Add default signals if table is empty
        if signals_df.empty:
            signals_df = pd.DataFrame({
                "signal_id": ["diesel_tank", "active", "fmcsa", "hospital", "school", "sector"],
                "signal_name": ["Diesel Tank", "Active Facility", "FMCSA Fleet", "Hospital", "School", "Sector"],
                "signal_category": ["tank", "status", "fleet", "infrastructure", "infrastructure", "sector"]
            })
    except Exception:
        # Fallback to placeholder
        signals_df = pd.DataFrame({
            "signal_id": ["diesel_tank", "active", "fmcsa", "hospital", "school", "sector"],
            "signal_name": ["Diesel Tank", "Active Facility", "FMCSA Fleet", "Hospital", "School", "Sector"],
            "signal_category": ["tank", "status", "fleet", "infrastructure", "infrastructure", "sector"]
        })
        sector_signals_df = pd.DataFrame()
    # Export signals with entity_id, signal_type, value, as_of
    try:
        signals_export_df = conn.execute("""
            SELECT 
                entity_id,
                signal_type,
                signal_value as value,
                created_at as as_of
            FROM signals
        """).df()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        signals_path = settings.out_dir / f"signals_{timestamp}.csv"
        signals_export_df.to_csv(signals_path, index=False, encoding='utf-8')
        logger.info(f"Exported {len(signals_export_df)} signals to {signals_path}")
    except Exception:
        logger.warning("No signals to export")
    
    # CRM sync bridge
    try:
        sync_df = conn.execute("SELECT * FROM crm_sync").df()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        sync_path = settings.out_dir / f"crm_sync_{timestamp}.csv"
        sync_df.to_csv(sync_path, index=False, encoding='utf-8')
        logger.info(f"Exported {len(sync_df)} sync records to {sync_path}")
    except Exception:
        logger.warning("crm_sync table not found, skipping")
    
    conn.close()


def export_tier_a_geojson():
    """Export Tier A sites as GeoJSON for mapping."""
    logger.info("Exporting Tier A GeoJSON...")
    
    conn = duckdb.connect(settings.duckdb_path)
    
    # Get Tier A entities with coordinates
    query = """
        SELECT 
            e.facility_id,
            e.facility_name,
            e.address,
            e.city,
            e.state,
            e.latitude,
            e.longitude,
            s.score,
            s.tier,
            s.reason_text
        FROM raw_pa_tanks e
        LEFT JOIN lead_score s ON e.facility_id = s.entity_id
        WHERE s.tier = 'Tier A'
        AND e.latitude IS NOT NULL
        AND e.longitude IS NOT NULL
    """
    
    df = conn.execute(query).df()
    conn.close()
    
    if df.empty:
        logger.warning("No Tier A entities with coordinates found")
        return
    
    # Build GeoJSON
    features = []
    for _, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]]
            },
            "properties": {
                "facility_id": row["facility_id"],
                "facility_name": row["facility_name"],
                "address": row["address"],
                "city": row["city"],
                "state": row["state"],
                "score": int(row["score"]) if pd.notna(row["score"]) else None,
                "tier": row["tier"],
                "reason_text": row["reason_text"]
            }
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    # Write timestamped GeoJSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    geojson_path = settings.out_dir / f"tierA_{timestamp}.geojson"
    with open(geojson_path, "w", encoding='utf-8') as f:
        json.dump(geojson, f, indent=2)
    
    logger.info(f"Exported {len(features)} Tier A sites to {geojson_path}")
    
    # Also write Tier A points CSV for quick map imports
    if not df.empty:
        # Reconnect to get additional fields from entity table
        conn2 = duckdb.connect(settings.duckdb_path)
        base_lat, base_lon = 40.144, -75.128  # Approximate for Willow Grove, PA
        entity_df_full = conn2.execute(f"""
            SELECT 
                e.facility_id,
                e.facility_name,
                e.county,
                e.latitude,
                e.longitude,
                e.sector_confidence,
                s.score,
                s.tier as band,
                COALESCE(sig.signal_value, '') as sector_primary,
                CASE 
                    WHEN e.latitude IS NOT NULL AND e.longitude IS NOT NULL THEN
                        69.0 * acos(
                            sin(radians({base_lat})) * sin(radians(e.latitude)) +
                            cos(radians({base_lat})) * cos(radians(e.latitude)) *
                            cos(radians(e.longitude - {base_lon}))
                        )
                    ELSE NULL
                END as distance_mi
            FROM raw_pa_tanks e
            LEFT JOIN lead_score s ON e.facility_id = s.entity_id
            LEFT JOIN signals sig ON e.facility_id = sig.entity_id AND sig.signal_type = 'sector'
            WHERE s.tier = 'Tier A'
            AND e.latitude IS NOT NULL
            AND e.longitude IS NOT NULL
        """).df()
        conn2.close()
        
        if not entity_df_full.empty:
            points_df = entity_df_full[['latitude', 'longitude', 'facility_name', 'county', 'score', 'band', 'sector_primary', 'distance_mi']].copy()
            points_df.columns = ['latitude', 'longitude', 'facility_name', 'county', 'score', 'band', 'sector_primary', 'distance_mi']
            points_path = settings.out_dir / f"tierA_points_{timestamp}.csv"
            points_df.to_csv(points_path, index=False, encoding='utf-8')
            logger.info(f"Exported {len(points_df)} Tier A points to {points_path}")


def export_opportunities():
    """Export procurement and permits opportunities."""
    logger.info("Exporting opportunities...")
    
    conn = duckdb.connect(settings.duckdb_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # Export bids
    try:
        bids_df = conn.execute("SELECT * FROM raw_procurement").df()
        if not bids_df.empty:
            bids_path = settings.out_dir / f"opportunities_bids_{timestamp}.csv"
            bids_df.to_csv(bids_path, index=False, encoding='utf-8')
            logger.info(f"Exported {len(bids_df)} bids to {bids_path}")
    except Exception:
        logger.warning("No procurement data to export")
    
    # Export permits
    try:
        permits_df = conn.execute("SELECT * FROM raw_permits").df()
        if not permits_df.empty:
            permits_path = settings.out_dir / f"opportunities_permits_{timestamp}.csv"
            permits_df.to_csv(permits_path, index=False, encoding='utf-8')
            logger.info(f"Exported {len(permits_df)} permits to {permits_path}")
    except Exception:
        logger.warning("No permits data to export")
    
    conn.close()


def main():
    """Main export function."""
    export_star_schema()
    export_tier_a_geojson()
    export_opportunities()
    logger.info("Power BI export complete")


if __name__ == "__main__":
    main()
