"""Build universe job - orchestrates full pipeline."""
import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import settings
from src.ingest.pa_tanks import ingest_pa_tanks
from src.ingest.naics_local import ingest_naics_local
from src.ingest.maps_extractor import ingest_maps_extractor
from src.ingest.fmcsa import ingest_fmcsa
from src.ingest.echo import ingest_echo
from src.ingest.eia_gen import ingest_eia_generators
from src.ingest.osm import ingest_osm
from src.ingest.procurement import ingest_procurement
from src.ingest.permits import ingest_permits
from src.entity.merge import merge_naics_signals, merge_maps_extractor
from src.utils.io import write_preview_csv

# Setup structured JSON logging
log_dir = Path("./logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "build_universe.log"

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName
        }
        if hasattr(record, "duration"):
            log_entry["duration_seconds"] = record.duration
        return json.dumps(log_entry)

# Setup file handler with JSON formatter
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(JSONFormatter())

# Setup console handler with standard format
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)


def init_duckdb_schema():
    """Initialize DuckDB schema idempotently."""
    import duckdb
    conn = duckdb.connect(settings.duckdb_path)
    
    # raw_pa_tanks
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
            distance_mi DOUBLE,
            sector_primary VARCHAR,
            sector_confidence INTEGER,
            naics_code VARCHAR,
            maps_category VARCHAR,
            source VARCHAR
        )
    """)
    
    # entity
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity (
            entity_id VARCHAR PRIMARY KEY,
            facility_name VARCHAR,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip VARCHAR,
            county VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            source VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # signals
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            signal_id VARCHAR PRIMARY KEY,
            entity_id VARCHAR,
            signal_type VARCHAR,
            signal_value VARCHAR,
            source VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # lead_score
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lead_score (
            entity_id VARCHAR PRIMARY KEY,
            score INTEGER,
            tier VARCHAR,
            reason_codes VARCHAR,
            reason_text TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # crm_sync
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_sync (
            entity_id VARCHAR PRIMARY KEY,
            crm_id VARCHAR,
            crm_type VARCHAR,
            synced_at TIMESTAMP,
            sync_status VARCHAR
        )
    """)
    
    # entity_points (spatial index for faster geohash/distance queries)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_points (
            entity_id VARCHAR PRIMARY KEY,
            latitude DOUBLE,
            longitude DOUBLE,
            facility_name VARCHAR
        )
    """)
    
    conn.close()
    logger.info("DuckDB schema initialized")


def main():
    """Main entry point for build_universe job."""
    start_time = datetime.now()
    
    parser = argparse.ArgumentParser(description="Build lead generation universe")
    parser.add_argument(
        "--input",
        "--pa-tanks-path",
        type=str,
        dest="pa_tanks_path",
        required=True,
        help="Path to PA tanks CSV or XLSX file"
    )
    parser.add_argument(
        "--skip-geocode",
        action="store_true",
        help="Skip geocoding (faster for testing, sets confidence='skipped')"
    )
    parser.add_argument(
        "--counties",
        type=str,
        help="Comma-separated list of counties to filter (overrides config)"
    )
    parser.add_argument(
        "--base-address",
        type=str,
        help="Base address for distance calculations (overrides config)"
    )
    parser.add_argument(
        "--naics-local-path",
        type=str,
        help="Path to NAICS local CSV file (overrides config)"
    )
    parser.add_argument(
        "--geocode-limit",
        type=int,
        help="Maximum records to geocode this run (default: no limit)"
    )
    parser.add_argument(
        "--geocode-qps",
        type=float,
        default=5.0,
        help="Geocoding QPS throttle (default: 5.0)"
    )
    parser.add_argument(
        "--geocode-batch-size",
        type=int,
        default=100,
        help="Batch size for cache inserts (default: 100)"
    )
    parser.add_argument(
        "--qa",
        action="store_true",
        help="Generate QA report after build"
    )
    parser.add_argument(
        "--skip-fmcsa",
        action="store_true",
        help="Skip FMCSA ingestion"
    )
    parser.add_argument(
        "--skip-echo",
        action="store_true",
        help="Skip ECHO ingestion"
    )
    parser.add_argument(
        "--skip-eia",
        action="store_true",
        help="Skip EIA generator ingestion"
    )
    parser.add_argument(
        "--skip-osm",
        action="store_true",
        help="Skip OSM ingestion"
    )
    parser.add_argument(
        "--skip-procurement",
        action="store_true",
        help="Skip procurement ingestion"
    )
    parser.add_argument(
        "--skip-permits",
        action="store_true",
        help="Skip permits ingestion"
    )
    parser.add_argument(
        "--maps-extractor-glob",
        type=str,
        help="Glob pattern for Maps Extractor CSV files (e.g. ./data/maps_extractor/*.csv)"
    )
    
    args = parser.parse_args()
    
    # Override settings if provided
    if args.counties:
        settings.counties = [c.strip() for c in args.counties.split(",")]
        logger.info(f"Using counties from CLI: {settings.counties}")
    
    if args.base_address:
        settings.base_address = args.base_address
        logger.info(f"Using base address from CLI: {settings.base_address}")
    
    if args.naics_local_path:
        settings.naics_local_path = Path(args.naics_local_path)
        logger.info(f"Using NAICS path from CLI: {settings.naics_local_path}")
    
    # Validate file exists
    pa_tanks_path = Path(args.pa_tanks_path)
    if not pa_tanks_path.exists():
        logger.error(f"File not found: {pa_tanks_path}")
        sys.exit(1)
    
    maps_df = pd.DataFrame()

    try:
        # Initialize schema
        init_duckdb_schema()
        
        # Initialize geocode cache
        from src.utils.geocode import init_geocode_cache
        init_geocode_cache(settings.duckdb_path)
        
        # Ingest PA tanks
        ingest_start = datetime.now()
        logger.info("Starting PA tanks ingestion...")
        df = ingest_pa_tanks(
            str(pa_tanks_path),
            geocode=not args.skip_geocode,
            skip_geocode=args.skip_geocode,
            geocode_limit=args.geocode_limit,
            geocode_qps=args.geocode_qps,
            geocode_batch_size=args.geocode_batch_size
        )
        ingest_duration = (datetime.now() - ingest_start).total_seconds()
        rows_per_sec = len(df) / ingest_duration if ingest_duration > 0 else 0
        ingested_count = len(df)
        geocoded_count = df['latitude'].notna().sum() if 'latitude' in df.columns else 0
        logger.info(f"PA tanks ingestion completed in {ingest_duration:.2f} seconds ({rows_per_sec:.1f} rows/sec)", extra={"duration": ingest_duration})
        logger.info(f"Ingested: {ingested_count} rows, Geocoded: {geocoded_count} rows")
        
        # Ingest NAICS local data
        if settings.naics_local_path.exists():
            naics_start = datetime.now()
            logger.info("Starting NAICS local ingestion...")
            naics_df = ingest_naics_local(geocode=not args.skip_geocode, skip_geocode=args.skip_geocode)
            naics_duration = (datetime.now() - naics_start).total_seconds()
            logger.info(f"NAICS ingestion completed in {naics_duration:.2f} seconds", extra={"duration": naics_duration})
        else:
            logger.warning(f"NAICS file not found at {settings.naics_local_path}, skipping")

        # Ingest Maps Extractor data
        maps_start = datetime.now()
        logger.info("Starting Maps Extractor ingestion...")
        if args.maps_extractor_glob:
            maps_df = ingest_maps_extractor(args.maps_extractor_glob)
        else:
            maps_df = ingest_maps_extractor()
        maps_duration = (datetime.now() - maps_start).total_seconds()
        logger.info(f"Maps Extractor ingestion completed in {maps_duration:.2f} seconds", extra={"duration": maps_duration})
        
        # Update spatial index
        spatial_start = datetime.now()
        logger.info("Updating spatial index...")
        import duckdb
        conn = duckdb.connect(settings.duckdb_path)
        conn.execute("DELETE FROM signals WHERE signal_type = 'sector_confidence'")
        conn.register('df_spatial', df[['facility_id', 'latitude', 'longitude', 'facility_name']].copy())
        conn.execute("""
            INSERT OR REPLACE INTO entity_points 
            SELECT facility_id, latitude, longitude, facility_name
            FROM df_spatial
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        conn.close()
        spatial_duration = (datetime.now() - spatial_start).total_seconds()
        logger.info(f"Spatial index updated in {spatial_duration:.2f} seconds", extra={"duration": spatial_duration})
        
        # Ingest additional data sources
        if not args.skip_fmcsa:
            fmcsa_start = datetime.now()
            logger.info("Starting FMCSA ingestion...")
            fmcsa_df = ingest_fmcsa()
            fmcsa_duration = (datetime.now() - fmcsa_start).total_seconds()
            logger.info(f"FMCSA ingestion completed in {fmcsa_duration:.2f} seconds", extra={"duration": fmcsa_duration})
        
        if not args.skip_echo:
            echo_start = datetime.now()
            logger.info("Starting ECHO ingestion...")
            echo_df = ingest_echo()
            echo_duration = (datetime.now() - echo_start).total_seconds()
            logger.info(f"ECHO ingestion completed in {echo_duration:.2f} seconds", extra={"duration": echo_duration})
        
        if not args.skip_eia:
            eia_start = datetime.now()
            logger.info("Starting EIA generator ingestion...")
            eia_df = ingest_eia_generators()
            eia_duration = (datetime.now() - eia_start).total_seconds()
            logger.info(f"EIA ingestion completed in {eia_duration:.2f} seconds", extra={"duration": eia_duration})
        
        if not args.skip_osm:
            osm_start = datetime.now()
            logger.info("Starting OSM ingestion...")
            osm_df = ingest_osm()
            osm_duration = (datetime.now() - osm_start).total_seconds()
            logger.info(f"OSM ingestion completed in {osm_duration:.2f} seconds", extra={"duration": osm_duration})
        
        if not args.skip_procurement:
            procurement_start = datetime.now()
            logger.info("Starting procurement ingestion...")
            procurement_df = ingest_procurement()
            procurement_duration = (datetime.now() - procurement_start).total_seconds()
            logger.info(f"Procurement ingestion completed in {procurement_duration:.2f} seconds", extra={"duration": procurement_duration})
        
        if not args.skip_permits:
            permits_start = datetime.now()
            logger.info("Starting permits ingestion...")
            permits_df = ingest_permits()
            permits_duration = (datetime.now() - permits_start).total_seconds()
            logger.info(f"Permits ingestion completed in {permits_duration:.2f} seconds", extra={"duration": permits_duration})
        
        # Merge NAICS sector signals into entities
        merge_start = datetime.now()
        logger.info("Merging NAICS sector signals...")
        naics_loaded = 0
        try:
            conn_naics = duckdb.connect(settings.duckdb_path)
            naics_check = conn_naics.execute("SELECT COUNT(*) FROM raw_naics_local").fetchone()[0]
            naics_loaded = naics_check
            conn_naics.close()
        except Exception:
            pass
        df = merge_naics_signals(df)
        merge_duration = (datetime.now() - merge_start).total_seconds()
        naics_matches = df['sector_primary'].notna().sum() if 'sector_primary' in df.columns else 0
        logger.info(f"NAICS merge completed in {merge_duration:.2f} seconds: {naics_loaded} NAICS rows loaded, {naics_matches} entities matched", extra={"duration": merge_duration})
        
        # Merge Maps Extractor data if available
        maps_merge_start = datetime.now()
        if not maps_df.empty:
            df = merge_maps_extractor(df, maps_df)
            maps_merge_duration = (datetime.now() - maps_merge_start).total_seconds()
            logger.info(f"Maps Extractor merge completed in {maps_merge_duration:.2f} seconds", extra={"duration": maps_merge_duration})
        else:
            logger.info("No Maps Extractor data provided, skipping merge")
        
        # Attach signals from other sources to signals table
        signals_start = datetime.now()
        logger.info("Attaching signals from additional sources...")
        conn = duckdb.connect(settings.duckdb_path)
        
        # Attach signals from PA tanks
        conn.execute("""
            INSERT OR REPLACE INTO signals
            SELECT 
                CONCAT(facility_id, '_diesel_like') as signal_id,
                facility_id as entity_id,
                'diesel_like' as signal_type,
                CAST(is_diesel_like AS VARCHAR) as signal_value,
                'pa_tanks' as source,
                CURRENT_TIMESTAMP
            FROM raw_pa_tanks
        """)
        
        conn.execute("""
            INSERT OR REPLACE INTO signals
            SELECT 
                CONCAT(facility_id, '_active_like') as signal_id,
                facility_id as entity_id,
                'active_like' as signal_type,
                CAST(is_active_like AS VARCHAR) as signal_value,
                'pa_tanks' as source,
                CURRENT_TIMESTAMP
            FROM raw_pa_tanks
        """)
        
        conn.execute("""
            INSERT OR REPLACE INTO signals
            SELECT 
                CONCAT(facility_id, '_capacity_bucket') as signal_id,
                facility_id as entity_id,
                'capacity_bucket' as signal_type,
                capacity_bucket as signal_value,
                'pa_tanks' as source,
                CURRENT_TIMESTAMP
            FROM raw_pa_tanks
            WHERE capacity_bucket IS NOT NULL
        """)
        
        # Attach ECHO signals
        try:
            conn.execute("""
                INSERT OR REPLACE INTO signals
                SELECT 
                    CONCAT(frs_id, '_echo') as signal_id,
                    frs_id as entity_id,
                    'echo' as signal_type,
                    'true' as signal_value,
                    'echo' as source,
                    CURRENT_TIMESTAMP
                FROM raw_echo
            """)
        except Exception:
            pass
        
        # Attach EIA generator signals
        try:
            conn.execute("""
                INSERT OR REPLACE INTO signals
                SELECT 
                    CONCAT(plant_name, '_eia_gen') as signal_id,
                    plant_name as entity_id,
                    'eia_gen' as signal_type,
                    'true' as signal_value,
                    'eia' as source,
                    CURRENT_TIMESTAMP
                FROM raw_eia
            """)
        except Exception:
            pass
        
        # Attach OSM depot signals
        try:
            conn.execute("""
                INSERT OR REPLACE INTO signals
                SELECT 
                    CONCAT(CAST(osm_id AS VARCHAR), '_osm_depot') as signal_id,
                    CAST(osm_id AS VARCHAR) as entity_id,
                    'osm_depot' as signal_type,
                    'true' as signal_value,
                    'osm' as source,
                    CURRENT_TIMESTAMP
                FROM raw_osm
                WHERE depot_flag = true OR yard_flag = true OR terminal_flag = true
            """)
        except Exception:
            pass
        
        conn.close()
        signals_duration = (datetime.now() - signals_start).total_seconds()
        logger.info(f"Signals attachment completed in {signals_duration:.2f} seconds", extra={"duration": signals_duration})
        
        # Generate QA report if requested
        if args.qa:
            from src.jobs.qa_report import generate_qa_report
            qa_start = datetime.now()
            logger.info("Generating QA report...")
            generate_qa_report()
            qa_duration = (datetime.now() - qa_start).total_seconds()
            logger.info(f"QA report completed in {qa_duration:.2f} seconds", extra={"duration": qa_duration})
        
        # Write timestamped preview CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        preview_path = settings.out_dir / f"pa_tanks_preview_{timestamp}.csv"
        write_preview_csv(df, preview_path, max_rows=1000)
        logger.info(f"Preview CSV written to {preview_path}")
        
        # Summary
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info("Ingestion Summary:")
        logger.info(f"  Total rows: {len(df)}")
        logger.info(f"  Diesel-like: {df['is_diesel_like'].sum()}")
        logger.info(f"  Active: {df['is_active_like'].sum()}")
        logger.info(f"  Geocoded: {df['latitude'].notna().sum()}")
        logger.info(f"  Capacity buckets:")
        for bucket, count in df['capacity_bucket'].value_counts().items():
            logger.info(f"    {bucket}: {count}")
        logger.info(f"  Total duration: {total_duration:.2f} seconds")
        logger.info("=" * 60, extra={"duration": total_duration})
        
    except Exception as e:
        logger.error(f"Error during ingestion: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

