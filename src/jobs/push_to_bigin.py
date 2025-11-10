"""Push leads to Bigin CRM job."""
import argparse
import json
import logging
import pandas as pd
import duckdb
from datetime import datetime, timezone
from pathlib import Path
from src.config import settings
from src.crm.bigin import BiginClient
from src.crm.sync import upsert_to_bigin
from src.crm.sync import is_synced
from src.crm.payloads import build_account_payload

# Setup structured JSON logging
log_dir = Path("./logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "push_to_bigin.log"

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
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


def load_talk_track(track_type: str) -> str:
    """
    Load talk track from docs/talk_tracks.md.
    
    Args:
        track_type: Type of talk track (fleet, generator, cold_storage, pre_storm)
    
    Returns:
        Talk track text
    """
    # TODO: Implement talk track loading from docs/talk_tracks.md
    return f"Talk track for {track_type}"


def main():
    """Main entry point for Bigin sync job."""
    parser = argparse.ArgumentParser(description="Push leads to Bigin CRM")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Dry run mode: print what would be created without calling API"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of records to process"
    )
    parser.add_argument(
        "--entity-ids",
        type=str,
        help="Comma-separated list of entity IDs to sync (overrides filters)"
    )
    args = parser.parse_args()
    
    logger.info("Starting Bigin sync job...")
    
    # Load scored entities
    conn = duckdb.connect(settings.duckdb_path)
    
    # Get entities with scores, filter for Tier A and B
    if args.entity_ids:
        # Sync specific entity IDs
        entity_ids_list = [id.strip() for id in args.entity_ids.split(",")]
        # Use parameterized query for safety
        placeholders = ",".join(["?" for _ in entity_ids_list])
        query = f"""
        SELECT 
            e.*, 
            s.score, 
            s.tier, 
            s.reason_codes, 
            s.reason_text,
            sig_sector.signal_value as sector_primary,
            e.sector_confidence
        FROM raw_pa_tanks e
        LEFT JOIN lead_score s ON e.facility_id = s.entity_id
        LEFT JOIN signals sig_sector ON CAST(e.facility_id AS VARCHAR) = CAST(sig_sector.entity_id AS VARCHAR) AND sig_sector.signal_type = 'sector'
        WHERE e.facility_id IN ({placeholders})
        ORDER BY s.score DESC
        """
        logger.info(f"Syncing specific entity IDs: {len(entity_ids_list)} records")
        entities_df = conn.execute(query, entity_ids_list).df()
    else:
        # Use tier filter
        limit_clause = f"LIMIT {args.limit}" if args.limit else ""
        query = f"""
        SELECT 
            e.*, 
            s.score, 
            s.tier, 
            s.reason_codes, 
            s.reason_text,
            sig_sector.signal_value as sector_primary,
            e.sector_confidence
        FROM raw_pa_tanks e
        LEFT JOIN lead_score s ON e.facility_id = s.entity_id
        LEFT JOIN signals sig_sector ON CAST(e.facility_id AS VARCHAR) = CAST(sig_sector.entity_id AS VARCHAR) AND sig_sector.signal_type = 'sector'
        WHERE s.tier IN ('Tier A', 'Tier B')
        ORDER BY s.score DESC
        {limit_clause}
        """
        entities_df = conn.execute(query).df()
    conn.close()
    
    if entities_df.empty:
        logger.warning("No Tier A or B entities found")
        return
    
    # Filter out already synced
    entities_df = entities_df[
        ~entities_df["facility_id"].apply(lambda x: is_synced(x, settings.duckdb_path))
    ]
    
    if args.dry_run:
        logger.info(f"DRY RUN: Would sync {len(entities_df)} entities to Bigin...")
        
        # Count by type
        account_count = len(entities_df)
        logger.info(f"  Would create: {account_count} Accounts")
        
        # Show top 3 payload examples
        logger.info("  Top 3 payload examples:")
        for idx, (_, entity) in enumerate(entities_df.head(3).iterrows()):
            entity_dict = entity.to_dict()
            payload = build_account_payload(
                account_name=entity_dict.get("facility_name", "Unknown"),
                lead_score=entity_dict.get("score"),
                reason_codes=entity_dict.get("reason_codes"),
                tank_capacity_bucket=entity_dict.get("capacity_bucket"),
                fleet_size=entity_dict.get("fleet_size"),
                generator_flag=entity_dict.get("has_generator", False),
                sector_primary=entity_dict.get("sector_primary"),
                sector_confidence=int(entity_dict.get("sector_confidence", 0)) if entity_dict.get("sector_confidence") else None,
                Billing_Street=entity_dict.get("address"),
                Billing_City=entity_dict.get("city"),
                Billing_State=entity_dict.get("state"),
                Billing_Code=entity_dict.get("zip"),
            )
            logger.info(f"    Example {idx+1}: {payload}")
        
        return
    
    logger.info(f"Syncing {len(entities_df)} entities to Bigin...")
    
    client = BiginClient()
    
    synced_count = 0
    for _, entity in entities_df.iterrows():
        entity_dict = entity.to_dict()
        crm_id = upsert_to_bigin(entity_dict, client)
        if crm_id:
            synced_count += 1
        
        # TODO: Create call tasks with talk tracks
    
    logger.info(f"Sync complete: {synced_count} entities synced to Bigin")


if __name__ == "__main__":
    main()

