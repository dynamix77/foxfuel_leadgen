"""CRM sync module with idempotency."""
import logging
import pandas as pd
import duckdb
from typing import Dict, Optional
from src.config import settings
from src.crm.bigin import BiginClient
from src.crm.payloads import build_account_payload

logger = logging.getLogger(__name__)


def init_sync_table(db_path: str):
    """Initialize CRM sync tracking table."""
    conn = duckdb.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_sync (
            entity_id VARCHAR PRIMARY KEY,
            crm_id VARCHAR,
            crm_type VARCHAR,
            synced_at TIMESTAMP,
            sync_status VARCHAR
        )
    """)
    conn.close()


def is_synced(entity_id: str, db_path: str) -> bool:
    """
    Check if entity is already synced to CRM.
    
    Args:
        entity_id: Entity ID to check
        db_path: DuckDB path
    
    Returns:
        True if entity is synced
    """
    conn = duckdb.connect(db_path)
    result = conn.execute(
        "SELECT COUNT(*) FROM crm_sync WHERE entity_id = ? AND sync_status = 'success'",
        [entity_id]
    ).fetchone()
    conn.close()
    return result[0] > 0 if result else False


def record_sync(
    entity_id: str,
    crm_id: str,
    crm_type: str,
    status: str,
    db_path: str
):
    """
    Record sync status in database.
    
    Args:
        entity_id: Entity ID
        crm_id: CRM record ID
        crm_type: CRM record type (Account, Contact, Deal)
        status: Sync status (success, failed, error)
        db_path: DuckDB path
    """
    init_sync_table(db_path)
    conn = duckdb.connect(db_path)
    conn.execute("""
        INSERT OR REPLACE INTO crm_sync 
        (entity_id, crm_id, crm_type, synced_at, sync_status)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)
    """, [entity_id, crm_id, crm_type, status])
    conn.close()


def upsert_to_bigin(
    entity: Dict,
    client: Optional[BiginClient] = None
) -> Optional[str]:
    """
    Upsert entity to Bigin with idempotency.
    
    Args:
        entity: Entity data dictionary
        client: Optional BiginClient instance
    
    Returns:
        CRM record ID if successful, None otherwise
    """
    entity_id = entity.get("facility_id")
    if not entity_id:
        logger.warning("Entity missing facility_id, skipping sync")
        return None
    
    # Check if already synced
    if is_synced(entity_id, settings.duckdb_path):
        logger.debug(f"Entity {entity_id} already synced, skipping")
        return None
    
    if client is None:
        client = BiginClient()
    
    try:
        # Build account payload
        account_payload = build_account_payload(
            account_name=entity.get("facility_name", "Unknown"),
            lead_score=entity.get("score"),
            reason_codes=entity.get("reason_codes"),
            tank_capacity_bucket=entity.get("capacity_bucket"),
            fleet_size=entity.get("fleet_size"),
            generator_flag=entity.get("has_generator", False),
            sector_primary=entity.get("sector_primary"),
            sector_confidence=entity.get("sector_confidence"),
            Billing_Street=entity.get("address"),
            Billing_City=entity.get("city"),
            Billing_State=entity.get("state"),
            Billing_Code=entity.get("zip"),
        )
        
        # Try to find existing account
        search_criteria = f"((Account_Name:equals:{account_payload['Account_Name']}))"
        search_result = client.search_accounts(search_criteria)
        
        if search_result.get("data") and len(search_result["data"]) > 0:
            # Update existing
            account_id = search_result["data"][0]["id"]
            client.update_account(account_id, account_payload)
            record_sync(entity_id, account_id, "Account", "success", settings.duckdb_path)
            logger.info(f"Updated account {account_id} for entity {entity_id}")
            return account_id
        else:
            # Create new
            result = client.create_account(account_payload)
            if result.get("data") and len(result["data"]) > 0:
                account_id = result["data"][0]["details"]["id"]
                record_sync(entity_id, account_id, "Account", "success", settings.duckdb_path)
                logger.info(f"Created account {account_id} for entity {entity_id}")
                return account_id
        
        return None
    
    except Exception as e:
        logger.error(f"Error syncing entity {entity_id} to Bigin: {e}")
        record_sync(entity_id, "", "Account", "error", settings.duckdb_path)
        return None

