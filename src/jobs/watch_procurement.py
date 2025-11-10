"""Procurement watcher job."""
import logging
import pandas as pd
import duckdb
from datetime import datetime
from pathlib import Path
from src.config import settings
from src.ingest.procurement import ingest_procurement

logger = logging.getLogger(__name__)


def watch_procurement():
    """Poll procurement sources and generate task suggestions."""
    logger.info("Starting procurement watch...")
    
    # Ingest new procurement data
    procurement_df = ingest_procurement()
    
    if procurement_df.empty:
        logger.info("No new procurement opportunities")
        return
    
    # Load entities for matching
    conn = duckdb.connect(settings.duckdb_path)
    try:
        entities_df = conn.execute("""
            SELECT facility_id, facility_name, address, city, latitude, longitude
            FROM raw_pa_tanks
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """).df()
    except Exception:
        logger.warning("No entities found for matching")
        conn.close()
        return
    
    conn.close()
    
    # Match bids to entities (within 20 miles, name/address similarity)
    # TODO: Implement matching logic
    
    # Write opportunities
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    bids_path = settings.out_dir / f"opportunities_bids_{timestamp}.csv"
    procurement_df.to_csv(bids_path, index=False, encoding='utf-8')
    logger.info(f"Wrote {len(procurement_df)} bids to {bids_path}")
    
    # Generate task suggestions
    tasks_data = []
    for _, bid in procurement_df.iterrows():
        tasks_data.append({
            "bid_id": bid.get("bid_id"),
            "title": bid.get("title"),
            "due_date": bid.get("due_date"),
            "url": bid.get("url"),
            "suggested_task_due": bid.get("due_date"),  # Use bid due date
            "task_subject": f"Bid Opportunity: {bid.get('title', '')[:50]}"
        })
    
    if tasks_data:
        tasks_df = pd.DataFrame(tasks_data)
        tasks_path = settings.out_dir / f"opportunities_bids_tasks_{timestamp}.csv"
        tasks_df.to_csv(tasks_path, index=False, encoding='utf-8')
        logger.info(f"Wrote {len(tasks_df)} task suggestions to {tasks_path}")


if __name__ == "__main__":
    watch_procurement()

