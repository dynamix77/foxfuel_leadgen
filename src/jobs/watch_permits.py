"""Permits watcher job."""
import logging
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
from src.config import settings
from src.ingest.permits import ingest_permits

logger = logging.getLogger(__name__)


def watch_permits():
    """Poll permits sources and generate task suggestions."""
    logger.info("Starting permits watch...")
    
    # Ingest new permits data
    permits_df = ingest_permits()
    
    if permits_df.empty:
        logger.info("No new permits")
        return
    
    # Filter for recent permits (last 12 months)
    if "issue_date" in permits_df.columns:
        cutoff_date = datetime.now() - timedelta(days=365)
        permits_df["issue_date"] = pd.to_datetime(permits_df["issue_date"], errors='coerce')
        permits_df = permits_df[permits_df["issue_date"] >= cutoff_date]
    
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
    
    # Match permits to entities
    # TODO: Implement matching logic
    
    # Write opportunities
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    permits_path = settings.out_dir / f"opportunities_permits_{timestamp}.csv"
    permits_df.to_csv(permits_path, index=False, encoding='utf-8')
    logger.info(f"Wrote {len(permits_df)} permits to {permits_path}")
    
    # Generate task suggestions (due date = issue_date + 7 days)
    tasks_data = []
    for _, permit in permits_df.iterrows():
        issue_date = permit.get("issue_date")
        if pd.notna(issue_date):
            if isinstance(issue_date, str):
                issue_date = pd.to_datetime(issue_date)
            task_due = issue_date + timedelta(days=7)
        else:
            task_due = None
        
        tasks_data.append({
            "permit_id": permit.get("permit_id"),
            "permit_type": permit.get("permit_type"),
            "class": permit.get("class"),
            "issue_date": permit.get("issue_date"),
            "contractor": permit.get("contractor"),
            "suggested_task_due": task_due,
            "task_subject": f"Permit Opportunity: {permit.get('permit_type', '')} - {permit.get('applicant', '')}"
        })
    
    if tasks_data:
        tasks_df = pd.DataFrame(tasks_data)
        tasks_path = settings.out_dir / f"opportunities_permits_tasks_{timestamp}.csv"
        tasks_df.to_csv(tasks_path, index=False, encoding='utf-8')
        logger.info(f"Wrote {len(tasks_df)} task suggestions to {tasks_path}")


if __name__ == "__main__":
    watch_permits()

