"""Daily rescoring job."""
import json
import logging
import pandas as pd
import duckdb
from datetime import datetime, timezone
from pathlib import Path
from src.config import settings
from src.score.scorer import score_entities

# Setup structured JSON logging
log_dir = Path("./logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "rescore_daily.log"

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


def main():
    """Main entry point for daily rescoring."""
    start_time = datetime.now()
    logger.info("Starting daily rescore job...")
    
    # Load entities from DuckDB
    conn = duckdb.connect(settings.duckdb_path)
    
    # Get entities from raw_pa_tanks (and other sources when available)
    entities_df = conn.execute("SELECT * FROM raw_pa_tanks").df()
    conn.close()
    
    if entities_df.empty:
        logger.warning("No entities found to score")
        return
    
    # Score entities
    score_start = datetime.now()
    scores_df = score_entities(entities_df)
    score_duration = (datetime.now() - score_start).total_seconds()
    logger.info(f"Scoring completed in {score_duration:.2f} seconds", extra={"duration": score_duration})
    
    # Merge scores back to entities
    result_df = entities_df.merge(scores_df, left_on="facility_id", right_on="entity_id", how="left")
    
    # Write daily CSV for Power BI
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_path = settings.out_dir / f"daily_scores_{timestamp}.csv"
    result_df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Daily scores written to {output_path}")
    
    total_duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Rescore complete: {len(scores_df)} entities scored in {total_duration:.2f} seconds", extra={"duration": total_duration})

if __name__ == "__main__":
    main()

