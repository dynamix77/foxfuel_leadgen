"""Lead scoring module."""
import logging
import pandas as pd
import duckdb
from typing import Dict, List
from src.config import settings
from src.score.rules import SCORING_RULES, MAX_SCORE, TIER_A_MIN, TIER_B_MIN, TIER_C_MIN
from src.score.reasons import compose_reasons

logger = logging.getLogger(__name__)


def calculate_score(entity: Dict) -> tuple:
    """
    Calculate lead score for an entity.
    
    Args:
        entity: Entity data dictionary
    
    Returns:
        Tuple of (score, tier, reason_codes, reason_text)
    """
    score = 0
    reason_codes = []
    
    # Diesel/fuel oil presence
    if entity.get("is_diesel_like"):
        score += SCORING_RULES["D_TANK"]
        reason_codes.append("D_TANK")
    
    # Capacity bucket
    capacity_bucket = entity.get("capacity_bucket", "")
    if capacity_bucket == "20K+":
        score += SCORING_RULES["CAP_20K"]
        reason_codes.append("CAP_20K")
    elif capacity_bucket == "10K-20K":
        score += SCORING_RULES["CAP_10K"]
        reason_codes.append("CAP_10K")
    elif capacity_bucket == "5K-10K":
        score += SCORING_RULES["CAP_5K"]
        reason_codes.append("CAP_5K")
    elif capacity_bucket == "1K-5K":
        score += SCORING_RULES["CAP_1K"]
        reason_codes.append("CAP_1K")
    
    # Active status
    if entity.get("is_active_like"):
        score += SCORING_RULES["ACTIVE"]
        reason_codes.append("ACTIVE")
    
    # FMCSA fleet size (if available)
    fleet_size = entity.get("fleet_size") or entity.get("power_units")
    if fleet_size:
        if fleet_size >= 50:
            score += SCORING_RULES["FMCSA_50"]
            reason_codes.append("FMCSA_50")
        elif fleet_size >= 10:
            score += SCORING_RULES["FMCSA_10"]
            reason_codes.append("FMCSA_10")
    
    # Critical infrastructure flags (if available)
    if entity.get("is_hospital"):
        score += SCORING_RULES["HOSP"]
        reason_codes.append("HOSP")
    if entity.get("is_school"):
        score += SCORING_RULES["SCHOOL"]
        reason_codes.append("SCHOOL")
    if entity.get("is_data_center"):
        score += SCORING_RULES["DCENTER"]
        reason_codes.append("DCENTER")
    if entity.get("is_echo"):
        score += SCORING_RULES["ECHO"]
        reason_codes.append("ECHO")
    
    # Distance from base (if available)
    distance = entity.get("distance_miles")
    if distance is not None:
        if distance <= 25:
            score += SCORING_RULES["NEAR"]
            reason_codes.append("NEAR")
        elif distance <= 40:
            score += SCORING_RULES["NEAR40"]
            reason_codes.append("NEAR40")
    
    # Website intent (if available)
    if entity.get("web_intent"):
        score += SCORING_RULES["WEB_INTENT"]
        reason_codes.append("WEB_INTENT")
    
    # Sector bonuses
    sector_primary = entity.get("sector_primary")
    if sector_primary and sector_primary != "Unknown":
        if sector_primary == "Fleet and Transportation":
            score += SCORING_RULES["SECTOR_FLEET"]
            reason_codes.append("SECTOR_FLEET")
        elif sector_primary == "Construction":
            score += SCORING_RULES["SECTOR_CONSTR"]
            reason_codes.append("SECTOR_CONSTR")
        elif sector_primary == "Healthcare":
            score += SCORING_RULES["SECTOR_HEALTH"]
            reason_codes.append("SECTOR_HEALTH")
        elif sector_primary == "Education":
            score += SCORING_RULES["SECTOR_EDU"]
            reason_codes.append("SECTOR_EDU")
        elif sector_primary == "Utilities and Data Centers":
            score += SCORING_RULES["SECTOR_UTIL_DC"]
            reason_codes.append("SECTOR_UTIL_DC")
        elif sector_primary == "Industrial and Manufacturing":
            score += SCORING_RULES["SECTOR_MFG"]
            reason_codes.append("SECTOR_MFG")
        elif sector_primary == "Public and Government":
            score += SCORING_RULES["SECTOR_PUBLIC"]
            reason_codes.append("SECTOR_PUBLIC")
        elif sector_primary == "Retail and Commercial Fueling":
            score += SCORING_RULES["SECTOR_RETAIL"]
            reason_codes.append("SECTOR_RETAIL")
    
    # EIA generator signal
    if entity.get("eia_gen") or entity.get("generator_flag"):
        score += SCORING_RULES["EIA_GEN"]
        reason_codes.append("EIA_GEN")
    
    # ECHO facility signal
    if entity.get("echo_flag"):
        score += SCORING_RULES["ECHO"]
        reason_codes.append("ECHO")
    
    # OSM depot signal
    if entity.get("osm_depot") or entity.get("depot_flag") or entity.get("yard_flag") or entity.get("terminal_flag"):
        score += SCORING_RULES["OSM_DEPOT"]
        reason_codes.append("OSM_DEPOT")
    
    # Procurement bid signal
    if entity.get("bid_open"):
        score += SCORING_RULES["BID_OPEN"]
        reason_codes.append("BID_OPEN")
    
    # Recent permit signal
    if entity.get("permit_recent"):
        score += SCORING_RULES["PERMIT_RECENT"]
        reason_codes.append("PERMIT_RECENT")
    
    # Multi-site operator signal
    if entity.get("multi_site"):
        score += SCORING_RULES["MULTI_SITE"]
        reason_codes.append("MULTI_SITE")
    
    # Negative signals
    if entity.get("has_incumbent"):
        score += SCORING_RULES["INCUMBENT"]
        reason_codes.append("INCUMBENT")
    if entity.get("is_dnc"):
        score += SCORING_RULES["DNC"]
        reason_codes.append("DNC")
    
    # Cap score
    score = min(score, MAX_SCORE)
    
    # Determine tier
    if score >= TIER_A_MIN:
        tier = "Tier A"
    elif score >= TIER_B_MIN:
        tier = "Tier B"
    elif score >= TIER_C_MIN:
        tier = "Tier C"
    else:
        tier = "Park"
    
    # Compose human-readable reasons
    reason_text = compose_reasons(reason_codes, entity)
    
    return score, tier, reason_codes, reason_text


def score_entities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score all entities in DataFrame.
    
    Args:
        df: DataFrame with entity data
    
    Returns:
        DataFrame with score, tier, reason_codes, reason_text columns
    """
    logger.info(f"Scoring {len(df)} entities...")
    
    results = []
    for _, row in df.iterrows():
        entity = row.to_dict()
        score, tier, reason_codes, reason_text = calculate_score(entity)
        
        results.append({
            "entity_id": entity.get("facility_id"),
            "score": score,
            "tier": tier,
            "reason_codes": ",".join(reason_codes),
            "reason_text": reason_text
        })
    
    result_df = pd.DataFrame(results)
    
    # Persist to DuckDB
    conn = duckdb.connect(settings.duckdb_path)
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
    
    conn.register('result_df', result_df)
    conn.execute("""
        INSERT OR REPLACE INTO lead_score 
        SELECT *, CURRENT_TIMESTAMP FROM result_df
    """)
    conn.close()
    
    logger.info(f"Scoring complete. Persisted to DuckDB.")
    return result_df

