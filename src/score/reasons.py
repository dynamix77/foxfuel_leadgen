"""Human-readable reason generation."""
from typing import List, Dict


def format_reason_code(code: str, value: any = None) -> str:
    """
    Format a reason code into human-readable text.
    
    Args:
        code: Reason code (e.g., "D_TANK", "CAP_10K")
        value: Optional value to include in reason
    
    Returns:
        Human-readable reason string
    """
    reason_map = {
        "D_TANK": "Diesel tanks present",
        "CAP_20K": f"Diesel tanks {value or '20,000+'} gal",
        "CAP_10K": f"Diesel tanks {value or '10,000-20,000'} gal",
        "CAP_5K": f"Diesel tanks {value or '5,000-10,000'} gal",
        "CAP_1K": f"Diesel tanks {value or '1,000-5,000'} gal",
        "ACTIVE": "Active facility",
        "FMCSA_50": f"FMCSA fleet size {value or '50+'} power units",
        "FMCSA_10": f"FMCSA fleet size {value or '10-49'} power units",
        "HOSP": "Hospital or healthcare facility",
        "SCHOOL": "School district, university, or bus depot",
        "DCENTER": "Data center",
        "ECHO": "ECHO facility registry",
        "NEAR": f"{value or 'Within 25'} miles from base",
        "NEAR40": f"{value or '25-40'} miles from base",
        "WEB_INTENT": "Website language indicates intent",
        "INCUMBENT": "Incumbent named on site page",
        "DNC": "CRM do not contact flag",
        "EIA_GEN": "EIA diesel generator present",
        "ECHO": "ECHO facility registry",
        "OSM_DEPOT": "Bus depot or logistics yard present",
        "BID_OPEN": "Relevant bid open",
        "PERMIT_RECENT": "Tank or generator permit issued in last 12 months",
        "MULTI_SITE": "Brand appears at 2+ entities within 25 miles",
    }
    
    return reason_map.get(code, code)


def compose_reasons(reason_codes: List[str], entity_data: Dict) -> str:
    """
    Compose human-readable reasons from reason codes.
    
    Args:
        reason_codes: List of reason codes
        entity_data: Entity data dict for value extraction
    
    Returns:
        Human-readable reason string
    """
    reasons = []
    
    for code in reason_codes:
        value = None
        
        # Extract relevant value from entity data
        if code.startswith("CAP_"):
            value = entity_data.get("capacity_gal")
            if value:
                value = f"{int(value):,} gal"
        elif code.startswith("FMCSA_"):
            value = entity_data.get("power_units") or entity_data.get("fleet_size")
        elif code in ["NEAR", "NEAR40"]:
            value = entity_data.get("distance_miles")
            if value:
                value = f"{value:.1f} miles"
        
        reason_text = format_reason_code(code, value)
        reasons.append(reason_text)
    
    return "; ".join(reasons)

