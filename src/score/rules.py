"""Scoring rules and constants."""
from typing import Dict

# Scoring points by signal type
SCORING_RULES: Dict[str, int] = {
    # Diesel/fuel oil presence
    "D_TANK": 40,
    
    # Capacity buckets
    "CAP_20K": 25,
    "CAP_10K": 20,
    "CAP_5K": 15,
    "CAP_1K": 8,
    
    # Active status
    "ACTIVE": 15,
    
    # FMCSA fleet size
    "FMCSA_50": 20,  # >= 50 power units
    "FMCSA_10": 10,  # 10-49 power units
    
    # Critical infrastructure
    "HOSP": 15,      # Hospital/healthcare
    "SCHOOL": 15,    # School/university/bus depot
    "DCENTER": 15,   # Data center
    
    # ECHO presence
    "ECHO": 10,
    
    # Distance from base
    "NEAR": 10,      # <= 25 miles
    "NEAR40": 5,     # 25-40 miles
    
    # Website intent
    "WEB_INTENT": 10,
    
    # Negative signals
    "INCUMBENT": -10,  # Incumbent named on site
    "DNC": -15,        # CRM do not contact
    
    # Sector bonuses
    "SECTOR_FLEET": 20,      # Fleet and Transportation
    "SECTOR_CONSTR": 15,     # Construction
    "SECTOR_HEALTH": 15,     # Healthcare
    "SECTOR_EDU": 10,        # Education
    "SECTOR_UTIL_DC": 15,    # Utilities and Data Centers
    "SECTOR_MFG": 10,        # Industrial and Manufacturing
    "SECTOR_PUBLIC": 5,      # Public and Government
    "SECTOR_RETAIL": 5,      # Retail and Commercial Fueling
    
    # Generator and facility signals
    "EIA_GEN": 15,           # EIA diesel generator present
    "ECHO": 10,               # ECHO facility present for target NAICS
    
    # Depot and yard signals
    "OSM_DEPOT": 10,         # Bus depot or logistics yard present
    
    # Procurement and permits
    "BID_OPEN": 10,           # Relevant bid open
    "PERMIT_RECENT": 10,     # Tank or generator permit issued in last 12 months
    
    # Multi-site operator
    "MULTI_SITE": 5,          # Brand appears at 2+ entities within 25 miles
}

# Score bands: A=80–100, B=60–79, C=40–59, Park<40
TIER_A_MIN = 80
TIER_B_MIN = 60
TIER_C_MIN = 40

# Maximum score cap: Cap total at 100
MAX_SCORE = 100

