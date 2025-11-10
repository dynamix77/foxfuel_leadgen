"""Bigin payload builders."""
from typing import Dict, Optional, List


def build_account_payload(
    account_name: str,
    lead_score: Optional[int] = None,
    reason_codes: Optional[str] = None,
    tank_capacity_bucket: Optional[str] = None,
    fleet_size: Optional[int] = None,
    generator_flag: Optional[bool] = None,
    sector_primary: Optional[str] = None,
    sector_confidence: Optional[int] = None,
    **kwargs
) -> Dict:
    """
    Build account payload for Bigin API.
    
    Args:
        account_name: Account name (required)
        lead_score: Lead score (custom field)
        reason_codes: Reason codes text (custom field)
        tank_capacity_bucket: Capacity bucket (custom field)
        fleet_size: Fleet size (custom field)
        generator_flag: Generator presence flag (custom field)
        **kwargs: Additional account fields
    
    Returns:
        Account payload dictionary
    """
    payload = {
        "Account_Name": account_name,
        **kwargs
    }
    
    # Add custom fields
    if lead_score is not None:
        payload["cf_lead_score"] = lead_score
    if reason_codes:
        payload["cf_reason_codes"] = reason_codes
    if tank_capacity_bucket:
        payload["cf_tank_capacity_bucket"] = tank_capacity_bucket
    if fleet_size is not None:
        payload["cf_fleet_size"] = fleet_size
    if generator_flag is not None:
        payload["cf_generator_flag"] = generator_flag
    
    # Sector fields
    if sector_primary:
        # Map to picklist values
        sector_map = {
            "Fleet and Transportation": "Fleet",
            "Construction": "Construction",
            "Healthcare": "Healthcare",
            "Education": "Education",
            "Utilities and Data Centers": "Utilities_DataCenters",
            "Industrial and Manufacturing": "Industrial_Manufacturing",
            "Public and Government": "Public_Government",
            "Retail and Commercial Fueling": "Retail_Commercial",
            "Unknown": "Unknown"
        }
        payload["cf_sector_primary"] = sector_map.get(sector_primary, "Unknown")
    
    if sector_confidence is not None:
        payload["cf_sector_confidence"] = sector_confidence
    
    return payload


def build_contact_payload(
    first_name: str,
    last_name: str,
    account_id: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    **kwargs
) -> Dict:
    """
    Build contact payload for Bigin API.
    
    Args:
        first_name: First name (required)
        last_name: Last name (required)
        account_id: Associated account ID
        email: Email address
        phone: Phone number
        **kwargs: Additional contact fields
    
    Returns:
        Contact payload dictionary
    """
    payload = {
        "First_Name": first_name,
        "Last_Name": last_name,
        **kwargs
    }
    
    if account_id:
        payload["Account_Name"] = {"id": account_id}
    if email:
        payload["Email"] = email
    if phone:
        payload["Phone"] = phone
    
    return payload


def build_deal_payload(
    deal_name: str,
    account_id: str,
    stage: str,
    amount: Optional[float] = None,
    **kwargs
) -> Dict:
    """
    Build deal payload for Bigin API.
    
    Args:
        deal_name: Deal name (required)
        account_id: Associated account ID (required)
        stage: Deal stage (required)
        amount: Deal amount
        **kwargs: Additional deal fields
    
    Returns:
        Deal payload dictionary
    """
    payload = {
        "Deal_Name": deal_name,
        "Account_Name": {"id": account_id},
        "Stage": stage,
        **kwargs
    }
    
    if amount is not None:
        payload["Amount"] = amount
    
    return payload

