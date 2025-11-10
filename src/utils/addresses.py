"""Address normalization utilities."""
import re
from typing import Optional
import usaddress


def normalize_address(
    address_line1: Optional[str],
    address_line2: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    zip_code: Optional[str] = None,
    country: str = "USA"
) -> str:
    """
    Build a normalized full address string.
    
    Args:
        address_line1: Primary address line
        address_line2: Secondary address line (optional)
        city: City name
        state: State abbreviation
        zip_code: ZIP code
        country: Country name (default: USA)
    
    Returns:
        Normalized address string
    """
    parts = []
    
    if address_line1:
        parts.append(address_line1.strip())
    if address_line2:
        parts.append(address_line2.strip())
    if city:
        parts.append(city.strip())
    if state:
        parts.append(state.strip())
    if zip_code:
        parts.append(zip_code.strip())
    if country:
        parts.append(country)
    
    return ", ".join(parts)


def create_street_key(address: str) -> str:
    """
    Create a normalized street key for matching.
    
    Args:
        address: Address string
    
    Returns:
        Normalized street key
    """
    if not address:
        return ""
    
    # Normalize: uppercase, remove punctuation, collapse whitespace
    normalized = re.sub(r'[^\w\s]', '', address.upper())
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Remove common suffixes and prefixes for better matching
    suffixes = ['ST', 'STREET', 'AVE', 'AVENUE', 'RD', 'ROAD', 'BLVD', 'BOULEVARD',
                'DR', 'DRIVE', 'LN', 'LANE', 'CT', 'COURT', 'PL', 'PLACE']
    
    words = normalized.split()
    filtered_words = [w for w in words if w not in suffixes]
    
    return ' '.join(filtered_words)


def parse_address(address: str) -> dict:
    """
    Parse address using usaddress library.
    
    Args:
        address: Address string to parse
    
    Returns:
        Dict with parsed components
    """
    try:
        parsed, _ = usaddress.tag(address)
        return parsed
    except Exception:
        return {}

