"""Fuzzy header matching utilities."""
from typing import Dict, Optional
from rapidfuzz import fuzz


def find_header_match(
    target: str,
    candidate_headers: list,
    threshold: float = 80.0
) -> Optional[str]:
    """
    Find the best matching header using fuzzy string matching.
    
    Args:
        target: The header name to match
        candidate_headers: List of candidate header names
        threshold: Minimum similarity score (0-100)
    
    Returns:
        Best matching header name or None if below threshold
    """
    if not candidate_headers:
        return None
    
    best_match = None
    best_score = 0.0
    
    for header in candidate_headers:
        score = fuzz.ratio(target.upper(), header.upper())
        if score > best_score:
            best_score = score
            best_match = header
    
    if best_score >= threshold:
        return best_match
    return None


def map_headers(
    expected_headers: Dict[str, str],
    actual_headers: list,
    threshold: float = 80.0
) -> Dict[str, Optional[str]]:
    """
    Map expected header names to actual headers using fuzzy matching.
    
    Args:
        expected_headers: Dict mapping canonical names to expected header names
        actual_headers: List of actual header names from file
        threshold: Minimum similarity score for fuzzy matching
    
    Returns:
        Dict mapping canonical names to actual header names (or None if not found)
    """
    mapping = {}
    used_headers = set()
    
    # First pass: exact matches (case-insensitive)
    for canonical, expected in expected_headers.items():
        for actual in actual_headers:
            if actual.upper() == expected.upper() and actual not in used_headers:
                mapping[canonical] = actual
                used_headers.add(actual)
                break
    
    # Second pass: fuzzy matches for unmapped headers
    for canonical, expected in expected_headers.items():
        if canonical not in mapping:
            match = find_header_match(expected, actual_headers, threshold)
            if match and match not in used_headers:
                mapping[canonical] = match
                used_headers.add(match)
    
    return mapping

