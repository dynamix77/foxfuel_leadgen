"""HTTP client utilities with retry logic."""
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def get_with_retry(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30
) -> requests.Response:
    """
    Make HTTP GET request with automatic retry on failure.
    
    Args:
        url: Request URL
        params: Query parameters
        headers: Request headers
        timeout: Request timeout in seconds
    
    Returns:
        Response object
    
    Raises:
        requests.RequestException: If request fails after retries
    """
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.warning(f"Request failed: {e}, retrying...")
        raise

