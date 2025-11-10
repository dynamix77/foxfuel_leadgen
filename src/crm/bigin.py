"""Bigin REST API client."""
import logging
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Dict, Optional, Any
from src.config import settings

logger = logging.getLogger(__name__)

BASE_URL = "https://www.zohoapis.com/bigin/v1"


class BiginClient:
    """Minimal Bigin REST API client."""
    
    def __init__(self, access_token: Optional[str] = None):
        """
        Initialize Bigin client.
        
        Args:
            access_token: Bigin access token (uses settings if not provided)
        """
        self.access_token = access_token or settings.bigin_access_token
        if not self.access_token:
            raise ValueError("BIGIN_ACCESS_TOKEN not set")
        
        self.headers = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json"
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make API request with retry logic.
        
        Args:
            method: HTTP method (GET, POST, PUT, PATCH)
            endpoint: API endpoint (without base URL)
            data: Request body data
        
        Returns:
            Response JSON as dict
        """
        url = f"{BASE_URL}/{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            elif method == "PUT":
                response = requests.put(url, headers=self.headers, json=data)
            elif method == "PATCH":
                response = requests.patch(url, headers=self.headers, json=data)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            response.raise_for_status()
            return response.json()
        
        except requests.RequestException as e:
            logger.error(f"Bigin API request failed: {e}")
            raise
    
    def create_account(self, account_data: Dict) -> Dict:
        """Create an account."""
        return self._request("POST", "Accounts", {"data": [account_data]})
    
    def update_account(self, account_id: str, account_data: Dict) -> Dict:
        """Update an account."""
        return self._request("PUT", f"Accounts/{account_id}", {"data": [account_data]})
    
    def search_accounts(self, criteria: str) -> Dict:
        """Search accounts."""
        return self._request("GET", f"Accounts/search?criteria={criteria}")

