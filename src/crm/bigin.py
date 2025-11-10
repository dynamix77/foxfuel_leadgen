"""Bigin REST API client."""
import logging
import requests
import time
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Dict, Optional, Any
from src.config import settings

logger = logging.getLogger(__name__)

# OAuth token endpoint
OAUTH_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"


class BiginClient:
    """Bigin REST API client with OAuth support."""
    
    def __init__(
        self,
        access_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
        base_url: Optional[str] = None
    ):
        """
        Initialize Bigin client.
        
        Supports two authentication methods:
        1. OAuth (preferred): Uses client_id, client_secret, refresh_token to get access tokens
        2. Direct access token: Uses access_token directly
        
        Args:
            access_token: Direct access token (uses settings if not provided)
            client_id: OAuth client ID (uses settings if not provided)
            client_secret: OAuth client secret (uses settings if not provided)
            refresh_token: OAuth refresh token (uses settings if not provided)
            base_url: Bigin API base URL (uses settings if not provided)
        """
        self.base_url = base_url or settings.bigin_base_url
        self.client_id = client_id or settings.bigin_client_id
        self.client_secret = client_secret or settings.bigin_client_secret
        self.refresh_token = refresh_token or settings.bigin_refresh_token
        self.access_token = access_token or settings.bigin_access_token
        
        # Token cache
        self._cached_token: Optional[str] = None
        self._token_expires_at: float = 0
        
        # Determine authentication method
        if self.client_id and self.client_secret and self.refresh_token:
            # Use OAuth flow
            logger.info("Using OAuth authentication for Bigin")
            self._use_oauth = True
        elif self.access_token:
            # Use direct access token
            logger.info("Using direct access token for Bigin")
            self._use_oauth = False
        else:
            raise ValueError(
                "Bigin authentication not configured. "
                "Provide either (BIGIN_CLIENT_ID, BIGIN_CLIENT_SECRET, BIGIN_REFRESH_TOKEN) "
                "or BIGIN_ACCESS_TOKEN"
            )
    
    def _get_access_token(self) -> str:
        """
        Get valid access token, refreshing if needed (OAuth) or returning cached token.
        
        Returns:
            Valid access token
        """
        if not self._use_oauth:
            return self.access_token
        
        # Check if cached token is still valid (refresh 5 minutes before expiry)
        if self._cached_token and time.time() < (self._token_expires_at - 300):
            return self._cached_token
        
        # Refresh token
        logger.info("Refreshing Bigin OAuth access token")
        try:
            response = requests.post(
                OAUTH_TOKEN_URL,
                params={
                    "refresh_token": self.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "refresh_token"
                }
            )
            response.raise_for_status()
            token_data = response.json()
            
            self._cached_token = token_data.get("access_token")
            # Default to 1 hour expiry if not provided
            expires_in = token_data.get("expires_in", 3600)
            self._token_expires_at = time.time() + expires_in
            
            if not self._cached_token:
                raise ValueError("No access_token in OAuth response")
            
            logger.info("Successfully refreshed Bigin access token")
            return self._cached_token
            
        except requests.RequestException as e:
            logger.error(f"Failed to refresh Bigin OAuth token: {e}")
            raise ValueError(f"OAuth token refresh failed: {e}")
    
    @property
    def headers(self) -> Dict[str, str]:
        """Get request headers with current access token."""
        token = self._get_access_token()
        return {
            "Authorization": f"Zoho-oauthtoken {token}",
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
        url = f"{self.base_url}/{endpoint}"
        
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

