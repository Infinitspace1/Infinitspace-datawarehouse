"""
shared/nexudus/auth.py

Gets a valid Nexudus bearer token.
In an Azure Function context, credentials come from environment variables
(stored in Azure Key Vault / App Settings), not Firebase.

Priority:
  1. NEXUDUS_BEARER_TOKEN  — static token (dev/test only)
  2. NEXUDUS_USERNAME + NEXUDUS_PASSWORD — password grant
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

import requests

logger = logging.getLogger(__name__)

TOKEN_URL = "https://spaces.nexudus.com/api/token"

# Module-level token cache (valid for the lifetime of one function instance)
_cached_token: Optional[str] = None
_token_expires_at: Optional[datetime] = None


def get_bearer_token() -> str:
    """
    Returns a valid Nexudus bearer token.
    Caches within the function instance lifetime.
    """
    global _cached_token, _token_expires_at

    # 1. Static token (dev/test)
    static = os.getenv("NEXUDUS_BEARER_TOKEN")
    if static:
        return static

    # 2. Use cached token if still valid (with 60s buffer)
    now = datetime.now(timezone.utc)
    if _cached_token and _token_expires_at and now < _token_expires_at - timedelta(seconds=60):
        logger.debug("Using cached Nexudus token")
        return _cached_token

    # 3. Fetch new token
    username = os.getenv("NEXUDUS_USERNAME")
    password = os.getenv("NEXUDUS_PASSWORD")

    if not username or not password:
        raise EnvironmentError(
            "Set NEXUDUS_BEARER_TOKEN or NEXUDUS_USERNAME + NEXUDUS_PASSWORD"
        )

    logger.info("Fetching new Nexudus bearer token")
    resp = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "password", "username": username, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    _cached_token = data["access_token"]
    expires_in = data.get("expires_in", 20159)
    _token_expires_at = now + timedelta(seconds=expires_in)

    logger.info(f"Token obtained, expires in {expires_in}s")
    return _cached_token