"""
shared/nexudus/auth.py

Gets a valid Nexudus bearer token.

Priority:
  1. NEXUDUS_BEARER_TOKEN       — static token (dev/test only)
  2. In-memory cache             — still valid within this process
  3. Refresh token grant         — NEXUDUS_REFRESH_TOKEN env var or .nexudus_token_cache.json
  4. Password grant              — NEXUDUS_USERNAME + NEXUDUS_PASSWORD (fallback)
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

import requests

logger = logging.getLogger(__name__)

TOKEN_URL = "https://spaces.nexudus.com/api/token"
_TOKEN_CACHE_FILE = Path(__file__).parent.parent.parent / ".nexudus_token_cache.json"

# Module-level token cache (valid for the lifetime of one function instance)
_cached_token: Optional[str] = None
_token_expires_at: Optional[datetime] = None
_cached_refresh_token: Optional[str] = None
_cache_loaded: bool = False


def _load_token_cache() -> None:
    """Load persisted tokens from env var or file on first call."""
    global _cached_token, _token_expires_at, _cached_refresh_token, _cache_loaded
    _cache_loaded = True

    # Env var takes priority over file (works in Azure Functions)
    env_refresh = os.getenv("NEXUDUS_REFRESH_TOKEN")
    if env_refresh:
        _cached_refresh_token = env_refresh
        return

    if not _TOKEN_CACHE_FILE.exists():
        return

    try:
        data = json.loads(_TOKEN_CACHE_FILE.read_text())
        _cached_refresh_token = data.get("refresh_token")
        access_token = data.get("access_token")
        expires_at_str = data.get("expires_at")
        if access_token and expires_at_str:
            expires_at = datetime.fromisoformat(expires_at_str)
            now = datetime.now(timezone.utc)
            if now < expires_at - timedelta(seconds=60):
                _cached_token = access_token
                _token_expires_at = expires_at
    except Exception as e:
        logger.warning(f"Could not read token cache file: {e}")


def _save_token_cache(access_token: str, refresh_token: str, expires_at: datetime) -> None:
    """Persist tokens to file for reuse across processes."""
    global _cached_refresh_token
    _cached_refresh_token = refresh_token
    try:
        _TOKEN_CACHE_FILE.write_text(json.dumps({
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": expires_at.isoformat(),
        }))
        logger.debug("Nexudus token cache updated")
    except Exception as e:
        logger.debug(f"Could not write token cache: {e}")

    # Always log the refresh token so it can be set as an Azure app setting
    logger.info(
        "Nexudus refresh_token obtained — if running in Azure Functions, set "
        f"NEXUDUS_REFRESH_TOKEN app setting to this value: {refresh_token}"
    )


def _try_refresh_grant(refresh_token: str) -> Optional[dict]:
    """Attempt a refresh_token grant. Returns parsed response or None on failure."""
    try:
        resp = requests.post(
            TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"Refresh token grant failed ({resp.status_code}): {resp.text}")
        return None
    except Exception as e:
        logger.warning(f"Refresh token request error: {e}")
        return None


def get_bearer_token() -> str:
    """
    Returns a valid Nexudus bearer token.
    Caches within the function instance lifetime; persists refresh_token across restarts.
    """
    global _cached_token, _token_expires_at

    # 1. Static token (dev/test)
    static = os.getenv("NEXUDUS_BEARER_TOKEN")
    if static:
        return static

    # Load persisted tokens on first call
    if not _cache_loaded:
        _load_token_cache()

    # 2. Use cached access token if still valid (with 60s buffer)
    now = datetime.now(timezone.utc)
    if _cached_token and _token_expires_at and now < _token_expires_at - timedelta(seconds=60):
        logger.debug("Using cached Nexudus token")
        return _cached_token

    # 3. Try refresh token grant (no password needed)
    if _cached_refresh_token:
        logger.info("Refreshing Nexudus token via refresh_token grant")
        data = _try_refresh_grant(_cached_refresh_token)
        if data and "access_token" in data:
            _cached_token = data["access_token"]
            expires_in = data.get("expires_in", 86400)
            _token_expires_at = now + timedelta(seconds=expires_in)
            new_refresh = data.get("refresh_token", _cached_refresh_token)
            _save_token_cache(_cached_token, new_refresh, _token_expires_at)
            logger.info(f"Token refreshed via refresh_token, expires in {expires_in}s")
            return _cached_token
        logger.warning("Refresh token failed — falling back to password grant")

    # 4. Fall back to password grant
    username = os.getenv("NEXUDUS_USERNAME")
    password = os.getenv("NEXUDUS_PASSWORD")

    if not username or not password:
        raise EnvironmentError(
            "Set NEXUDUS_BEARER_TOKEN, NEXUDUS_REFRESH_TOKEN, "
            "or NEXUDUS_USERNAME + NEXUDUS_PASSWORD"
        )

    logger.info("Fetching Nexudus token via password grant")
    resp = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "password", "username": username, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    _cached_token = data["access_token"]
    expires_in = data.get("expires_in", 86400)
    _token_expires_at = now + timedelta(seconds=expires_in)

    new_refresh = data.get("refresh_token")
    if new_refresh:
        _save_token_cache(_cached_token, new_refresh, _token_expires_at)

    logger.info(f"Token obtained via password grant, expires in {expires_in}s")
    return _cached_token
