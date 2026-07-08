"""
shared/xero/oauth.py

Low-level Xero OAuth helpers:
  - Build authorization URL
  - Exchange auth code for tokens
  - Refresh tokens (with refresh token rotation)
  - Fetch tenant connections
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from requests.auth import HTTPBasicAuth

XERO_AUTHORIZE_URL = "https://login.xero.com/identity/connect/authorize"
XERO_TOKEN_URL = "https://identity.xero.com/connect/token"
XERO_CONNECTIONS_URL = "https://api.xero.com/connections"


@dataclass(frozen=True)
class TokenSet:
    access_token: str
    refresh_token: str
    id_token: Optional[str]
    token_type: Optional[str]
    scope: Optional[str]
    expires_at: datetime
    raw_payload: dict[str, Any]
    xero_user_id: Optional[str]


class XeroOAuthService:
    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        scopes: Optional[str] = None,
        timeout_seconds: int = 30,
        leeway_seconds: int = 300,
    ):
        self.client_id = client_id or os.getenv("XERO_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("XERO_CLIENT_SECRET")
        self.redirect_uri = redirect_uri or os.getenv("XERO_REDIRECT_URI")
        self.scopes = scopes or os.getenv(
            "XERO_SCOPES",
            "offline_access accounting.contacts.read accounting.transactions.read accounting.settings.read accounting.reports.read",
        )
        self.timeout_seconds = timeout_seconds
        self.leeway_seconds = leeway_seconds
        self._validate_config()

    def _validate_config(self) -> None:
        missing = []
        if not self.client_id:
            missing.append("XERO_CLIENT_ID")
        if not self.client_secret:
            missing.append("XERO_CLIENT_SECRET")
        if not self.redirect_uri:
            missing.append("XERO_REDIRECT_URI")
        if missing:
            raise EnvironmentError(f"Missing Xero config: {', '.join(missing)}")

    def build_authorization_url(self, state: str) -> str:
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": self.scopes,
            "state": state,
        }
        return f"{XERO_AUTHORIZE_URL}?{urlencode(params)}"

    def exchange_code_for_tokens(self, code: str) -> TokenSet:
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }
        return self._post_token(payload)

    def refresh_tokens(self, refresh_token: str) -> TokenSet:
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        return self._post_token(payload)

    def _post_token(self, payload: dict[str, Any]) -> TokenSet:
        response = requests.post(
            XERO_TOKEN_URL,
            data=payload,
            auth=HTTPBasicAuth(self.client_id, self.client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        body = response.json()
        expires_in = int(body.get("expires_in", 1800))
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        return TokenSet(
            access_token=body["access_token"],
            refresh_token=body["refresh_token"],
            id_token=body.get("id_token"),
            token_type=body.get("token_type"),
            scope=body.get("scope"),
            expires_at=expires_at,
            raw_payload=body,
            xero_user_id=body.get("xero_userid"),
        )

    def fetch_connections(self, access_token: str) -> list[dict[str, Any]]:
        response = requests.get(
            XERO_CONNECTIONS_URL,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError("Unexpected Xero /connections payload")
        return data

    def should_refresh(self, expires_at: datetime) -> bool:
        refresh_after = datetime.now(timezone.utc) + timedelta(seconds=self.leeway_seconds)
        return expires_at <= refresh_after
