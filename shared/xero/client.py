"""
shared/xero/client.py

Authenticated Xero API client with automatic token refresh.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import requests

from shared.xero.flow import DEFAULT_OWNER_ID, DEFAULT_OWNER_TYPE
from shared.xero.oauth import XeroOAuthService
from shared.xero.store import StoredXeroConnection, XeroStore

XERO_API_BASE = "https://api.xero.com"


class XeroApiClient:
    def __init__(
        self,
        owner_type: str = DEFAULT_OWNER_TYPE,
        owner_id: str = DEFAULT_OWNER_ID,
        connection_id: Optional[int] = None,
        tenant_id: Optional[str] = None,
        store: Optional[XeroStore] = None,
        oauth_service: Optional[XeroOAuthService] = None,
        timeout_seconds: int = 30,
    ):
        self.owner_type = owner_type
        self.owner_id = owner_id
        self.connection_id = connection_id
        self.tenant_id = tenant_id
        self.store = store or XeroStore()
        self.oauth = oauth_service or XeroOAuthService()
        self.timeout_seconds = timeout_seconds

    def get_connections(self) -> list[dict[str, Any]]:
        conn = self._ensure_connection()
        return self.oauth.fetch_connections(conn.access_token)

    def get_contacts(self, summary_only: bool = True) -> dict[str, Any]:
        params = {"summaryOnly": "true"} if summary_only else None
        return self.request(
            method="GET",
            path="/api.xro/2.0/Contacts",
            params=params,
        )

    def get_invoice(
        self,
        invoice_id: str,
        tenant_id: Optional[str] = None,
    ) -> dict[str, Any]:
        return self.request(
            method="GET",
            path=f"/api.xro/2.0/Invoices/{invoice_id}",
            tenant_id=tenant_id,
        )

    def get_invoices(
        self,
        summary_only: bool = False,
        page: int = 1,
        tenant_id: Optional[str] = None,
        if_modified_since: Optional[datetime] = None,
    ) -> dict[str, Any]:
        params = {"page": page}
        if summary_only:
            params["summaryOnly"] = "true"

        extra_headers: dict[str, str] = {}
        if if_modified_since is not None:
            extra_headers["If-Modified-Since"] = if_modified_since.strftime("%a, %d %b %Y %H:%M:%S GMT")

        return self.request(
            method="GET",
            path="/api.xro/2.0/Invoices",
            params=params,
            tenant_id=tenant_id,
            extra_headers=extra_headers or None,
        )

    def get_invoice_pdf(
        self,
        invoice_id: str,
        tenant_id: Optional[str] = None,
    ) -> tuple[bytes, str, Optional[str]]:
        response = self.request_response(
            method="GET",
            path=f"/api.xro/2.0/Invoices/{invoice_id}",
            tenant_id=tenant_id,
            extra_headers={"Accept": "application/pdf"},
        )
        content_type = response.headers.get("Content-Type", "application/pdf")
        content_disposition = response.headers.get("Content-Disposition")
        filename = None
        if content_disposition and "filename=" in content_disposition:
            filename = content_disposition.split("filename=", 1)[1].strip().strip("\"")
        return response.content, content_type, filename

    def request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        extra_headers: Optional[dict[str, str]] = None,
    ) -> Any:
        response = self.request_response(
            method=method,
            path=path,
            params=params,
            json_body=json_body,
            tenant_id=tenant_id,
            extra_headers=extra_headers,
        )
        if response.content:
            return response.json()
        return {}

    def request_response(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        extra_headers: Optional[dict[str, str]] = None,
    ) -> requests.Response:
        conn = self._ensure_connection()
        resolved_tenant_id = tenant_id or self._resolve_tenant_id(conn)
        headers = {
            "Authorization": f"Bearer {conn.access_token}",
            "xero-tenant-id": resolved_tenant_id,
            "Accept": "application/json",
        }
        if extra_headers:
            headers.update(extra_headers)
        url = path if path.startswith("http") else f"{XERO_API_BASE}{path}"
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response

    def _ensure_connection(self) -> StoredXeroConnection:
        if self.connection_id is not None:
            conn = self.store.get_connection(connection_id=self.connection_id)
        else:
            conn = self.store.get_connection(owner_type=self.owner_type, owner_id=self.owner_id)

        if conn is None:
            raise ValueError("No Xero connection found for the requested owner")
        if not conn.is_connected:
            raise ValueError(f"Xero connection is disconnected: {conn.last_error or 'unknown error'}")

        if not self.oauth.should_refresh(conn.expires_at):
            return conn

        try:
            refreshed = self.oauth.refresh_tokens(conn.refresh_token)
        except requests.HTTPError as exc:
            response_text = ""
            status = None
            if exc.response is not None:
                status = exc.response.status_code
                response_text = exc.response.text or ""
            if status == 400 and "invalid_grant" in response_text:
                self.store.mark_disconnected(conn.id, "invalid_grant during token refresh")
            raise

        self.store.update_connection_tokens(conn.id, refreshed)
        # Optional but useful: update tenants with latest /connections response.
        tenants = self.oauth.fetch_connections(refreshed.access_token)
        self.store.save_tenants(conn.id, tenants)
        self.connection_id = conn.id
        refreshed_conn = self.store.get_connection(connection_id=conn.id)
        if refreshed_conn is None:
            raise RuntimeError("Connection disappeared after refresh")
        return refreshed_conn

    def _resolve_tenant_id(self, conn: StoredXeroConnection) -> str:
        if self.tenant_id:
            return self.tenant_id
        if conn.selected_xero_tenant_id:
            return conn.selected_xero_tenant_id

        tenants = self.store.list_tenants(connection_id=conn.id)
        if not tenants:
            raise ValueError("No Xero tenant found for this connection")
        tenant_id = tenants[0]["xero_tenant_id"]
        self.store.set_selected_tenant(conn.id, tenant_id)
        return tenant_id
