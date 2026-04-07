"""
shared/xero/flow.py

High-level Xero auth flow orchestration used by HTTP endpoints and scripts.
"""
from __future__ import annotations

import secrets
from typing import Any, Optional

from shared.xero.oauth import XeroOAuthService
from shared.xero.store import XeroStore
from shared.xero.tenant_directory import XeroTenantDirectoryService

DEFAULT_OWNER_TYPE = "workspace"
DEFAULT_OWNER_ID = "default"


class XeroAuthFlowService:
    def __init__(
        self,
        oauth_service: Optional[XeroOAuthService] = None,
        store: Optional[XeroStore] = None,
    ):
        self.oauth = oauth_service or XeroOAuthService()
        self.store = store or XeroStore()

    def start_auth(
        self,
        owner_type: str = DEFAULT_OWNER_TYPE,
        owner_id: str = DEFAULT_OWNER_ID,
    ) -> dict[str, str]:
        state = secrets.token_urlsafe(32)
        self.store.create_state(state=state, owner_type=owner_type, owner_id=owner_id)
        return {
            "state": state,
            "authorization_url": self.oauth.build_authorization_url(state),
        }

    def handle_callback(self, code: str, state: str) -> dict[str, Any]:
        state_context = self.store.consume_state(state)
        if not state_context:
            raise ValueError("Invalid or expired OAuth state")

        owner_type = state_context.get("owner_type") or DEFAULT_OWNER_TYPE
        owner_id = state_context.get("owner_id") or DEFAULT_OWNER_ID

        token_set = self.oauth.exchange_code_for_tokens(code)
        tenants = self.oauth.fetch_connections(token_set.access_token)
        selected_tenant_id = None
        if tenants:
            selected_tenant_id = tenants[0].get("tenantId")

        connection_id = self.store.upsert_connection(
            owner_type=owner_type,
            owner_id=owner_id,
            token_set=token_set,
            selected_tenant_id=selected_tenant_id,
        )
        self.store.save_tenants(connection_id, tenants)

        tenant_directory = None
        tenant_directory_error = None
        try:
            tenant_directory = XeroTenantDirectoryService(store=self.store).refresh_tenants(
                connection_id=connection_id,
            )
        except Exception as exc:
            tenant_directory_error = str(exc)

        return {
            "owner_type": owner_type,
            "owner_id": owner_id,
            "xero_connection_id": connection_id,
            "tenant_count": len(tenants),
            "selected_tenant_id": selected_tenant_id,
            "tenant_directory": tenant_directory,
            "tenant_directory_error": tenant_directory_error,
        }
