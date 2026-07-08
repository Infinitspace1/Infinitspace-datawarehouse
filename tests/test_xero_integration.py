import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import requests

from shared.xero.client import XeroApiClient
from shared.xero.flow import XeroAuthFlowService
from shared.xero.oauth import TokenSet, XeroOAuthService
from shared.xero.store import StoredXeroConnection


class FakeOAuth:
    def __init__(self):
        self.exchanged_code = None
        self.refreshed_token = None

    def exchange_code_for_tokens(self, code: str) -> TokenSet:
        self.exchanged_code = code
        return TokenSet(
            access_token="access-1",
            refresh_token="refresh-1",
            id_token="id-1",
            token_type="Bearer",
            scope="offline_access accounting.contacts",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=30),
            raw_payload={"access_token": "access-1", "refresh_token": "refresh-1"},
            xero_user_id="xero-user-1",
        )

    def fetch_connections(self, access_token: str):
        return [
            {
                "tenantId": "tenant-1",
                "tenantName": "My Org",
                "tenantType": "ORGANISATION",
                "id": "conn-1",
            }
        ]

    def should_refresh(self, expires_at: datetime) -> bool:
        return expires_at <= datetime.now(timezone.utc) + timedelta(minutes=5)

    def refresh_tokens(self, refresh_token: str) -> TokenSet:
        self.refreshed_token = refresh_token
        return TokenSet(
            access_token="access-2",
            refresh_token="refresh-2",
            id_token="id-2",
            token_type="Bearer",
            scope="offline_access accounting.contacts",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=30),
            raw_payload={"access_token": "access-2", "refresh_token": "refresh-2"},
            xero_user_id="xero-user-1",
        )


class FakeStore:
    def __init__(self):
        self.state_context = {"owner_type": "workspace", "owner_id": "default"}
        self.saved_state = None
        self.saved_tenants = None
        self.updated_tokens = None
        self.marked_disconnected = None
        self.selected_tenant = None

        self._connection = StoredXeroConnection(
            id=1,
            owner_type="workspace",
            owner_id="default",
            xero_user_id="xero-user-1",
            access_token="access-old",
            refresh_token="refresh-old",
            id_token=None,
            scope="offline_access accounting.contacts",
            token_type="Bearer",
            expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            selected_xero_tenant_id="tenant-1",
            is_connected=True,
            last_error=None,
        )

    def create_state(self, state, owner_type, owner_id, ttl_seconds=600):
        self.saved_state = (state, owner_type, owner_id, ttl_seconds)

    def consume_state(self, state):
        return self.state_context

    def upsert_connection(self, owner_type, owner_id, token_set, selected_tenant_id=None):
        self._connection = StoredXeroConnection(
            id=1,
            owner_type=owner_type,
            owner_id=owner_id,
            xero_user_id=token_set.xero_user_id,
            access_token=token_set.access_token,
            refresh_token=token_set.refresh_token,
            id_token=token_set.id_token,
            scope=token_set.scope,
            token_type=token_set.token_type,
            expires_at=token_set.expires_at,
            selected_xero_tenant_id=selected_tenant_id,
            is_connected=True,
            last_error=None,
        )
        return 1

    def save_tenants(self, connection_id, tenants):
        self.saved_tenants = (connection_id, tenants)

    def get_connection(self, owner_type=None, owner_id=None, connection_id=None):
        return self._connection

    def update_connection_tokens(self, connection_id, token_set):
        self.updated_tokens = token_set
        self._connection = StoredXeroConnection(
            id=connection_id,
            owner_type=self._connection.owner_type,
            owner_id=self._connection.owner_id,
            xero_user_id=token_set.xero_user_id,
            access_token=token_set.access_token,
            refresh_token=token_set.refresh_token,
            id_token=token_set.id_token,
            scope=token_set.scope,
            token_type=token_set.token_type,
            expires_at=token_set.expires_at,
            selected_xero_tenant_id=self._connection.selected_xero_tenant_id,
            is_connected=True,
            last_error=None,
        )

    def mark_disconnected(self, connection_id, error):
        self.marked_disconnected = (connection_id, error)

    def list_tenants(self, owner_type=None, owner_id=None, connection_id=None):
        return [{"xero_tenant_id": "tenant-1"}]

    def set_selected_tenant(self, connection_id, tenant_id):
        self.selected_tenant = (connection_id, tenant_id)


@dataclass
class FakeResponse:
    status_code: int = 200
    payload: dict = None
    headers: dict = None

    @property
    def content(self):
        if self.payload is None:
            return b"%PDF-1.4"
        return b"{}"

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload if self.payload is not None else {"Contacts": []}


@dataclass
class FakeErrorResponse:
    status_code: int
    text: str


class InvalidGrantOAuth(FakeOAuth):
    def refresh_tokens(self, refresh_token: str) -> TokenSet:
        response = FakeErrorResponse(status_code=400, text='{"error":"invalid_grant"}')
        raise requests.HTTPError("invalid_grant", response=response)


class TestXeroIntegration(unittest.TestCase):
    def test_authorization_url_contains_scope_and_redirect(self):
        oauth = XeroOAuthService(
            client_id="cid",
            client_secret="secret",
            redirect_uri="https://example.com/callback",
            scopes="offline_access accounting.contacts",
        )
        url = oauth.build_authorization_url("abc123")
        self.assertIn("response_type=code", url)
        self.assertIn("client_id=cid", url)
        self.assertIn("redirect_uri=https%3A%2F%2Fexample.com%2Fcallback", url)
        self.assertIn("scope=offline_access+accounting.contacts", url)
        self.assertIn("state=abc123", url)

    def test_callback_rejects_state_mismatch(self):
        store = FakeStore()
        store.state_context = None
        flow = XeroAuthFlowService(oauth_service=FakeOAuth(), store=store)
        with self.assertRaises(ValueError):
            flow.handle_callback(code="code-1", state="bad-state")

    def test_callback_exchanges_code_and_persists_tokens_and_tenants(self):
        oauth = FakeOAuth()
        store = FakeStore()
        flow = XeroAuthFlowService(oauth_service=oauth, store=store)
        result = flow.handle_callback(code="code-abc", state="state-xyz")
        self.assertEqual(oauth.exchanged_code, "code-abc")
        self.assertEqual(result["xero_connection_id"], 1)
        self.assertEqual(result["tenant_count"], 1)
        self.assertIsNotNone(store.saved_tenants)

    def test_refresh_replaces_access_and_refresh_token(self):
        oauth = FakeOAuth()
        store = FakeStore()
        client = XeroApiClient(store=store, oauth_service=oauth)

        with patch("shared.xero.client.requests.request") as mock_request:
            mock_request.return_value = FakeResponse(payload={"Contacts": []})
            client.get_contacts(summary_only=True)

        self.assertIsNotNone(store.updated_tokens)
        self.assertEqual(store.updated_tokens.access_token, "access-2")
        self.assertEqual(store.updated_tokens.refresh_token, "refresh-2")

    def test_refresh_invalid_grant_marks_connection_disconnected(self):
        oauth = InvalidGrantOAuth()
        store = FakeStore()
        client = XeroApiClient(store=store, oauth_service=oauth)

        with patch("shared.xero.client.requests.request"):
            with self.assertRaises(requests.HTTPError):
                client.get_contacts(summary_only=True)

        self.assertEqual(store.marked_disconnected, (1, "invalid_grant during token refresh"))
        self.assertIsNone(store.updated_tokens)

    def test_api_client_includes_xero_tenant_header(self):
        oauth = FakeOAuth()
        store = FakeStore()
        # Prevent refresh path so we can focus on headers.
        store._connection = StoredXeroConnection(
            id=1,
            owner_type="workspace",
            owner_id="default",
            xero_user_id="xero-user-1",
            access_token="access-ok",
            refresh_token="refresh-ok",
            id_token=None,
            scope="offline_access accounting.contacts",
            token_type="Bearer",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
            selected_xero_tenant_id="tenant-xyz",
            is_connected=True,
            last_error=None,
        )
        client = XeroApiClient(store=store, oauth_service=oauth)

        with patch("shared.xero.client.requests.request") as mock_request:
            mock_request.return_value = FakeResponse(payload={"Contacts": []})
            client.get_contacts(summary_only=True)

        headers = mock_request.call_args.kwargs["headers"]
        self.assertEqual(headers["xero-tenant-id"], "tenant-xyz")
        self.assertEqual(headers["Authorization"], "Bearer access-ok")

    def test_get_invoices_uses_invoice_endpoint_and_page(self):
        oauth = FakeOAuth()
        store = FakeStore()
        store._connection = StoredXeroConnection(
            id=1,
            owner_type="workspace",
            owner_id="default",
            xero_user_id="xero-user-1",
            access_token="access-ok",
            refresh_token="refresh-ok",
            id_token=None,
            scope="offline_access accounting.contacts accounting.invoices",
            token_type="Bearer",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
            selected_xero_tenant_id="tenant-xyz",
            is_connected=True,
            last_error=None,
        )
        client = XeroApiClient(store=store, oauth_service=oauth)

        with patch("shared.xero.client.requests.request") as mock_request:
            mock_request.return_value = FakeResponse(payload={"Invoices": []})
            client.get_invoices(page=3, tenant_id="tenant-alt")

        self.assertIn("/api.xro/2.0/Invoices", mock_request.call_args.kwargs["url"])
        self.assertEqual(mock_request.call_args.kwargs["params"]["page"], 3)
        self.assertEqual(mock_request.call_args.kwargs["headers"]["xero-tenant-id"], "tenant-alt")

    def test_get_accounts_uses_accounts_endpoint(self):
        oauth = FakeOAuth()
        store = FakeStore()
        store._connection = StoredXeroConnection(
            id=1,
            owner_type="workspace",
            owner_id="default",
            xero_user_id="xero-user-1",
            access_token="access-ok",
            refresh_token="refresh-ok",
            id_token=None,
            scope="offline_access accounting.contacts accounting.invoices accounting.settings.read",
            token_type="Bearer",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
            selected_xero_tenant_id="tenant-xyz",
            is_connected=True,
            last_error=None,
        )
        client = XeroApiClient(store=store, oauth_service=oauth)

        with patch("shared.xero.client.requests.request") as mock_request:
            mock_request.return_value = FakeResponse(payload={"Accounts": []})
            client.get_accounts(tenant_id="tenant-alt")

        self.assertIn("/api.xro/2.0/Accounts", mock_request.call_args.kwargs["url"])
        self.assertEqual(mock_request.call_args.kwargs["headers"]["xero-tenant-id"], "tenant-alt")

    def test_get_organisation_uses_organisation_endpoint(self):
        oauth = FakeOAuth()
        store = FakeStore()
        store._connection = StoredXeroConnection(
            id=1,
            owner_type="workspace",
            owner_id="default",
            xero_user_id="xero-user-1",
            access_token="access-ok",
            refresh_token="refresh-ok",
            id_token=None,
            scope="offline_access accounting.settings.read",
            token_type="Bearer",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
            selected_xero_tenant_id="tenant-xyz",
            is_connected=True,
            last_error=None,
        )
        client = XeroApiClient(store=store, oauth_service=oauth)

        with patch("shared.xero.client.requests.request") as mock_request:
            mock_request.return_value = FakeResponse(payload={"Organisations": []})
            client.get_organisation(tenant_id="tenant-alt")

        self.assertIn("/api.xro/2.0/Organisation", mock_request.call_args.kwargs["url"])
        self.assertEqual(mock_request.call_args.kwargs["headers"]["xero-tenant-id"], "tenant-alt")

    def test_get_profit_and_loss_uses_report_endpoint_and_standard_layout(self):
        oauth = FakeOAuth()
        store = FakeStore()
        store._connection = StoredXeroConnection(
            id=1,
            owner_type="workspace",
            owner_id="default",
            xero_user_id="xero-user-1",
            access_token="access-ok",
            refresh_token="refresh-ok",
            id_token=None,
            scope="offline_access accounting.reports.read",
            token_type="Bearer",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
            selected_xero_tenant_id="tenant-xyz",
            is_connected=True,
            last_error=None,
        )
        client = XeroApiClient(store=store, oauth_service=oauth)

        with patch("shared.xero.client.requests.request") as mock_request:
            mock_request.return_value = FakeResponse(payload={"Reports": []})
            client.get_profit_and_loss(
                from_date="2026-06-01",
                to_date="2026-06-30",
                tenant_id="tenant-alt",
            )

        kwargs = mock_request.call_args.kwargs
        self.assertIn("/api.xro/2.0/Reports/ProfitAndLoss", kwargs["url"])
        self.assertEqual(kwargs["headers"]["xero-tenant-id"], "tenant-alt")
        self.assertEqual(kwargs["params"]["fromDate"], "2026-06-01")
        self.assertEqual(kwargs["params"]["toDate"], "2026-06-30")
        self.assertEqual(kwargs["params"]["standardLayout"], "true")

    def test_get_invoice_pdf_requests_pdf_accept_header(self):
        oauth = FakeOAuth()
        store = FakeStore()
        store._connection = StoredXeroConnection(
            id=1,
            owner_type="workspace",
            owner_id="default",
            xero_user_id="xero-user-1",
            access_token="access-ok",
            refresh_token="refresh-ok",
            id_token=None,
            scope="offline_access accounting.contacts accounting.invoices",
            token_type="Bearer",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
            selected_xero_tenant_id="tenant-xyz",
            is_connected=True,
            last_error=None,
        )
        client = XeroApiClient(store=store, oauth_service=oauth)

        with patch("shared.xero.client.requests.request") as mock_request:
            mock_request.return_value = FakeResponse(
                payload=None,
                headers={
                    "Content-Type": "application/pdf",
                    "Content-Disposition": 'inline; filename="invoice-1.pdf"',
                },
            )
            pdf_bytes, content_type, file_name = client.get_invoice_pdf("invoice-1", tenant_id="tenant-xyz")

        self.assertEqual(content_type, "application/pdf")
        self.assertEqual(file_name, "invoice-1.pdf")
        self.assertEqual(pdf_bytes, b"%PDF-1.4")
        self.assertEqual(mock_request.call_args.kwargs["headers"]["Accept"], "application/pdf")


if __name__ == "__main__":
    unittest.main()
