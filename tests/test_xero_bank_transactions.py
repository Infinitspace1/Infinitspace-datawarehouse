import unittest
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import requests

from shared.xero.bank_transaction_sync import (
    XeroBankTransactionSyncService,
    line_item_rows,
    silver_header_values,
)
from shared.xero.client import XeroApiClient
from shared.xero.store import StoredXeroConnection

SAMPLE_TXN = {
    "BankTransactionID": "bt-001",
    "Type": "SPEND",
    "Status": "AUTHORISED",
    "IsReconciled": True,
    "HasAttachments": False,
    "Reference": "Revolut fee",
    "Url": "https://go.xero.com/Bank/ViewTransaction.aspx?bankTransactionID=bt-001",
    "CurrencyCode": "EUR",
    "CurrencyRate": 1.0,
    "LineAmountTypes": "Inclusive",
    "Date": "/Date(1751068800000+0000)/",  # 2025-06-28
    "UpdatedDateUTC": "/Date(1751155200000+0000)/",
    "Contact": {"ContactID": "contact-1", "Name": "Revolut"},
    "BankAccount": {"AccountID": "acct-1", "Code": "1002", "Name": "Revolut EUR Main"},
    "SubTotal": 24.38,
    "TotalTax": 5.12,
    "Total": 29.50,
    "LineItems": [
        {
            "LineItemID": "li-1",
            "Description": "Merchant fees",
            "AccountCode": "404",
            "AccountID": "acct-404",
            "TaxType": "INPUT2",
            "TaxAmount": 5.12,
            "LineAmount": 29.50,
            "Quantity": 1.0,
            "UnitAmount": 29.50,
        }
    ],
}


class FakeOAuth:
    def should_refresh(self, expires_at):
        return False


def make_connection() -> StoredXeroConnection:
    return StoredXeroConnection(
        id=1,
        owner_type="workspace",
        owner_id="default",
        xero_user_id="xero-user-1",
        access_token="access-ok",
        refresh_token="refresh-ok",
        id_token=None,
        scope="offline_access accounting.transactions.read",
        token_type="Bearer",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
        selected_xero_tenant_id="tenant-1",
        is_connected=True,
        last_error=None,
    )


class FakeStore:
    def __init__(self):
        self._connection = make_connection()

    def get_connection(self, owner_type=None, owner_id=None, connection_id=None):
        return self._connection

    def list_tenants(self, owner_type=None, owner_id=None, connection_id=None):
        return [{"xero_tenant_id": "tenant-1"}]


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.executemany_calls = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, rows))

    def fetchone(self):
        return ("INSERT", 1)


class FakeConnectionCtx:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self._cursor


class FakeSql:
    def __init__(self, schema_ready=True, meta_columns=True, tenant_state=None, silver_max=None):
        self.schema_ready = schema_ready
        self.meta_columns = meta_columns
        self.tenant_state = tenant_state
        self.silver_max = silver_max
        self.cursor = FakeCursor()
        self.non_queries = []

    def execute_query(self, query, params=None):
        if "OBJECT_ID('bronze.xero_bank_transactions')" in query:
            obj = 1 if self.schema_ready else None
            return [{"bronze_tbl": obj, "header_tbl": obj, "lines_tbl": obj}]
        if "COL_LENGTH" in query:
            return [{"col_len": 8 if self.meta_columns else None}]
        if "MAX(updated_date_utc)" in query:
            return [{"last_bank_transaction_modified_utc": self.silver_max}]
        if "last_bank_transaction_sync_started_at" in query:
            return [self.tenant_state] if self.tenant_state is not None else []
        return []

    def execute_non_query(self, query, params=None):
        self.non_queries.append((query, params))

    def get_connection(self):
        return FakeConnectionCtx(self.cursor)


@dataclass
class FakeErrorResponse:
    status_code: int
    text: str = ""


class ScopeDeniedClient:
    def get_bank_transactions(self, page=1, tenant_id=None, if_modified_since=None):
        raise requests.HTTPError(
            "forbidden", response=FakeErrorResponse(status_code=403)
        )


class OnePageClient:
    def __init__(self, transactions):
        self.transactions = transactions
        self.calls = []

    def get_bank_transactions(self, page=1, tenant_id=None, if_modified_since=None):
        self.calls.append({"page": page, "tenant_id": tenant_id, "if_modified_since": if_modified_since})
        if page == 1:
            return {"BankTransactions": self.transactions}
        return {"BankTransactions": []}


class TestClientEndpoint(unittest.TestCase):
    def test_get_bank_transactions_endpoint_page_and_if_modified_since(self):
        store = FakeStore()
        client = XeroApiClient(store=store, oauth_service=FakeOAuth())

        @dataclass
        class FakeResponse:
            status_code: int = 200

            @property
            def content(self):
                return b"{}"

            def raise_for_status(self):
                return None

            def json(self):
                return {"BankTransactions": []}

        since = datetime(2026, 7, 1, 4, 0, 0, tzinfo=timezone.utc)
        with patch("shared.xero.client.requests.request") as mock_request:
            mock_request.return_value = FakeResponse()
            client.get_bank_transactions(page=2, tenant_id="tenant-alt", if_modified_since=since)

        kwargs = mock_request.call_args.kwargs
        self.assertIn("/api.xro/2.0/BankTransactions", kwargs["url"])
        self.assertEqual(kwargs["params"]["page"], 2)
        self.assertEqual(kwargs["headers"]["xero-tenant-id"], "tenant-alt")
        self.assertEqual(kwargs["headers"]["If-Modified-Since"], "Wed, 01 Jul 2026 04:00:00 GMT")


class TestTransform(unittest.TestCase):
    def test_silver_header_values_maps_payload(self):
        values = silver_header_values(SAMPLE_TXN)
        (
            txn_type,
            txn_status,
            is_reconciled,
            bank_account_id,
            bank_account_code,
            bank_account_name,
            contact_id,
            contact_name,
            reference,
            url,
            currency_code,
            currency_rate,
            line_amount_types,
            txn_date,
            updated_date_utc,
            sub_total,
            total_tax,
            total,
            has_attachments,
        ) = values

        self.assertEqual(txn_type, "SPEND")
        self.assertEqual(txn_status, "AUTHORISED")
        self.assertTrue(is_reconciled)
        self.assertEqual(bank_account_id, "acct-1")
        self.assertEqual(bank_account_code, "1002")
        self.assertEqual(bank_account_name, "Revolut EUR Main")
        self.assertEqual(contact_id, "contact-1")
        self.assertEqual(contact_name, "Revolut")
        self.assertEqual(reference, "Revolut fee")
        self.assertEqual(currency_code, "EUR")
        self.assertEqual(currency_rate, Decimal("1.000000"))
        self.assertEqual(line_amount_types, "Inclusive")
        self.assertEqual(txn_date.date(), date(2025, 6, 28))
        self.assertIsInstance(updated_date_utc, datetime)
        self.assertEqual(sub_total, Decimal("24.38"))
        self.assertEqual(total_tax, Decimal("5.12"))
        self.assertEqual(total, Decimal("29.50"))
        self.assertFalse(has_attachments)

    def test_silver_header_values_tolerates_missing_optional_fields(self):
        values = silver_header_values({"BankTransactionID": "bt-x", "Type": "RECEIVE"})
        self.assertEqual(values[0], "RECEIVE")
        self.assertIsNone(values[3])  # bank_account_id
        self.assertIsNone(values[6])  # contact_id
        self.assertFalse(values[2])  # is_reconciled defaults False

    def test_line_item_rows_shape(self):
        rows = line_item_rows("tenant-1", "bt-001", SAMPLE_TXN["LineItems"])
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(len(row), 15)
        self.assertEqual(row[0], "tenant-1")
        self.assertEqual(row[1], "bt-001")
        self.assertEqual(row[2], 0)  # line_item_index
        self.assertEqual(row[3], "Merchant fees")
        self.assertEqual(row[5], "acct-404")  # account_id
        self.assertEqual(row[6], "404")  # account_code
        self.assertEqual(row[11], Decimal("29.50"))  # line_amount
        self.assertEqual(row[12], Decimal("5.12"))  # tax_amount

    def test_line_item_rows_skips_non_dict_items(self):
        rows = line_item_rows("tenant-1", "bt-001", [None, "junk", SAMPLE_TXN["LineItems"][0]])
        self.assertEqual(len(rows), 1)


class TestMergePlaceholderParity(unittest.TestCase):
    """Every '?' in the SQL must have exactly one bound parameter."""

    def setUp(self):
        self.sql = FakeSql()
        self.service = XeroBankTransactionSyncService(sql_client=self.sql, store=FakeStore())
        self.cursor = FakeCursor()

    def test_bronze_merge_parity(self):
        self.service._upsert_bronze_transaction(
            cursor=self.cursor,
            sync_run_id="run-1",
            connection_id=1,
            tenant_id="tenant-1",
            transaction_id="bt-001",
            txn=SAMPLE_TXN,
        )
        sql, params = self.cursor.executed[-1]
        self.assertEqual(sql.count("?"), len(params))

    def test_silver_header_merge_parity(self):
        self.service._upsert_silver_transaction(
            cursor=self.cursor,
            sync_run_id="run-1",
            bronze_id=7,
            connection_id=1,
            tenant_id="tenant-1",
            transaction_id="bt-001",
            txn=SAMPLE_TXN,
        )
        sql, params = self.cursor.executed[-1]
        self.assertEqual(sql.count("?"), len(params))

    def test_line_items_insert_parity(self):
        written = self.service._replace_line_items(
            cursor=self.cursor,
            tenant_id="tenant-1",
            transaction_id="bt-001",
            line_items=SAMPLE_TXN["LineItems"],
        )
        self.assertEqual(written, 1)
        delete_sql, delete_params = self.cursor.executed[-1]
        self.assertEqual(delete_sql.count("?"), len(delete_params))
        insert_sql, rows = self.cursor.executemany_calls[-1]
        for row in rows:
            self.assertEqual(insert_sql.count("?"), len(row))


class TestServiceBehaviour(unittest.TestCase):
    def test_missing_schema_skips_run(self):
        sql = FakeSql(schema_ready=False)
        service = XeroBankTransactionSyncService(sql_client=sql, store=FakeStore())
        result = service.sync_bank_transactions()
        self.assertTrue(result["skipped_schema_missing"])
        self.assertEqual(result["tenant_count"], 0)

    def test_scope_403_skips_tenant_without_failing(self):
        sql = FakeSql()
        service = XeroBankTransactionSyncService(sql_client=sql, store=FakeStore())
        service._make_client = lambda connection_id: ScopeDeniedClient()

        result = service.sync_bank_transactions()

        self.assertEqual(result["scope_skipped_tenant_ids"], ["tenant-1"])
        self.assertEqual(result["failed_tenant_ids"], [])
        self.assertEqual(result["transaction_count_seen"], 0)
        # The scope problem is persisted on the tenant row for observability.
        self.assertTrue(
            any("last_bank_transaction_sync_error" in q for q, _ in sql.non_queries)
        )

    def test_happy_path_writes_bronze_silver_and_lines(self):
        sql = FakeSql()
        service = XeroBankTransactionSyncService(sql_client=sql, store=FakeStore())
        client = OnePageClient([SAMPLE_TXN])
        service._make_client = lambda connection_id: client

        result = service.sync_bank_transactions()

        self.assertEqual(result["transaction_count_seen"], 1)
        self.assertEqual(result["bronze_rows_created"], 1)
        self.assertEqual(result["header_rows_created"], 1)
        self.assertEqual(result["line_item_rows_written"], 1)
        self.assertEqual(result["failed_tenant_ids"], [])
        self.assertEqual(result["scope_skipped_tenant_ids"], [])
        # Watermark completion recorded on meta.xero_tenants.
        self.assertTrue(
            any("last_bank_transaction_sync_completed_at" in q for q, _ in sql.non_queries)
        )

    def test_tenant_error_marks_failed_but_run_continues(self):
        class ExplodingClient:
            def get_bank_transactions(self, **kwargs):
                raise RuntimeError("boom")

        sql = FakeSql()
        service = XeroBankTransactionSyncService(sql_client=sql, store=FakeStore())
        service._make_client = lambda connection_id: ExplodingClient()

        result = service.sync_bank_transactions()

        self.assertEqual(result["failed_tenant_ids"], ["tenant-1"])
        self.assertEqual(result["scope_skipped_tenant_ids"], [])


class TestWatermarkResolution(unittest.TestCase):
    def test_force_full_returns_none(self):
        service = XeroBankTransactionSyncService(sql_client=FakeSql(), store=FakeStore())
        self.assertIsNone(
            service._resolve_if_modified_since({"last_bank_transaction_modified_utc": datetime.now(timezone.utc)}, force_full=True)
        )

    def test_meta_watermark_minus_lookback(self):
        service = XeroBankTransactionSyncService(sql_client=FakeSql(), store=FakeStore())
        watermark = datetime(2026, 7, 7, 4, 0, 0, tzinfo=timezone.utc)
        resolved = service._resolve_if_modified_since(
            {"last_bank_transaction_modified_utc": watermark},
            force_full=False,
        )
        self.assertEqual(resolved, watermark - timedelta(minutes=5))

    def test_naive_meta_watermark_becomes_utc(self):
        service = XeroBankTransactionSyncService(sql_client=FakeSql(), store=FakeStore())
        watermark = datetime(2026, 7, 7, 4, 0, 0)  # naive, as pyodbc returns
        resolved = service._resolve_if_modified_since(
            {"last_bank_transaction_modified_utc": watermark},
            force_full=False,
        )
        self.assertEqual(resolved.tzinfo, timezone.utc)

    def test_fallback_to_silver_max_when_no_meta_state(self):
        silver_max = datetime(2026, 7, 6, 19, 0, 0, tzinfo=timezone.utc)
        service = XeroBankTransactionSyncService(
            sql_client=FakeSql(silver_max=silver_max), store=FakeStore()
        )
        resolved = service._resolve_if_modified_since(
            None, force_full=False, connection_id=1, tenant_id="tenant-1"
        )
        self.assertEqual(resolved, silver_max - timedelta(minutes=5))

    def test_no_watermark_anywhere_means_full_fetch(self):
        service = XeroBankTransactionSyncService(sql_client=FakeSql(), store=FakeStore())
        resolved = service._resolve_if_modified_since(
            None, force_full=False, connection_id=1, tenant_id="tenant-1"
        )
        self.assertIsNone(resolved)


if __name__ == "__main__":
    unittest.main()
