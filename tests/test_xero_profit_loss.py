import unittest
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import requests

from shared.xero.profit_loss_sync import (
    XeroProfitLossSyncService,
    flatten_profit_loss_report,
)
from shared.xero.store import StoredXeroConnection


SAMPLE_REPORT = {
    "Reports": [
        {
            "ReportName": "Profit and Loss",
            "ReportType": "ProfitAndLoss",
            "ReportTitles": ["Profit and Loss", "Aldgate", "1 June 2026 to 30 June 2026"],
            "UpdatedDateUTC": "/Date(1782864000000+0000)/",
            "Rows": [
                {
                    "RowType": "Header",
                    "Cells": [{"Value": ""}, {"Value": "Jun 2026"}],
                },
                {
                    "RowType": "Section",
                    "Title": "Turnover",
                    "Rows": [
                        {
                            "RowType": "Row",
                            "Cells": [
                                {
                                    "Value": "Membership Revenue",
                                    "Attributes": [
                                        {"Id": "account", "Value": "acct-sales"},
                                        {"Id": "accountCode", "Value": "200"},
                                    ],
                                },
                                {"Value": "1234.56"},
                            ],
                        },
                        {
                            "RowType": "SummaryRow",
                            "Cells": [
                                {"Value": "Total Turnover"},
                                {"Value": "1234.56"},
                            ],
                        },
                    ],
                },
                {
                    "RowType": "Section",
                    "Title": "Administrative Costs",
                    "Rows": [
                        {
                            "RowType": "Row",
                            "Cells": [
                                {
                                    "Value": "Bank Fees",
                                    "Attributes": [{"Id": "account", "Value": "acct-bank-fees"}],
                                },
                                {"Value": "(12.30)"},
                            ],
                        }
                    ],
                },
            ],
        }
    ]
}


class FakeStore:
    def __init__(self):
        self._connection = StoredXeroConnection(
            id=1,
            owner_type="workspace",
            owner_id="default",
            xero_user_id="xero-user-1",
            access_token="access-ok",
            refresh_token="refresh-ok",
            id_token=None,
            scope="offline_access accounting.reports.profitandloss.read accounting.settings.read",
            token_type="Bearer",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
            selected_xero_tenant_id="tenant-1",
            is_connected=True,
            last_error=None,
        )

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
        return ("INSERT", 42)


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
    def __init__(self, schema_ready=True, meta_columns=True, existing_months=None):
        self.schema_ready = schema_ready
        self.meta_columns = meta_columns
        self.existing_months = existing_months or []
        self.cursor = FakeCursor()
        self.non_queries = []

    def execute_query(self, query, params=None):
        if "OBJECT_ID('bronze.xero_profit_loss_reports')" in query:
            obj = 1 if self.schema_ready else None
            return [{"bronze_tbl": obj, "silver_tbl": obj}]
        if "COL_LENGTH" in query:
            return [{"col_len": 8 if self.meta_columns else None}]
        if "SELECT DISTINCT period_month" in query:
            return [{"period_month": month} for month in self.existing_months]
        return []

    def execute_non_query(self, query, params=None):
        self.non_queries.append((query, params))

    def get_connection(self):
        return FakeConnectionCtx(self.cursor)


class OneMonthClient:
    def __init__(self):
        self.report_calls = []

    def get_organisation(self, tenant_id=None):
        return {"Organisations": [{"BaseCurrency": "EUR"}]}

    def get_profit_and_loss(self, from_date, to_date, tenant_id=None, standard_layout=True):
        self.report_calls.append(
            {
                "from_date": from_date,
                "to_date": to_date,
                "tenant_id": tenant_id,
                "standard_layout": standard_layout,
            }
        )
        return SAMPLE_REPORT


class ScopeDeniedClient:
    def get_organisation(self, tenant_id=None):
        raise requests.HTTPError("forbidden", response=type("Response", (), {"status_code": 403})())


class TestProfitLossFlattening(unittest.TestCase):
    def test_flatten_includes_account_and_summary_rows(self):
        rows = flatten_profit_loss_report(SAMPLE_REPORT, date(2026, 6, 1), "EUR")

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].section, "Turnover")
        self.assertEqual(rows[0].account_id, "acct-sales")
        self.assertEqual(rows[0].account_code, "200")
        self.assertEqual(rows[0].account_name, "Membership Revenue")
        self.assertEqual(rows[0].amount, Decimal("1234.56"))
        self.assertFalse(rows[0].is_summary)

        self.assertEqual(rows[1].account_name, "Total Turnover")
        self.assertTrue(rows[1].is_summary)
        self.assertIsNone(rows[1].account_id)

        self.assertEqual(rows[2].section, "Administrative Costs")
        self.assertEqual(rows[2].amount, Decimal("-12.30"))


class TestProfitLossService(unittest.TestCase):
    def test_missing_schema_skips_run(self):
        service = XeroProfitLossSyncService(sql_client=FakeSql(schema_ready=False), store=FakeStore())
        result = service.sync_profit_loss(from_month="2026-06", to_month="2026-06")
        self.assertTrue(result["skipped_schema_missing"])
        self.assertEqual(result["tenant_count"], 0)

    def test_months_to_sync_missing_plus_recent(self):
        sql = FakeSql(existing_months=[date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)])
        service = XeroProfitLossSyncService(sql_client=sql, store=FakeStore())
        months = service._months_to_sync(
            tenant_id="tenant-1",
            start_month=date(2026, 1, 1),
            final_month=date(2026, 3, 1),
            refresh_months=2,
            force_full=False,
        )
        self.assertEqual(months, [date(2026, 2, 1), date(2026, 3, 1)])

    def test_happy_path_writes_bronze_and_replaces_silver_rows(self):
        sql = FakeSql()
        service = XeroProfitLossSyncService(sql_client=sql, store=FakeStore())
        client = OneMonthClient()
        service._make_client = lambda connection_id: client

        result = service.sync_profit_loss(
            from_month="2026-06",
            to_month="2026-06",
            force_full=True,
        )

        self.assertEqual(result["months_requested"], 1)
        self.assertEqual(result["bronze_rows_created"], 1)
        self.assertEqual(result["silver_rows_written"], 3)
        self.assertEqual(result["account_rows_written"], 2)
        self.assertEqual(result["summary_rows_written"], 1)
        self.assertEqual(result["failed_tenant_ids"], [])
        self.assertEqual(result["scope_skipped_tenant_ids"], [])
        self.assertEqual(client.report_calls[0]["to_date"], date(2026, 6, 30))

        delete_sql, delete_params = sql.cursor.executed[-1]
        self.assertIn("DELETE FROM silver.xero_profit_loss_accounts", delete_sql)
        self.assertEqual(delete_params, ("tenant-1", date(2026, 6, 1)))
        insert_sql, insert_rows = sql.cursor.executemany_calls[-1]
        self.assertEqual(insert_sql.count("?"), len(insert_rows[0]))

    def test_scope_403_skips_tenant_without_failing_run(self):
        sql = FakeSql()
        service = XeroProfitLossSyncService(sql_client=sql, store=FakeStore())
        service._make_client = lambda connection_id: ScopeDeniedClient()

        result = service.sync_profit_loss(
            from_month="2026-06",
            to_month="2026-06",
            force_full=True,
        )

        self.assertEqual(result["scope_skipped_tenant_ids"], ["tenant-1"])
        self.assertEqual(result["failed_tenant_ids"], [])


if __name__ == "__main__":
    unittest.main()
