"""
shared/xero/profit_loss_sync.py

Monthly Xero Profit & Loss report ingestion across all stored tenants.

The report is pulled one tenant/month at a time with standardLayout=true so
Xero returns account-level rows using its canonical P&L layout. Silver stores
both account rows and summary rows; bronze keeps the full raw response so the
parser can be improved without another API backfill.
"""
from __future__ import annotations

import json
import logging
import os
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import requests

from shared.xero.client import XeroApiClient
from shared.xero.flow import DEFAULT_OWNER_ID, DEFAULT_OWNER_TYPE
from shared.xero.invoice_sync import _parse_xero_datetime, _to_text
from shared.xero.store import XeroStore

logger = logging.getLogger(__name__)

DEFAULT_BACKFILL_START_MONTH = "2024-01"
DEFAULT_REFRESH_MONTHS = 3
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


@dataclass
class ProfitLossRow:
    period_month: date
    from_date: date
    to_date: date
    currency_code: Optional[str]
    section: Optional[str]
    row_type: str
    is_summary: bool
    row_order: int
    account_id: Optional[str]
    account_code: Optional[str]
    account_name: Optional[str]
    amount: Optional[Decimal]
    report_updated_utc: Optional[datetime]
    raw_row: dict[str, Any]


@dataclass
class XeroProfitLossSyncStats:
    connection_id: Optional[int] = None
    tenant_count: int = 0
    tenant_ids_processed: list[str] = field(default_factory=list)
    months_requested: int = 0
    bronze_rows_created: int = 0
    bronze_rows_updated: int = 0
    silver_rows_written: int = 0
    account_rows_written: int = 0
    summary_rows_written: int = 0
    scope_skipped_tenant_ids: list[str] = field(default_factory=list)
    failed_tenant_ids: list[str] = field(default_factory=list)
    skipped_schema_missing: bool = False
    from_month: Optional[str] = None
    to_month: Optional[str] = None
    refresh_months: int = DEFAULT_REFRESH_MONTHS

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_month(value: Optional[Any]) -> date:
    if value is None:
        value = DEFAULT_BACKFILL_START_MONTH
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)
    if isinstance(value, date):
        return date(value.year, value.month, 1)

    text = str(value).strip()
    if not text:
        return _parse_month(DEFAULT_BACKFILL_START_MONTH)
    if len(text) == 7:
        text = f"{text}-01"
    parsed = date.fromisoformat(text)
    return date(parsed.year, parsed.month, 1)


def _add_months(value: date, months: int) -> date:
    month_index = (value.year * 12 + value.month - 1) + months
    return date(month_index // 12, month_index % 12 + 1, 1)


def _month_end(value: date) -> date:
    return _add_months(value, 1) - timedelta(days=1)


def _iter_months(start_month: date, end_month: date) -> list[date]:
    months = []
    current = start_month
    while current <= end_month:
        months.append(current)
        current = _add_months(current, 1)
    return months


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("()").replace(",", "")
    try:
        amount = Decimal(text).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None
    return -amount if negative else amount


def _cell_value(row: dict[str, Any], index: int) -> Optional[str]:
    cells = row.get("Cells") if isinstance(row, dict) else None
    if not isinstance(cells, list) or index >= len(cells):
        return None
    cell = cells[index]
    if not isinstance(cell, dict):
        return None
    return _to_text(cell.get("Value"))


def _cell_attributes(row: dict[str, Any]) -> list[dict[str, Any]]:
    attrs = []
    cells = row.get("Cells") if isinstance(row, dict) else None
    if not isinstance(cells, list):
        return attrs
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        raw_attrs = cell.get("Attributes")
        if isinstance(raw_attrs, list):
            attrs.extend(attr for attr in raw_attrs if isinstance(attr, dict))
    return attrs


def _attribute_value(row: dict[str, Any], wanted_ids: set[str]) -> Optional[str]:
    for attr in _cell_attributes(row):
        attr_id = _to_text(attr.get("Id"))
        value = _to_text(attr.get("Value"))
        if not attr_id or not value:
            continue
        normalized = attr_id.lower().replace("_", "").replace("-", "")
        if normalized in wanted_ids:
            return value
    return None


def _account_id(row: dict[str, Any]) -> Optional[str]:
    value = _attribute_value(row, {"account", "accountid"})
    if value:
        return value
    for attr in _cell_attributes(row):
        value = _to_text(attr.get("Value"))
        if value and _UUID_RE.match(value):
            return value
    return None


def _account_code(row: dict[str, Any]) -> Optional[str]:
    return _attribute_value(row, {"accountcode", "code"})


def _report_currency(payload: dict[str, Any], fallback: Optional[str]) -> Optional[str]:
    reports = payload.get("Reports") if isinstance(payload, dict) else None
    if not isinstance(reports, list) or not reports:
        return fallback
    fields = reports[0].get("Fields")
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, dict):
                continue
            field_id = _to_text(field.get("FieldID")) or _to_text(field.get("Description"))
            if field_id and "currency" in field_id.lower():
                return _to_text(field.get("Value")) or fallback
    return fallback


def _base_currency(organisation_payload: dict[str, Any]) -> Optional[str]:
    organisations = organisation_payload.get("Organisations") if isinstance(organisation_payload, dict) else None
    if not isinstance(organisations, list) or not organisations:
        return None
    org = organisations[0]
    if not isinstance(org, dict):
        return None
    return _to_text(org.get("BaseCurrency")) or _to_text(org.get("CurrencyCode"))


def flatten_profit_loss_report(
    payload: dict[str, Any],
    period_month: date,
    currency_code: Optional[str],
) -> list[ProfitLossRow]:
    reports = payload.get("Reports") if isinstance(payload, dict) else None
    if not isinstance(reports, list) or not reports:
        return []

    report = reports[0]
    report_updated_utc = _parse_xero_datetime(report.get("UpdatedDateUTC"))
    resolved_currency = _report_currency(payload, currency_code)
    from_date = period_month
    to_date = _month_end(period_month)
    flattened: list[ProfitLossRow] = []
    row_order = 0

    def visit(rows: Any, section: Optional[str]) -> None:
        nonlocal row_order
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_type = _to_text(row.get("RowType")) or ""
            if row_type == "Section":
                visit(row.get("Rows"), _to_text(row.get("Title")) or section)
                continue
            if row_type not in {"Row", "SummaryRow"}:
                continue

            account_name = _cell_value(row, 0)
            amount = _to_decimal(_cell_value(row, 1))
            if account_name is None and amount is None:
                continue

            row_order += 1
            flattened.append(
                ProfitLossRow(
                    period_month=period_month,
                    from_date=from_date,
                    to_date=to_date,
                    currency_code=resolved_currency,
                    section=section,
                    row_type=row_type,
                    is_summary=row_type == "SummaryRow",
                    row_order=row_order,
                    account_id=None if row_type == "SummaryRow" else _account_id(row),
                    account_code=None if row_type == "SummaryRow" else _account_code(row),
                    account_name=account_name,
                    amount=amount,
                    report_updated_utc=report_updated_utc,
                    raw_row=row,
                )
            )

    visit(report.get("Rows"), None)
    return flattened


class XeroProfitLossSyncService:
    def __init__(self, sql_client=None, store: Optional[XeroStore] = None):
        if sql_client is None:
            from shared.azure_clients.sql_client import get_sql_client

            sql_client = get_sql_client()
        self.sql = sql_client
        self.store = store or XeroStore(sql_client=sql_client)

    def _make_client(self, connection_id: int) -> XeroApiClient:
        return XeroApiClient(connection_id=connection_id, store=self.store)

    def sync_profit_loss(
        self,
        owner_type: str = DEFAULT_OWNER_TYPE,
        owner_id: str = DEFAULT_OWNER_ID,
        connection_id: Optional[int] = None,
        tenant_id: Optional[str] = None,
        from_month: Optional[Any] = None,
        to_month: Optional[Any] = None,
        refresh_months: Optional[int] = None,
        force_full: bool = False,
        include_current_month: Optional[bool] = None,
    ) -> dict[str, Any]:
        stats = XeroProfitLossSyncStats()

        if not self._schema_ready():
            logger.warning(
                "Xero Profit & Loss tables missing; apply "
                "scripts/sql_scripts/xero_profit_loss_schema.sql; skipping run"
            )
            stats.skipped_schema_missing = True
            return stats.to_dict()

        connection = self.store.get_connection(
            connection_id=connection_id,
            owner_type=None if connection_id is not None else owner_type,
            owner_id=None if connection_id is not None else owner_id,
        )
        if connection is None:
            raise ValueError("No Xero connection found for the requested owner")

        tenants = self.store.list_tenants(connection_id=connection.id)
        if tenant_id:
            tenants = [tenant for tenant in tenants if tenant.get("xero_tenant_id") == tenant_id]
            if not tenants:
                raise ValueError(f"Tenant {tenant_id} is not linked to Xero connection {connection.id}")

        start_month = _parse_month(from_month or os.getenv("XERO_PROFIT_LOSS_BACKFILL_START", DEFAULT_BACKFILL_START_MONTH))
        final_month = self._resolve_to_month(to_month=to_month, include_current_month=include_current_month)
        refresh_count = self._resolve_refresh_months(refresh_months)

        stats.connection_id = connection.id
        stats.tenant_count = len(tenants)
        stats.from_month = start_month.isoformat()
        stats.to_month = final_month.isoformat()
        stats.refresh_months = refresh_count
        sync_run_id = str(uuid.uuid4())
        client = self._make_client(connection.id)
        meta_columns = self._meta_columns_available()

        for tenant in tenants:
            current_tenant_id = str(tenant["xero_tenant_id"])
            stats.tenant_ids_processed.append(current_tenant_id)
            try:
                months = self._months_to_sync(
                    tenant_id=current_tenant_id,
                    start_month=start_month,
                    final_month=final_month,
                    refresh_months=refresh_count,
                    force_full=force_full,
                )
                if not months:
                    continue
                if meta_columns:
                    self._mark_started(connection.id, current_tenant_id)

                currency_code = _base_currency(client.get_organisation(tenant_id=current_tenant_id))
                for month in months:
                    self._sync_tenant_month(
                        client=client,
                        sync_run_id=sync_run_id,
                        connection_id=connection.id,
                        tenant_id=current_tenant_id,
                        period_month=month,
                        currency_code=currency_code,
                        stats=stats,
                    )
                if meta_columns:
                    self._mark_completed(connection.id, current_tenant_id, max(months))
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status in {401, 403}:
                    stats.scope_skipped_tenant_ids.append(current_tenant_id)
                    if meta_columns:
                        self._mark_failed(
                            connection.id,
                            current_tenant_id,
                            f"HTTP {status}; token likely missing accounting.reports.profitandloss.read or accounting.settings.read",
                        )
                    logger.warning(
                        "Skipping Xero Profit & Loss sync (missing scope or access)",
                        extra={
                            "xero_connection_id": connection.id,
                            "xero_tenant_id": current_tenant_id,
                            "status_code": status,
                        },
                    )
                    continue
                stats.failed_tenant_ids.append(current_tenant_id)
                if meta_columns:
                    self._mark_failed(connection.id, current_tenant_id, "profit and loss sync failed")
                logger.exception(
                    "Failed syncing Xero Profit & Loss for tenant",
                    extra={"xero_connection_id": connection.id, "xero_tenant_id": current_tenant_id},
                )
            except Exception:
                stats.failed_tenant_ids.append(current_tenant_id)
                if meta_columns:
                    self._mark_failed(connection.id, current_tenant_id, "profit and loss sync failed")
                logger.exception(
                    "Failed syncing Xero Profit & Loss for tenant",
                    extra={"xero_connection_id": connection.id, "xero_tenant_id": current_tenant_id},
                )

        return stats.to_dict()

    def _sync_tenant_month(
        self,
        client: XeroApiClient,
        sync_run_id: str,
        connection_id: int,
        tenant_id: str,
        period_month: date,
        currency_code: Optional[str],
        stats: XeroProfitLossSyncStats,
    ) -> None:
        logger.info(
            "Fetching Xero Profit & Loss report",
            extra={"xero_connection_id": connection_id, "xero_tenant_id": tenant_id, "period_month": period_month.isoformat()},
        )
        payload = client.get_profit_and_loss(
            from_date=period_month,
            to_date=_month_end(period_month),
            tenant_id=tenant_id,
            standard_layout=True,
        )
        rows = flatten_profit_loss_report(
            payload=payload,
            period_month=period_month,
            currency_code=currency_code,
        )
        report = payload.get("Reports", [{}])[0] if isinstance(payload.get("Reports"), list) else {}
        report_updated_utc = _parse_xero_datetime(report.get("UpdatedDateUTC")) if isinstance(report, dict) else None
        report_titles = report.get("ReportTitles") if isinstance(report, dict) else None
        resolved_currency = rows[0].currency_code if rows else _report_currency(payload, currency_code)

        with self.sql.get_connection() as conn:
            cursor = conn.cursor()
            bronze_id, bronze_action = self._upsert_bronze_report(
                cursor=cursor,
                sync_run_id=sync_run_id,
                connection_id=connection_id,
                tenant_id=tenant_id,
                period_month=period_month,
                currency_code=resolved_currency,
                report_updated_utc=report_updated_utc,
                report_titles=report_titles,
                payload=payload,
            )
            if bronze_action == "INSERT":
                stats.bronze_rows_created += 1
            else:
                stats.bronze_rows_updated += 1

            written = self._replace_silver_rows(
                cursor=cursor,
                sync_run_id=sync_run_id,
                bronze_id=bronze_id,
                connection_id=connection_id,
                tenant_id=tenant_id,
                period_month=period_month,
                rows=rows,
            )

        stats.months_requested += 1
        stats.silver_rows_written += written
        stats.account_rows_written += sum(1 for row in rows if not row.is_summary)
        stats.summary_rows_written += sum(1 for row in rows if row.is_summary)

    def _resolve_to_month(self, to_month: Optional[Any], include_current_month: Optional[bool]) -> date:
        if to_month is not None:
            return _parse_month(to_month)
        include_current = (
            os.getenv("XERO_PROFIT_LOSS_INCLUDE_CURRENT_MONTH", "0").strip().lower()
            in {"1", "true", "yes", "on"}
            if include_current_month is None
            else include_current_month
        )
        today = datetime.now(timezone.utc).date()
        current = date(today.year, today.month, 1)
        return current if include_current else _add_months(current, -1)

    def _resolve_refresh_months(self, refresh_months: Optional[int]) -> int:
        if refresh_months is not None:
            return max(0, int(refresh_months))
        return max(0, int(os.getenv("XERO_PROFIT_LOSS_REFRESH_MONTHS", str(DEFAULT_REFRESH_MONTHS))))

    def _months_to_sync(
        self,
        tenant_id: str,
        start_month: date,
        final_month: date,
        refresh_months: int,
        force_full: bool,
    ) -> list[date]:
        all_months = _iter_months(start_month, final_month)
        if force_full:
            return all_months

        existing = self._existing_period_months(tenant_id, start_month, final_month)
        missing = {month for month in all_months if month not in existing}
        if refresh_months > 0:
            recent_start = max(start_month, _add_months(final_month, -(refresh_months - 1)))
            missing.update(_iter_months(recent_start, final_month))
        return sorted(missing)

    def _existing_period_months(self, tenant_id: str, start_month: date, final_month: date) -> set[date]:
        rows = self.sql.execute_query(
            """
            SELECT DISTINCT period_month
            FROM silver.xero_profit_loss_accounts
            WHERE xero_tenant_id = ?
              AND period_month >= ?
              AND period_month <= ?
            """,
            (tenant_id, start_month, final_month),
        )
        months = set()
        for row in rows:
            value = row.get("period_month")
            if isinstance(value, datetime):
                months.add(date(value.year, value.month, 1))
            elif isinstance(value, date):
                months.add(date(value.year, value.month, 1))
        return months

    def _schema_ready(self) -> bool:
        rows = self.sql.execute_query(
            """
            SELECT
                OBJECT_ID('bronze.xero_profit_loss_reports') AS bronze_tbl,
                OBJECT_ID('silver.xero_profit_loss_accounts') AS silver_tbl
            """
        )
        if not rows:
            return False
        row = rows[0]
        return row.get("bronze_tbl") is not None and row.get("silver_tbl") is not None

    def _meta_columns_available(self) -> bool:
        rows = self.sql.execute_query(
            "SELECT COL_LENGTH('meta.xero_tenants', 'last_profit_loss_sync_completed_at') AS col_len"
        )
        available = bool(rows) and rows[0].get("col_len") is not None
        if not available:
            logger.warning(
                "meta.xero_tenants Profit & Loss sync columns missing; "
                "sync will run without tenant-level observability"
            )
        return available

    def _mark_started(self, connection_id: int, tenant_id: str) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_tenants
            SET last_profit_loss_sync_started_at = GETUTCDATE(),
                last_profit_loss_sync_error = NULL,
                updated_at = GETUTCDATE()
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (connection_id, tenant_id),
        )

    def _mark_completed(self, connection_id: int, tenant_id: str, last_month: date) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_tenants
            SET last_profit_loss_sync_completed_at = GETUTCDATE(),
                last_profit_loss_period_month = ?,
                last_profit_loss_sync_error = NULL,
                updated_at = GETUTCDATE()
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (last_month, connection_id, tenant_id),
        )

    def _mark_failed(self, connection_id: int, tenant_id: str, error: str) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_tenants
            SET last_profit_loss_sync_completed_at = GETUTCDATE(),
                last_profit_loss_sync_error = ?,
                updated_at = GETUTCDATE()
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (error[:1024], connection_id, tenant_id),
        )

    def _upsert_bronze_report(
        self,
        cursor,
        sync_run_id: str,
        connection_id: int,
        tenant_id: str,
        period_month: date,
        currency_code: Optional[str],
        report_updated_utc: Optional[datetime],
        report_titles: Any,
        payload: dict[str, Any],
    ) -> tuple[int, str]:
        raw_json = json.dumps(payload, default=str, ensure_ascii=False)
        titles_json = json.dumps(report_titles, default=str, ensure_ascii=False) if report_titles is not None else None
        cursor.execute(
            """
            MERGE bronze.xero_profit_loss_reports AS target
            USING (SELECT ? AS xero_tenant_id, ? AS period_month) AS source
                ON target.xero_tenant_id = source.xero_tenant_id
               AND target.period_month = source.period_month
            WHEN MATCHED THEN UPDATE SET
                sync_run_id = ?,
                xero_connection_id = ?,
                from_date = ?,
                to_date = ?,
                currency_code = ?,
                report_updated_utc = ?,
                report_titles_json = ?,
                raw_json = ?,
                synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                sync_run_id,
                xero_connection_id,
                xero_tenant_id,
                period_month,
                from_date,
                to_date,
                currency_code,
                report_updated_utc,
                report_titles_json,
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            OUTPUT $action, inserted.id;
            """,
            (
                tenant_id,
                period_month,
                sync_run_id,
                connection_id,
                period_month,
                _month_end(period_month),
                currency_code,
                report_updated_utc,
                titles_json,
                raw_json,
                sync_run_id,
                connection_id,
                tenant_id,
                period_month,
                period_month,
                _month_end(period_month),
                currency_code,
                report_updated_utc,
                titles_json,
                raw_json,
            ),
        )
        action, bronze_id = cursor.fetchone()
        return int(bronze_id), str(action)

    def _replace_silver_rows(
        self,
        cursor,
        sync_run_id: str,
        bronze_id: int,
        connection_id: int,
        tenant_id: str,
        period_month: date,
        rows: list[ProfitLossRow],
    ) -> int:
        cursor.execute(
            """
            DELETE FROM silver.xero_profit_loss_accounts
            WHERE xero_tenant_id = ? AND period_month = ?
            """,
            (tenant_id, period_month),
        )

        insert_rows = [
            (
                bronze_id,
                sync_run_id,
                connection_id,
                tenant_id,
                row.period_month,
                row.from_date,
                row.to_date,
                row.currency_code,
                row.section,
                row.row_type,
                row.is_summary,
                row.row_order,
                row.account_id,
                row.account_code,
                row.account_name,
                row.amount,
                row.report_updated_utc,
                json.dumps(row.raw_row, default=str, ensure_ascii=False),
            )
            for row in rows
        ]
        if insert_rows:
            cursor.executemany(
                """
                INSERT INTO silver.xero_profit_loss_accounts (
                    bronze_id,
                    sync_run_id,
                    xero_connection_id,
                    xero_tenant_id,
                    period_month,
                    from_date,
                    to_date,
                    currency_code,
                    section,
                    row_type,
                    is_summary,
                    row_order,
                    account_id,
                    account_code,
                    account_name,
                    amount,
                    report_updated_utc,
                    raw_row_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                insert_rows,
            )

        return len(insert_rows)
