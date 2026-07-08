"""
shared/xero/bank_transaction_sync.py

Warehouse sync for Xero bank transactions (spend/receive money) across all
stored tenants on a connection. Mirrors shared/xero/invoice_sync.py.

Why: bank fees (Bank Fees, Revolut Merchant Fees, direct debits...) are coded
straight from the bank feeds as spend-money transactions and never appear on
ACCPAY invoices, so the invoice sync alone leaves a structural hole in P&L
actuals.

Data model:
  - bronze.xero_bank_transactions             raw payloads
  - silver.xero_bank_transactions             typed headers
  - silver.xero_bank_transaction_line_items   typed line items
  - silver.vw_xero_bank_transaction_pnl_lines P&L serving view (net of tax)

Scope: requires accounting.transactions.read on the stored token. A
tenant hitting 401/403 is skipped with a warning (same pattern as the accounts
sync in invoice_sync), so this can be deployed BEFORE the OAuth re-consent —
data starts flowing the night after the consent lands, without a redeploy.

Deploy-before-apply safety: if the bank transaction tables are missing the run
skips with a warning instead of failing (same convention as ava_refresh).

Deletions: Xero flips Status to DELETED and bumps UpdatedDateUTC, so the
incremental sync picks deletions up — no separate reconcile job (same
semantics as silver.xero_invoices).
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import requests

from shared.xero.client import XeroApiClient
from shared.xero.flow import DEFAULT_OWNER_ID, DEFAULT_OWNER_TYPE
from shared.xero.invoice_sync import (
    DEFAULT_INCREMENTAL_LOOKBACK,
    _parse_xero_datetime,
    _to_decimal,
    _to_text,
)
from shared.xero.store import XeroStore

logger = logging.getLogger(__name__)


@dataclass
class XeroBankTransactionSyncStats:
    connection_id: Optional[int] = None
    tenant_count: int = 0
    tenant_ids_processed: list[str] = field(default_factory=list)
    incremental_since_utc: Optional[str] = None
    transaction_count_seen: int = 0
    bronze_rows_created: int = 0
    bronze_rows_updated: int = 0
    header_rows_created: int = 0
    header_rows_updated: int = 0
    line_item_rows_written: int = 0
    scope_skipped_tenant_ids: list[str] = field(default_factory=list)
    failed_tenant_ids: list[str] = field(default_factory=list)
    skipped_schema_missing: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _contact_fields(txn: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    contact = txn.get("Contact")
    if not isinstance(contact, dict):
        return None, None
    return _to_text(contact.get("ContactID")), _to_text(contact.get("Name"))


def _bank_account_fields(txn: dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    bank_account = txn.get("BankAccount")
    if not isinstance(bank_account, dict):
        return None, None, None
    return (
        _to_text(bank_account.get("AccountID")),
        _to_text(bank_account.get("Code")),
        _to_text(bank_account.get("Name")),
    )


def silver_header_values(txn: dict[str, Any]) -> tuple:
    """Typed silver column values shared by the UPDATE and INSERT branches of
    the header MERGE (everything except the key/audit columns), in column
    order: transaction_type, transaction_status, is_reconciled,
    bank_account_id, bank_account_code, bank_account_name, contact_id,
    contact_name, reference, url, currency_code, currency_rate,
    line_amount_types, transaction_date, updated_date_utc, sub_total,
    total_tax, total, has_attachments."""
    contact_id, contact_name = _contact_fields(txn)
    bank_account_id, bank_account_code, bank_account_name = _bank_account_fields(txn)
    return (
        _to_text(txn.get("Type")),
        _to_text(txn.get("Status")),
        bool(txn.get("IsReconciled", False)),
        bank_account_id,
        bank_account_code,
        bank_account_name,
        contact_id,
        contact_name,
        _to_text(txn.get("Reference")),
        _to_text(txn.get("Url")),
        _to_text(txn.get("CurrencyCode")),
        _to_decimal(txn.get("CurrencyRate"), scale=6),
        _to_text(txn.get("LineAmountTypes")),
        _parse_xero_datetime(txn.get("Date")),
        _parse_xero_datetime(txn.get("UpdatedDateUTC")),
        _to_decimal(txn.get("SubTotal")),
        _to_decimal(txn.get("TotalTax")),
        _to_decimal(txn.get("Total")),
        bool(txn.get("HasAttachments", False)),
    )


def line_item_rows(
    tenant_id: str,
    transaction_id: str,
    line_items: list[dict[str, Any]],
) -> list[tuple]:
    """Rows for silver.xero_bank_transaction_line_items — same shape as
    silver.xero_invoice_line_items."""
    rows = []
    for index, item in enumerate(line_items):
        if not isinstance(item, dict):
            continue
        rows.append(
            (
                tenant_id,
                transaction_id,
                index,
                _to_text(item.get("Description")),
                _to_text(item.get("ItemCode")),
                _to_text(item.get("AccountID")),
                _to_text(item.get("AccountCode")),
                _to_text(item.get("TaxType")),
                json.dumps(item.get("Tracking"), default=str, ensure_ascii=False)
                if item.get("Tracking") is not None
                else None,
                _to_decimal(item.get("Quantity"), scale=4),
                _to_decimal(item.get("UnitAmount"), scale=4),
                _to_decimal(item.get("LineAmount")),
                _to_decimal(item.get("TaxAmount")),
                _to_decimal(item.get("DiscountRate"), scale=4),
                json.dumps(item, default=str, ensure_ascii=False),
            )
        )
    return rows


class XeroBankTransactionSyncService:
    def __init__(self, sql_client=None, store: Optional[XeroStore] = None):
        if sql_client is None:
            from shared.azure_clients.sql_client import get_sql_client

            sql_client = get_sql_client()
        self.sql = sql_client
        self.store = store or XeroStore(sql_client=sql_client)

    def _make_client(self, connection_id: int) -> XeroApiClient:
        return XeroApiClient(connection_id=connection_id, store=self.store)

    def sync_bank_transactions(
        self,
        owner_type: str = DEFAULT_OWNER_TYPE,
        owner_id: str = DEFAULT_OWNER_ID,
        connection_id: Optional[int] = None,
        tenant_id: Optional[str] = None,
        force_full: bool = False,
    ) -> dict[str, Any]:
        stats = XeroBankTransactionSyncStats()

        if not self._schema_ready():
            logger.warning(
                "Xero bank transaction tables missing — apply "
                "scripts/sql_scripts/xero_bank_transactions_schema.sql; skipping run"
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

        stats.connection_id = connection.id
        stats.tenant_count = len(tenants)
        sync_run_id = str(uuid.uuid4())
        client = self._make_client(connection.id)
        meta_columns = self._meta_columns_available()

        for tenant in tenants:
            current_tenant_id = str(tenant["xero_tenant_id"])
            stats.tenant_ids_processed.append(current_tenant_id)
            try:
                self._sync_tenant_bank_transactions(
                    client=client,
                    sync_run_id=sync_run_id,
                    connection_id=connection.id,
                    tenant_id=current_tenant_id,
                    force_full=force_full,
                    stats=stats,
                    meta_columns=meta_columns,
                )
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status in {401, 403}:
                    stats.scope_skipped_tenant_ids.append(current_tenant_id)
                    if meta_columns:
                        self._mark_failed(
                            connection.id,
                            current_tenant_id,
                            f"HTTP {status} — token likely missing accounting.transactions.read scope",
                        )
                    logger.warning(
                        "Skipping Xero bank transactions sync (missing scope or access)",
                        extra={
                            "xero_connection_id": connection.id,
                            "xero_tenant_id": current_tenant_id,
                            "status_code": status,
                        },
                    )
                    continue
                stats.failed_tenant_ids.append(current_tenant_id)
                if meta_columns:
                    self._mark_failed(connection.id, current_tenant_id, "bank transaction sync failed")
                logger.exception(
                    "Failed syncing Xero bank transactions for tenant",
                    extra={"xero_connection_id": connection.id, "xero_tenant_id": current_tenant_id},
                )
            except Exception:
                stats.failed_tenant_ids.append(current_tenant_id)
                if meta_columns:
                    self._mark_failed(connection.id, current_tenant_id, "bank transaction sync failed")
                logger.exception(
                    "Failed syncing Xero bank transactions for tenant",
                    extra={"xero_connection_id": connection.id, "xero_tenant_id": current_tenant_id},
                )

        return stats.to_dict()

    # ------------------------------------------------------------------ #
    # per-tenant sync                                                     #
    # ------------------------------------------------------------------ #

    def _sync_tenant_bank_transactions(
        self,
        client: XeroApiClient,
        sync_run_id: str,
        connection_id: int,
        tenant_id: str,
        force_full: bool,
        stats: XeroBankTransactionSyncStats,
        meta_columns: bool,
    ) -> None:
        tenant_state = self._get_tenant_bank_state(connection_id, tenant_id) if meta_columns else None
        if_modified_since = self._resolve_if_modified_since(
            tenant_state,
            force_full=force_full,
            connection_id=connection_id,
            tenant_id=tenant_id,
        )
        if if_modified_since is not None:
            stats.incremental_since_utc = if_modified_since.isoformat()

        if meta_columns:
            self._mark_started(connection_id, tenant_id)

        latest_modified_utc = tenant_state.get("last_bank_transaction_modified_utc") if tenant_state else None
        if isinstance(latest_modified_utc, datetime) and latest_modified_utc.tzinfo is None:
            latest_modified_utc = latest_modified_utc.replace(tzinfo=timezone.utc)

        page = 1
        while True:
            logger.info(
                "Fetching Xero bank transactions page",
                extra={"xero_connection_id": connection_id, "xero_tenant_id": tenant_id, "page": page},
            )
            payload = client.get_bank_transactions(
                page=page,
                tenant_id=tenant_id,
                if_modified_since=if_modified_since,
            )
            transactions = payload.get("BankTransactions", []) if isinstance(payload, dict) else []
            if not transactions:
                if meta_columns:
                    self._mark_completed(connection_id, tenant_id, latest_modified_utc)
                return

            logger.info(
                "Writing Xero bank transactions page",
                extra={
                    "xero_connection_id": connection_id,
                    "xero_tenant_id": tenant_id,
                    "page": page,
                    "transaction_count": len(transactions),
                },
            )
            with self.sql.get_connection() as conn:
                cursor = conn.cursor()
                for txn in transactions:
                    transaction_id = _to_text(txn.get("BankTransactionID"))
                    if not transaction_id:
                        continue

                    updated_date_utc = _parse_xero_datetime(txn.get("UpdatedDateUTC"))
                    if updated_date_utc and (
                        latest_modified_utc is None or updated_date_utc > latest_modified_utc
                    ):
                        latest_modified_utc = updated_date_utc

                    bronze_id, bronze_action = self._upsert_bronze_transaction(
                        cursor=cursor,
                        sync_run_id=sync_run_id,
                        connection_id=connection_id,
                        tenant_id=tenant_id,
                        transaction_id=transaction_id,
                        txn=txn,
                    )
                    if bronze_action == "INSERT":
                        stats.bronze_rows_created += 1
                    else:
                        stats.bronze_rows_updated += 1

                    header_action = self._upsert_silver_transaction(
                        cursor=cursor,
                        sync_run_id=sync_run_id,
                        bronze_id=bronze_id,
                        connection_id=connection_id,
                        tenant_id=tenant_id,
                        transaction_id=transaction_id,
                        txn=txn,
                    )
                    if header_action == "INSERT":
                        stats.header_rows_created += 1
                    else:
                        stats.header_rows_updated += 1

                    stats.line_item_rows_written += self._replace_line_items(
                        cursor=cursor,
                        tenant_id=tenant_id,
                        transaction_id=transaction_id,
                        line_items=txn.get("LineItems") or [],
                    )

                    stats.transaction_count_seen += 1

            page += 1

    # ------------------------------------------------------------------ #
    # watermark helpers                                                   #
    # ------------------------------------------------------------------ #

    def _resolve_if_modified_since(
        self,
        tenant_state: Optional[dict[str, Any]],
        force_full: bool,
        connection_id: Optional[int] = None,
        tenant_id: Optional[str] = None,
    ) -> Optional[datetime]:
        if force_full:
            return None

        last_modified = tenant_state.get("last_bank_transaction_modified_utc") if tenant_state else None
        if not isinstance(last_modified, datetime) and connection_id is not None and tenant_id is not None:
            last_modified = self._load_existing_watermark(connection_id, tenant_id)
        if not isinstance(last_modified, datetime):
            return None
        if last_modified.tzinfo is None:
            last_modified = last_modified.replace(tzinfo=timezone.utc)
        return last_modified - DEFAULT_INCREMENTAL_LOOKBACK

    def _load_existing_watermark(
        self,
        connection_id: int,
        tenant_id: str,
    ) -> Optional[datetime]:
        rows = self.sql.execute_query(
            """
            SELECT MAX(updated_date_utc) AS last_bank_transaction_modified_utc
            FROM silver.xero_bank_transactions
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (connection_id, tenant_id),
        )
        if not rows:
            return None
        value = rows[0].get("last_bank_transaction_modified_utc")
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value if isinstance(value, datetime) else None

    # ------------------------------------------------------------------ #
    # deploy-safety checks                                                #
    # ------------------------------------------------------------------ #

    def _schema_ready(self) -> bool:
        rows = self.sql.execute_query(
            """
            SELECT
                OBJECT_ID('bronze.xero_bank_transactions') AS bronze_tbl,
                OBJECT_ID('silver.xero_bank_transactions') AS header_tbl,
                OBJECT_ID('silver.xero_bank_transaction_line_items') AS lines_tbl
            """
        )
        if not rows:
            return False
        row = rows[0]
        return all(row.get(key) is not None for key in ("bronze_tbl", "header_tbl", "lines_tbl"))

    def _meta_columns_available(self) -> bool:
        rows = self.sql.execute_query(
            "SELECT COL_LENGTH('meta.xero_tenants', 'last_bank_transaction_modified_utc') AS col_len"
        )
        available = bool(rows) and rows[0].get("col_len") is not None
        if not available:
            logger.warning(
                "meta.xero_tenants bank-transaction watermark columns missing — "
                "falling back to the silver MAX(updated_date_utc) watermark"
            )
        return available

    # ------------------------------------------------------------------ #
    # meta.xero_tenants state (only called when the columns exist)        #
    # ------------------------------------------------------------------ #

    def _get_tenant_bank_state(self, connection_id: int, tenant_id: str) -> Optional[dict[str, Any]]:
        rows = self.sql.execute_query(
            """
            SELECT TOP 1
                last_bank_transaction_sync_started_at,
                last_bank_transaction_sync_completed_at,
                last_bank_transaction_modified_utc,
                last_bank_transaction_sync_error
            FROM meta.xero_tenants
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (connection_id, tenant_id),
        )
        return rows[0] if rows else None

    def _mark_started(self, connection_id: int, tenant_id: str) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_tenants
            SET last_bank_transaction_sync_started_at = GETUTCDATE(),
                last_bank_transaction_sync_error = NULL,
                updated_at = GETUTCDATE()
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (connection_id, tenant_id),
        )

    def _mark_completed(
        self,
        connection_id: int,
        tenant_id: str,
        last_modified_utc: Optional[datetime],
    ) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_tenants
            SET last_bank_transaction_sync_completed_at = GETUTCDATE(),
                last_bank_transaction_modified_utc = COALESCE(?, last_bank_transaction_modified_utc),
                last_bank_transaction_sync_error = NULL,
                updated_at = GETUTCDATE()
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (last_modified_utc, connection_id, tenant_id),
        )

    def _mark_failed(self, connection_id: int, tenant_id: str, error: str) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_tenants
            SET last_bank_transaction_sync_completed_at = GETUTCDATE(),
                last_bank_transaction_sync_error = ?,
                updated_at = GETUTCDATE()
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (error[:1024], connection_id, tenant_id),
        )

    # ------------------------------------------------------------------ #
    # writes                                                              #
    # ------------------------------------------------------------------ #

    def _upsert_bronze_transaction(
        self,
        cursor,
        sync_run_id: str,
        connection_id: int,
        tenant_id: str,
        transaction_id: str,
        txn: dict[str, Any],
    ) -> tuple[int, str]:
        raw_json = json.dumps(txn, default=str, ensure_ascii=False)
        cursor.execute(
            """
            MERGE bronze.xero_bank_transactions AS target
            USING (SELECT ? AS xero_tenant_id, ? AS source_id) AS source
                ON target.xero_tenant_id = source.xero_tenant_id
               AND target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET
                sync_run_id = ?,
                xero_connection_id = ?,
                raw_json = ?,
                synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                sync_run_id,
                xero_connection_id,
                xero_tenant_id,
                source_id,
                raw_json
            ) VALUES (?, ?, ?, ?, ?)
            OUTPUT $action, inserted.id;
            """,
            (
                tenant_id,
                transaction_id,
                sync_run_id,
                connection_id,
                raw_json,
                sync_run_id,
                connection_id,
                tenant_id,
                transaction_id,
                raw_json,
            ),
        )
        action, bronze_id = cursor.fetchone()
        return int(bronze_id), str(action)

    def _upsert_silver_transaction(
        self,
        cursor,
        sync_run_id: str,
        bronze_id: int,
        connection_id: int,
        tenant_id: str,
        transaction_id: str,
        txn: dict[str, Any],
    ) -> str:
        values = silver_header_values(txn)
        cursor.execute(
            """
            MERGE silver.xero_bank_transactions AS target
            USING (SELECT ? AS xero_tenant_id, ? AS source_id) AS source
                ON target.xero_tenant_id = source.xero_tenant_id
               AND target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET
                bronze_id = ?,
                sync_run_id = ?,
                xero_connection_id = ?,
                transaction_type = ?,
                transaction_status = ?,
                is_reconciled = ?,
                bank_account_id = ?,
                bank_account_code = ?,
                bank_account_name = ?,
                contact_id = ?,
                contact_name = ?,
                reference = ?,
                url = ?,
                currency_code = ?,
                currency_rate = ?,
                line_amount_types = ?,
                transaction_date = ?,
                updated_date_utc = ?,
                sub_total = ?,
                total_tax = ?,
                total = ?,
                has_attachments = ?,
                last_synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                bronze_id,
                sync_run_id,
                xero_connection_id,
                xero_tenant_id,
                source_id,
                transaction_type,
                transaction_status,
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
                transaction_date,
                updated_date_utc,
                sub_total,
                total_tax,
                total,
                has_attachments
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            OUTPUT $action;
            """,
            (
                tenant_id,
                transaction_id,
                bronze_id,
                sync_run_id,
                connection_id,
                *values,
                bronze_id,
                sync_run_id,
                connection_id,
                tenant_id,
                transaction_id,
                *values,
            ),
        )
        row = cursor.fetchone()
        return str(row[0])

    def _replace_line_items(
        self,
        cursor,
        tenant_id: str,
        transaction_id: str,
        line_items: list[dict[str, Any]],
    ) -> int:
        cursor.execute(
            """
            DELETE FROM silver.xero_bank_transaction_line_items
            WHERE xero_tenant_id = ? AND bank_transaction_source_id = ?
            """,
            (tenant_id, transaction_id),
        )

        rows = line_item_rows(tenant_id, transaction_id, line_items)
        if rows:
            cursor.executemany(
                """
                INSERT INTO silver.xero_bank_transaction_line_items (
                    xero_tenant_id,
                    bank_transaction_source_id,
                    line_item_index,
                    description,
                    item_code,
                    account_id,
                    account_code,
                    tax_type,
                    tracking_json,
                    quantity,
                    unit_amount,
                    line_amount,
                    tax_amount,
                    discount_rate,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        return len(rows)
