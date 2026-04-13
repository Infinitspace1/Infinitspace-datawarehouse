"""
shared/xero/invoice_sync.py

Warehouse sync for Xero invoices across all stored tenants on a connection.

Data model:
  - bronze.xero_accounts: raw account payloads
  - bronze.xero_invoices: raw invoice payloads
  - silver.xero_accounts: typed account rows
  - silver.xero_invoices: typed invoice rows
  - silver.xero_invoice_line_items: typed invoice line items
  - bronze.xero_invoice_pdfs: optional cached PDF metadata/blob paths
"""
from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import requests

from shared.azure_clients.blob_writer import BlobWriter
from shared.xero.client import XeroApiClient
from shared.xero.flow import DEFAULT_OWNER_ID, DEFAULT_OWNER_TYPE
from shared.xero.store import XeroStore
from shared.xero.tenant_directory import XeroTenantDirectoryService

_XERO_DOTNET_DATE_RE = re.compile(r"^/Date\((?P<ms>-?\d+)(?P<offset>[+-]\d{4})?\)/$")
logger = logging.getLogger(__name__)
DEFAULT_INCREMENTAL_LOOKBACK = timedelta(minutes=5)
PDF_FETCH_MAX_RETRIES = 5
PDF_FETCH_DEFAULT_RETRY_AFTER_SECONDS = 60
PDF_FETCH_THROTTLE_SECONDS = 1.2


@dataclass
class XeroInvoiceSyncStats:
    connection_id: Optional[int] = None
    tenant_count: int = 0
    tenant_ids_processed: list[str] = None
    incremental_since_utc: Optional[str] = None
    account_rows_created: int = 0
    account_rows_updated: int = 0
    account_count_seen: int = 0
    invoice_rows_created: int = 0
    invoice_rows_updated: int = 0
    bronze_rows_created: int = 0
    bronze_rows_updated: int = 0
    line_item_rows_written: int = 0
    pdf_rows_cached: int = 0
    invoice_count_seen: int = 0
    account_sync_skipped_tenant_ids: list[str] = None
    failed_tenant_ids: list[str] = None

    def __post_init__(self) -> None:
        if self.tenant_ids_processed is None:
            self.tenant_ids_processed = []
        if self.account_sync_skipped_tenant_ids is None:
            self.account_sync_skipped_tenant_ids = []
        if self.failed_tenant_ids is None:
            self.failed_tenant_ids = []

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _to_decimal(value: Any, scale: int = 2) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        quant = Decimal("1").scaleb(-scale)
        return Decimal(str(value)).quantize(quant)
    except (InvalidOperation, ValueError, TypeError):
        return None


def _to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_xero_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, str):
        match = _XERO_DOTNET_DATE_RE.match(value.strip())
        if match:
            ms = int(match.group("ms"))
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None

    return None


def _invoice_contact(invoice: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    contact = invoice.get("Contact")
    if not isinstance(contact, dict):
        return None, None
    return _to_text(contact.get("ContactID")), _to_text(contact.get("Name"))


def _account_label(account_code: Optional[str], account_name: Optional[str]) -> Optional[str]:
    if account_code and account_name:
        return f"{account_code} - {account_name}"
    return account_name or account_code


class XeroInvoiceSyncService:
    def __init__(self, sql_client=None, store: Optional[XeroStore] = None, blob_writer: Optional[BlobWriter] = None):
        if sql_client is None:
            from shared.azure_clients.sql_client import get_sql_client

            sql_client = get_sql_client()
        self.sql = sql_client
        self.store = store or XeroStore(sql_client=sql_client)
        self._blob_writer = blob_writer  # lazily instantiated on first PDF upload

    def sync_invoices(
        self,
        owner_type: str = DEFAULT_OWNER_TYPE,
        owner_id: str = DEFAULT_OWNER_ID,
        connection_id: Optional[int] = None,
        tenant_id: Optional[str] = None,
        include_pdfs: bool = False,
        force_full: bool = False,
    ) -> dict[str, Any]:
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

        stats = XeroInvoiceSyncStats(connection_id=connection.id, tenant_count=len(tenants))
        sync_run_id = str(uuid.uuid4())
        client = XeroApiClient(connection_id=connection.id, store=self.store)

        for tenant in tenants:
            current_tenant_id = str(tenant["xero_tenant_id"])
            stats.tenant_ids_processed.append(current_tenant_id)
            try:
                self._sync_tenant_invoices(
                    client=client,
                    sync_run_id=sync_run_id,
                    connection_id=connection.id,
                    tenant_id=current_tenant_id,
                    include_pdfs=include_pdfs,
                    force_full=force_full,
                    stats=stats,
                )
            except Exception:
                stats.failed_tenant_ids.append(current_tenant_id)
                self.store.mark_invoice_sync_failed(
                    connection.id,
                    current_tenant_id,
                    "invoice sync failed",
                )
                logger.exception(
                    "Failed syncing Xero invoices for tenant",
                    extra={"xero_connection_id": connection.id, "xero_tenant_id": current_tenant_id},
                )

        tenant_directory = XeroTenantDirectoryService(
            sql_client=self.sql,
            store=self.store,
        ).refresh_tenants(connection_id=connection.id)

        result = stats.to_dict()
        result["tenant_directory"] = tenant_directory
        return result

    def cache_invoice_pdf(
        self,
        invoice_id: str,
        owner_type: str = DEFAULT_OWNER_TYPE,
        owner_id: str = DEFAULT_OWNER_ID,
        connection_id: Optional[int] = None,
        tenant_id: Optional[str] = None,
    ) -> dict[str, Any]:
        connection = self.store.get_connection(
            connection_id=connection_id,
            owner_type=None if connection_id is not None else owner_type,
            owner_id=None if connection_id is not None else owner_id,
        )
        if connection is None:
            raise ValueError("No Xero connection found for the requested owner")

        resolved_tenant_id = tenant_id or connection.selected_xero_tenant_id
        if not resolved_tenant_id:
            tenants = self.store.list_tenants(connection_id=connection.id)
            if not tenants:
                raise ValueError("No Xero tenant found for this connection")
            resolved_tenant_id = str(tenants[0]["xero_tenant_id"])

        client = XeroApiClient(connection_id=connection.id, store=self.store)
        invoice_payload = client.get_invoice(
            invoice_id=invoice_id,
            tenant_id=resolved_tenant_id,
        )
        pdf_bytes, content_type, file_name = self._fetch_invoice_pdf_with_retry(
            client=client,
            invoice_id=invoice_id,
            tenant_id=resolved_tenant_id,
        )
        blob_path = self._upload_pdf(
            tenant_id=resolved_tenant_id,
            invoice_source_id=invoice_id,
            pdf_bytes=pdf_bytes,
            content_type=content_type,
        )

        sync_run_id = str(uuid.uuid4())
        with self.sql.get_connection() as conn:
            cursor = conn.cursor()
            self._upsert_invoice_pdf(
                cursor=cursor,
                sync_run_id=sync_run_id,
                connection_id=connection.id,
                tenant_id=resolved_tenant_id,
                invoice_id=invoice_id,
                blob_path=blob_path,
                content_type=content_type,
                file_name=file_name,
            )

        return {
            "connection_id": connection.id,
            "tenant_id": resolved_tenant_id,
            "invoice_id": invoice_id,
            "invoice_payload": invoice_payload,
            "content_type": content_type,
            "file_name": file_name,
            "byte_count": len(pdf_bytes),
            "blob_path": blob_path,
        }

    def get_cached_invoice_pdf(
        self,
        invoice_id: str,
        tenant_id: str,
    ) -> Optional[dict[str, Any]]:
        """
        Returns the cached PDF record (with blob_path) if it exists.
        To get the actual bytes, call blob_writer.read_pdf(row['blob_path']).
        """
        rows = self.sql.execute_query(
            """
            SELECT TOP 1 invoice_source_id, xero_tenant_id, content_type, file_name, blob_path, synced_at
            FROM bronze.xero_invoice_pdfs
            WHERE xero_tenant_id = ? AND invoice_source_id = ?
            ORDER BY synced_at DESC, id DESC
            """,
            (tenant_id, invoice_id),
        )
        if not rows:
            return None
        return rows[0]

    def cache_overdue_pdfs(self) -> dict[str, Any]:
        """Alias kept for backwards compatibility — delegates to cache_missing_pdfs."""
        return self.cache_missing_pdfs()

    def cache_missing_pdfs(self, limit: Optional[int] = None) -> dict[str, Any]:
        """
        Fetch and cache PDFs for all invoices that don't have a blob_path yet,
        regardless of status (paid, overdue, voided, etc.).
        Called after the main invoice sync so newly synced invoices get a PDF.
        The pdf_blob_path IS NULL filter is the natural watermark — already-cached
        invoices are never re-fetched.
        """
        top = f"TOP {limit}" if limit else ""
        rows = self.sql.execute_query(
            f"""
            SELECT {top}
                xi.source_id,
                xi.xero_tenant_id,
                xi.xero_connection_id,
                xi.invoice_number
            FROM silver.xero_invoices xi
            WHERE xi.pdf_blob_path IS NULL
            ORDER BY xi.invoice_date DESC
            """
        )
        ok = errors = 0
        sync_run_id = str(uuid.uuid4())
        connections: dict[int, XeroApiClient] = {}

        for row in rows:
            conn_id = int(row["xero_connection_id"])
            if conn_id not in connections:
                connections[conn_id] = XeroApiClient(connection_id=conn_id, store=self.store)
            client = connections[conn_id]

            try:
                pdf_bytes, content_type, file_name = self._fetch_invoice_pdf_with_retry(
                    client=client,
                    invoice_id=str(row["source_id"]),
                    tenant_id=str(row["xero_tenant_id"]),
                )
                blob_path = self._upload_pdf(
                    tenant_id=str(row["xero_tenant_id"]),
                    invoice_source_id=str(row["source_id"]),
                    pdf_bytes=pdf_bytes,
                    content_type=content_type,
                )
                with self.sql.get_connection() as conn:
                    cursor = conn.cursor()
                    self._upsert_invoice_pdf(
                        cursor=cursor,
                        sync_run_id=sync_run_id,
                        connection_id=conn_id,
                        tenant_id=str(row["xero_tenant_id"]),
                        invoice_id=str(row["source_id"]),
                        blob_path=blob_path,
                        content_type=content_type,
                        file_name=file_name,
                    )
                ok += 1
                logger.info(
                    "PDF cached for invoice missing a blob path",
                    extra={"invoice_number": row["invoice_number"], "blob_path": blob_path},
                )
            except Exception:
                errors += 1
                logger.exception(
                    "Failed to cache PDF for invoice missing a blob path",
                    extra={"invoice_number": row["invoice_number"]},
                )
            time.sleep(PDF_FETCH_THROTTLE_SECONDS)

        return {"pdfs_cached": ok, "pdfs_failed": errors, "pdfs_total": len(rows)}

    def _fetch_invoice_pdf_with_retry(
        self,
        client: XeroApiClient,
        invoice_id: str,
        tenant_id: str,
    ) -> tuple[bytes, str, Optional[str]]:
        for attempt in range(1, PDF_FETCH_MAX_RETRIES + 1):
            try:
                return client.get_invoice_pdf(
                    invoice_id=invoice_id,
                    tenant_id=tenant_id,
                )
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code != 429 or attempt == PDF_FETCH_MAX_RETRIES:
                    raise

                wait_seconds = self._resolve_retry_after_seconds(exc)
                logger.warning(
                    "Xero PDF fetch rate limited; retrying",
                    extra={
                        "invoice_id": invoice_id,
                        "tenant_id": tenant_id,
                        "attempt": attempt,
                        "max_retries": PDF_FETCH_MAX_RETRIES,
                        "wait_seconds": wait_seconds,
                    },
                )
                time.sleep(wait_seconds + 1)

        raise RuntimeError("unreachable")  # pragma: no cover

    def _resolve_retry_after_seconds(self, exc: requests.HTTPError) -> int:
        if exc.response is None:
            return PDF_FETCH_DEFAULT_RETRY_AFTER_SECONDS

        raw_value = exc.response.headers.get("Retry-After")
        try:
            return max(1, int(str(raw_value).strip()))
        except (AttributeError, TypeError, ValueError):
            return PDF_FETCH_DEFAULT_RETRY_AFTER_SECONDS

    def _upload_pdf(
        self,
        tenant_id: str,
        invoice_source_id: str,
        pdf_bytes: bytes,
        content_type: str,
    ) -> str:
        """Upload PDF bytes to blob storage and return the blob path."""
        if self._blob_writer is None:
            self._blob_writer = BlobWriter()
        return self._blob_writer.write_pdf(
            tenant_id=tenant_id,
            invoice_source_id=invoice_source_id,
            pdf_bytes=pdf_bytes,
            content_type=content_type,
        )

    def list_invoices(
        self,
        owner_type: str = DEFAULT_OWNER_TYPE,
        owner_id: str = DEFAULT_OWNER_ID,
        connection_id: Optional[int] = None,
        tenant_id: Optional[str] = None,
        top: int = 100,
    ) -> list[dict[str, Any]]:
        top = max(1, min(int(top), 500))

        if connection_id is None:
            connection = self.store.get_connection(owner_type=owner_type, owner_id=owner_id)
            if connection is None:
                raise ValueError("No Xero connection found for the requested owner")
            connection_id = connection.id

        if tenant_id:
            return self.sql.execute_query(
                f"""
                SELECT TOP {top}
                    id, xero_connection_id, xero_tenant_id, source_id, invoice_number,
                    invoice_type, invoice_status, contact_name, invoice_date, due_date,
                    currency_code, total, amount_due, amount_paid, amount_credited,
                    pdf_cached_at, pdf_file_name, updated_date_utc, last_synced_at
                FROM silver.xero_invoices
                WHERE xero_connection_id = ? AND xero_tenant_id = ?
                ORDER BY invoice_date DESC, updated_date_utc DESC
                """,
                (connection_id, tenant_id),
            )

        return self.sql.execute_query(
            f"""
            SELECT TOP {top}
                id, xero_connection_id, xero_tenant_id, source_id, invoice_number,
                invoice_type, invoice_status, contact_name, invoice_date, due_date,
                currency_code, total, amount_due, amount_paid, amount_credited,
                pdf_cached_at, pdf_file_name, updated_date_utc, last_synced_at
            FROM silver.xero_invoices
            WHERE xero_connection_id = ?
            ORDER BY invoice_date DESC, updated_date_utc DESC
            """,
            (connection_id,),
        )

    def _sync_tenant_invoices(
        self,
        client: XeroApiClient,
        sync_run_id: str,
        connection_id: int,
        tenant_id: str,
        include_pdfs: bool,
        force_full: bool,
        stats: XeroInvoiceSyncStats,
    ) -> None:
        tenant_state = self.store.get_tenant(connection_id=connection_id, tenant_id=tenant_id)
        if_modified_since = self._resolve_if_modified_since(
            tenant_state,
            force_full=force_full,
            connection_id=connection_id,
            tenant_id=tenant_id,
        )
        if if_modified_since is not None:
            stats.incremental_since_utc = if_modified_since.isoformat()

        self.store.mark_invoice_sync_started(connection_id=connection_id, tenant_id=tenant_id)
        self._sync_tenant_accounts(
            client=client,
            sync_run_id=sync_run_id,
            connection_id=connection_id,
            tenant_id=tenant_id,
            stats=stats,
        )
        latest_modified_utc = tenant_state.get("last_invoice_modified_utc") if tenant_state else None
        if isinstance(latest_modified_utc, datetime) and latest_modified_utc.tzinfo is None:
            latest_modified_utc = latest_modified_utc.replace(tzinfo=timezone.utc)

        page = 1
        while True:
            logger.info(
                "Fetching Xero invoices page",
                extra={"xero_connection_id": connection_id, "xero_tenant_id": tenant_id, "page": page},
            )
            payload = client.get_invoices(
                summary_only=False,
                page=page,
                tenant_id=tenant_id,
                if_modified_since=if_modified_since,
            )
            invoices = payload.get("Invoices", []) if isinstance(payload, dict) else []
            if not invoices:
                self.store.mark_invoice_sync_completed(
                    connection_id=connection_id,
                    tenant_id=tenant_id,
                    last_invoice_modified_utc=latest_modified_utc,
                )
                return

            logger.info(
                "Writing Xero invoices page",
                extra={
                    "xero_connection_id": connection_id,
                    "xero_tenant_id": tenant_id,
                    "page": page,
                    "invoice_count": len(invoices),
                },
            )
            with self.sql.get_connection() as conn:
                cursor = conn.cursor()
                for invoice in invoices:
                    invoice_id = _to_text(invoice.get("InvoiceID"))
                    if not invoice_id:
                        continue

                    updated_date_utc = _parse_xero_datetime(invoice.get("UpdatedDateUTC"))
                    if updated_date_utc and (
                        latest_modified_utc is None or updated_date_utc > latest_modified_utc
                    ):
                        latest_modified_utc = updated_date_utc

                    bronze_id, bronze_action = self._upsert_bronze_invoice(
                        cursor=cursor,
                        sync_run_id=sync_run_id,
                        connection_id=connection_id,
                        tenant_id=tenant_id,
                        invoice_id=invoice_id,
                        invoice=invoice,
                    )
                    if bronze_action == "INSERT":
                        stats.bronze_rows_created += 1
                    else:
                        stats.bronze_rows_updated += 1

                    invoice_action = self._upsert_silver_invoice(
                        cursor=cursor,
                        sync_run_id=sync_run_id,
                        bronze_id=bronze_id,
                        connection_id=connection_id,
                        tenant_id=tenant_id,
                        invoice=invoice,
                    )
                    if invoice_action == "INSERT":
                        stats.invoice_rows_created += 1
                    else:
                        stats.invoice_rows_updated += 1

                    stats.line_item_rows_written += self._replace_line_items(
                        cursor=cursor,
                        tenant_id=tenant_id,
                        invoice_id=invoice_id,
                        line_items=invoice.get("LineItems") or [],
                    )

                    if include_pdfs:
                        pdf_bytes, content_type, file_name = self._fetch_invoice_pdf_with_retry(
                            client=client,
                            invoice_id=invoice_id,
                            tenant_id=tenant_id,
                        )
                        blob_path = self._upload_pdf(
                            tenant_id=tenant_id,
                            invoice_source_id=invoice_id,
                            pdf_bytes=pdf_bytes,
                            content_type=content_type,
                        )
                        self._upsert_invoice_pdf(
                            cursor=cursor,
                            sync_run_id=sync_run_id,
                            connection_id=connection_id,
                            tenant_id=tenant_id,
                            invoice_id=invoice_id,
                            blob_path=blob_path,
                            content_type=content_type,
                            file_name=file_name,
                        )
                        stats.pdf_rows_cached += 1
                        time.sleep(PDF_FETCH_THROTTLE_SECONDS)

                    stats.invoice_count_seen += 1

            page += 1

    def _sync_tenant_accounts(
        self,
        client: XeroApiClient,
        sync_run_id: str,
        connection_id: int,
        tenant_id: str,
        stats: XeroInvoiceSyncStats,
    ) -> None:
        try:
            payload = client.get_accounts(tenant_id=tenant_id)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in {401, 403}:
                stats.account_sync_skipped_tenant_ids.append(tenant_id)
                logger.warning(
                    "Skipping Xero accounts sync because the connection is missing settings scope or access",
                    extra={"xero_connection_id": connection_id, "xero_tenant_id": tenant_id, "status_code": status},
                )
                return
            raise

        accounts = payload.get("Accounts", []) if isinstance(payload, dict) else []
        if not accounts:
            return

        with self.sql.get_connection() as conn:
            cursor = conn.cursor()
            for account in accounts:
                account_id = _to_text(account.get("AccountID"))
                if not account_id:
                    continue

                bronze_id, bronze_action = self._upsert_bronze_account(
                    cursor=cursor,
                    sync_run_id=sync_run_id,
                    connection_id=connection_id,
                    tenant_id=tenant_id,
                    account_id=account_id,
                    account=account,
                )
                if bronze_action == "INSERT":
                    stats.account_rows_created += 1
                else:
                    stats.account_rows_updated += 1

                self._upsert_silver_account(
                    cursor=cursor,
                    sync_run_id=sync_run_id,
                    bronze_id=bronze_id,
                    connection_id=connection_id,
                    tenant_id=tenant_id,
                    account=account,
                )
                stats.account_count_seen += 1

    def _resolve_if_modified_since(
        self,
        tenant_state: Optional[dict[str, Any]],
        force_full: bool,
        connection_id: Optional[int] = None,
        tenant_id: Optional[str] = None,
    ) -> Optional[datetime]:
        if force_full or not tenant_state:
            if force_full:
                return None
            if connection_id is None or tenant_id is None:
                return None
            last_modified = self._load_existing_invoice_watermark(connection_id, tenant_id)
            if last_modified is None:
                return None
            return last_modified - DEFAULT_INCREMENTAL_LOOKBACK

        last_modified = tenant_state.get("last_invoice_modified_utc")
        if not isinstance(last_modified, datetime) and connection_id is not None and tenant_id is not None:
            last_modified = self._load_existing_invoice_watermark(connection_id, tenant_id)
        if not isinstance(last_modified, datetime):
            return None
        if last_modified.tzinfo is None:
            last_modified = last_modified.replace(tzinfo=timezone.utc)
        return last_modified - DEFAULT_INCREMENTAL_LOOKBACK

    def _load_existing_invoice_watermark(
        self,
        connection_id: int,
        tenant_id: str,
    ) -> Optional[datetime]:
        rows = self.sql.execute_query(
            """
            SELECT MAX(updated_date_utc) AS last_invoice_modified_utc
            FROM silver.xero_invoices
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (connection_id, tenant_id),
        )
        if not rows:
            return None
        value = rows[0].get("last_invoice_modified_utc")
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value if isinstance(value, datetime) else None

    def _upsert_bronze_invoice(
        self,
        cursor,
        sync_run_id: str,
        connection_id: int,
        tenant_id: str,
        invoice_id: str,
        invoice: dict[str, Any],
    ) -> tuple[int, str]:
        cursor.execute(
            """
            MERGE bronze.xero_invoices AS target
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
                invoice_id,
                sync_run_id,
                connection_id,
                json.dumps(invoice, default=str, ensure_ascii=False),
                sync_run_id,
                connection_id,
                tenant_id,
                invoice_id,
                json.dumps(invoice, default=str, ensure_ascii=False),
            ),
        )
        action, bronze_id = cursor.fetchone()
        return int(bronze_id), str(action)

    def _upsert_bronze_account(
        self,
        cursor,
        sync_run_id: str,
        connection_id: int,
        tenant_id: str,
        account_id: str,
        account: dict[str, Any],
    ) -> tuple[int, str]:
        cursor.execute(
            """
            MERGE bronze.xero_accounts AS target
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
                account_id,
                sync_run_id,
                connection_id,
                json.dumps(account, default=str, ensure_ascii=False),
                sync_run_id,
                connection_id,
                tenant_id,
                account_id,
                json.dumps(account, default=str, ensure_ascii=False),
            ),
        )
        action, bronze_id = cursor.fetchone()
        return int(bronze_id), str(action)

    def _upsert_silver_account(
        self,
        cursor,
        sync_run_id: str,
        bronze_id: int,
        connection_id: int,
        tenant_id: str,
        account: dict[str, Any],
    ) -> str:
        account_id = _to_text(account.get("AccountID"))
        account_code = _to_text(account.get("Code"))
        account_name = _to_text(account.get("Name"))
        cursor.execute(
            """
            MERGE silver.xero_accounts AS target
            USING (SELECT ? AS xero_tenant_id, ? AS source_id) AS source
                ON target.xero_tenant_id = source.xero_tenant_id
               AND target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET
                bronze_id = ?,
                sync_run_id = ?,
                xero_connection_id = ?,
                account_code = ?,
                account_name = ?,
                account_label = ?,
                account_type = ?,
                account_class = ?,
                status = ?,
                description = ?,
                tax_type = ?,
                enable_payments_to_account = ?,
                show_in_expense_claims = ?,
                last_synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                bronze_id,
                sync_run_id,
                xero_connection_id,
                xero_tenant_id,
                source_id,
                account_code,
                account_name,
                account_label,
                account_type,
                account_class,
                status,
                description,
                tax_type,
                enable_payments_to_account,
                show_in_expense_claims
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            OUTPUT $action;
            """,
            (
                tenant_id,
                account_id,
                bronze_id,
                sync_run_id,
                connection_id,
                account_code,
                account_name,
                _account_label(account_code, account_name),
                _to_text(account.get("Type")),
                _to_text(account.get("Class")),
                _to_text(account.get("Status")),
                _to_text(account.get("Description")),
                _to_text(account.get("TaxType")),
                bool(account.get("EnablePaymentsToAccount", False)),
                bool(account.get("ShowInExpenseClaims", False)),
                bronze_id,
                sync_run_id,
                connection_id,
                tenant_id,
                account_id,
                account_code,
                account_name,
                _account_label(account_code, account_name),
                _to_text(account.get("Type")),
                _to_text(account.get("Class")),
                _to_text(account.get("Status")),
                _to_text(account.get("Description")),
                _to_text(account.get("TaxType")),
                bool(account.get("EnablePaymentsToAccount", False)),
                bool(account.get("ShowInExpenseClaims", False)),
            ),
        )
        row = cursor.fetchone()
        return str(row[0])

    def _upsert_silver_invoice(
        self,
        cursor,
        sync_run_id: str,
        bronze_id: int,
        connection_id: int,
        tenant_id: str,
        invoice: dict[str, Any],
    ) -> str:
        invoice_id = _to_text(invoice.get("InvoiceID"))
        contact_id, contact_name = _invoice_contact(invoice)
        cursor.execute(
            """
            MERGE silver.xero_invoices AS target
            USING (SELECT ? AS xero_tenant_id, ? AS source_id) AS source
                ON target.xero_tenant_id = source.xero_tenant_id
               AND target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET
                bronze_id = ?,
                sync_run_id = ?,
                xero_connection_id = ?,
                invoice_number = ?,
                invoice_type = ?,
                invoice_status = ?,
                contact_id = ?,
                contact_name = ?,
                reference = ?,
                url = ?,
                currency_code = ?,
                currency_rate = ?,
                line_amount_types = ?,
                invoice_date = ?,
                due_date = ?,
                planned_payment_date = ?,
                expected_payment_date = ?,
                fully_paid_on_date = ?,
                updated_date_utc = ?,
                sub_total = ?,
                total_tax = ?,
                total = ?,
                amount_due = ?,
                amount_paid = ?,
                amount_credited = ?,
                has_attachments = ?,
                is_discounted = ?,
                has_errors = ?,
                last_synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                bronze_id,
                sync_run_id,
                xero_connection_id,
                xero_tenant_id,
                source_id,
                invoice_number,
                invoice_type,
                invoice_status,
                contact_id,
                contact_name,
                reference,
                url,
                currency_code,
                currency_rate,
                line_amount_types,
                invoice_date,
                due_date,
                planned_payment_date,
                expected_payment_date,
                fully_paid_on_date,
                updated_date_utc,
                sub_total,
                total_tax,
                total,
                amount_due,
                amount_paid,
                amount_credited,
                has_attachments,
                is_discounted,
                has_errors
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            OUTPUT $action;
            """,
            (
                tenant_id,
                invoice_id,
                bronze_id,
                sync_run_id,
                connection_id,
                _to_text(invoice.get("InvoiceNumber")),
                _to_text(invoice.get("Type")),
                _to_text(invoice.get("Status")),
                contact_id,
                contact_name,
                _to_text(invoice.get("Reference")),
                _to_text(invoice.get("Url")),
                _to_text(invoice.get("CurrencyCode")),
                _to_decimal(invoice.get("CurrencyRate"), scale=6),
                _to_text(invoice.get("LineAmountTypes")),
                _parse_xero_datetime(invoice.get("Date")),
                _parse_xero_datetime(invoice.get("DueDate")),
                _parse_xero_datetime(invoice.get("PlannedPaymentDate")),
                _parse_xero_datetime(invoice.get("ExpectedPaymentDate")),
                _parse_xero_datetime(invoice.get("FullyPaidOnDate")),
                _parse_xero_datetime(invoice.get("UpdatedDateUTC")),
                _to_decimal(invoice.get("SubTotal")),
                _to_decimal(invoice.get("TotalTax")),
                _to_decimal(invoice.get("Total")),
                _to_decimal(invoice.get("AmountDue")),
                _to_decimal(invoice.get("AmountPaid")),
                _to_decimal(invoice.get("AmountCredited")),
                bool(invoice.get("HasAttachments", False)),
                bool(invoice.get("IsDiscounted", False)),
                bool(invoice.get("HasErrors", False)),
                bronze_id,
                sync_run_id,
                connection_id,
                tenant_id,
                invoice_id,
                _to_text(invoice.get("InvoiceNumber")),
                _to_text(invoice.get("Type")),
                _to_text(invoice.get("Status")),
                contact_id,
                contact_name,
                _to_text(invoice.get("Reference")),
                _to_text(invoice.get("Url")),
                _to_text(invoice.get("CurrencyCode")),
                _to_decimal(invoice.get("CurrencyRate"), scale=6),
                _to_text(invoice.get("LineAmountTypes")),
                _parse_xero_datetime(invoice.get("Date")),
                _parse_xero_datetime(invoice.get("DueDate")),
                _parse_xero_datetime(invoice.get("PlannedPaymentDate")),
                _parse_xero_datetime(invoice.get("ExpectedPaymentDate")),
                _parse_xero_datetime(invoice.get("FullyPaidOnDate")),
                _parse_xero_datetime(invoice.get("UpdatedDateUTC")),
                _to_decimal(invoice.get("SubTotal")),
                _to_decimal(invoice.get("TotalTax")),
                _to_decimal(invoice.get("Total")),
                _to_decimal(invoice.get("AmountDue")),
                _to_decimal(invoice.get("AmountPaid")),
                _to_decimal(invoice.get("AmountCredited")),
                bool(invoice.get("HasAttachments", False)),
                bool(invoice.get("IsDiscounted", False)),
                bool(invoice.get("HasErrors", False)),
            ),
        )
        row = cursor.fetchone()
        return str(row[0])

    def _replace_line_items(
        self,
        cursor,
        tenant_id: str,
        invoice_id: str,
        line_items: list[dict[str, Any]],
    ) -> int:
        cursor.execute(
            """
            DELETE FROM silver.xero_invoice_line_items
            WHERE xero_tenant_id = ? AND invoice_source_id = ?
            """,
            (tenant_id, invoice_id),
        )

        rows = []
        for index, item in enumerate(line_items):
            if not isinstance(item, dict):
                continue
            rows.append(
                (
                    tenant_id,
                    invoice_id,
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

        if rows:
            cursor.executemany(
                """
                INSERT INTO silver.xero_invoice_line_items (
                    xero_tenant_id,
                    invoice_source_id,
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

    def _upsert_invoice_pdf(
        self,
        cursor,
        sync_run_id: str,
        connection_id: int,
        tenant_id: str,
        invoice_id: str,
        blob_path: str,
        content_type: str,
        file_name: Optional[str],
    ) -> None:
        cached_at = datetime.now(timezone.utc)
        cursor.execute(
            """
            MERGE bronze.xero_invoice_pdfs AS target
            USING (SELECT ? AS xero_tenant_id, ? AS invoice_source_id) AS source
                ON target.xero_tenant_id = source.xero_tenant_id
               AND target.invoice_source_id = source.invoice_source_id
            WHEN MATCHED THEN UPDATE SET
                sync_run_id = ?,
                xero_connection_id = ?,
                content_type = ?,
                file_name = ?,
                blob_path = ?,
                synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                sync_run_id,
                xero_connection_id,
                xero_tenant_id,
                invoice_source_id,
                content_type,
                file_name,
                blob_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (
                tenant_id,
                invoice_id,
                sync_run_id,
                connection_id,
                content_type,
                file_name,
                blob_path,
                sync_run_id,
                connection_id,
                tenant_id,
                invoice_id,
                content_type,
                file_name,
                blob_path,
            ),
        )

        cursor.execute(
            """
            UPDATE silver.xero_invoices
            SET pdf_cached_at = ?,
                pdf_content_type = ?,
                pdf_file_name = ?,
                pdf_blob_path = ?,
                last_synced_at = GETUTCDATE()
            WHERE xero_tenant_id = ? AND source_id = ?
            """,
            (
                cached_at,
                content_type,
                file_name,
                blob_path,
                tenant_id,
                invoice_id,
            ),
        )
