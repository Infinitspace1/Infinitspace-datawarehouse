"""
Backfill PDFs for ALL Xero invoices that don't have a blob_path yet.

Fetches from the Xero API, uploads to xero-invoice-pdfs blob container,
and saves the path to both bronze.xero_invoice_pdfs and silver.xero_invoices.

The pdf_blob_path IS NULL filter is the natural watermark — already-cached
invoices are never re-fetched. Safe to resume after a failure.

NOTE: 16k+ invoices at ~60 req/min = ~4.5 hours for a full backfill.
      Run with --limit 100 first to verify, then leave overnight.

Usage:
  # Dry-run: see how many invoices need PDFs
  python scripts/python_scripts/backfill_xero_pdfs.py --dry-run

  # Test with a small batch first
  python scripts/python_scripts/backfill_xero_pdfs.py --limit 20

  # Full backfill (leave running overnight)
  python scripts/python_scripts/backfill_xero_pdfs.py
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.xero.client import XeroApiClient
from shared.xero.store import XeroStore


_MAX_RETRIES = 5
_DEFAULT_RETRY_AFTER = 60  # seconds to wait when no Retry-After header


def _fetch_pdf_with_retry(client, invoice_id: str, tenant_id: str) -> tuple:
    for attempt in range(_MAX_RETRIES):
        try:
            return client.get_invoice_pdf(invoice_id=invoice_id, tenant_id=tenant_id)
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                wait = int(exc.response.headers.get("Retry-After", _DEFAULT_RETRY_AFTER))
                print(f"    Rate limited. Waiting {wait}s (attempt {attempt + 1}/{_MAX_RETRIES})...")
                time.sleep(wait + 1)
                if attempt == _MAX_RETRIES - 1:
                    raise
            else:
                raise
    raise RuntimeError("unreachable")  # pragma: no cover


def _load_missing(sql, limit: int | None) -> list[dict]:
    top = f"TOP {limit}" if limit else ""
    return sql.execute_query(
        f"""
        SELECT {top}
            xi.source_id,
            xi.xero_tenant_id,
            xi.xero_connection_id,
            xi.invoice_number,
            xi.invoice_status,
            xi.invoice_date,
            xi.amount_due
        FROM silver.xero_invoices xi
        WHERE xi.pdf_blob_path IS NULL
        ORDER BY xi.invoice_date DESC
        """
    )


def _save(sql, sync_run_id: str, row: dict, blob_path: str, content_type: str, file_name: str | None) -> None:
    sql.execute_non_query(
        """
        MERGE bronze.xero_invoice_pdfs AS target
        USING (SELECT ? AS xero_tenant_id, ? AS invoice_source_id) AS source
            ON target.xero_tenant_id = source.xero_tenant_id
           AND target.invoice_source_id = source.invoice_source_id
        WHEN MATCHED THEN UPDATE SET
            sync_run_id = ?, xero_connection_id = ?,
            content_type = ?, file_name = ?, blob_path = ?,
            synced_at = GETUTCDATE()
        WHEN NOT MATCHED THEN INSERT (
            sync_run_id, xero_connection_id, xero_tenant_id,
            invoice_source_id, content_type, file_name, blob_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (
            row["xero_tenant_id"], row["source_id"],
            sync_run_id, row["xero_connection_id"], content_type, file_name, blob_path,
            sync_run_id, row["xero_connection_id"], row["xero_tenant_id"],
            row["source_id"], content_type, file_name, blob_path,
        ),
    )
    sql.execute_non_query(
        """
        UPDATE silver.xero_invoices
        SET pdf_cached_at = GETUTCDATE(),
            pdf_content_type = ?,
            pdf_file_name = ?,
            pdf_blob_path = ?,
            last_synced_at = GETUTCDATE()
        WHERE xero_tenant_id = ? AND source_id = ?
        """,
        (content_type, file_name, blob_path, row["xero_tenant_id"], row["source_id"]),
    )


_RATE_LIMIT_HEADERS = (
    "X-Rate-Limit-Remaining",
    "X-Rate-Limit-Reset",
    "X-MinuteLimit-Remaining",
    "X-AppMinuteLimit-Remaining",
    "X-DayLimit-Remaining",
    "Retry-After",
)


def _check_limits(sql) -> None:
    store = XeroStore(sql_client=sql)
    connections_rows = sql.execute_query(
        "SELECT id, xero_tenant_id FROM meta.xero_connections WHERE is_connected = 1"
    )
    if not connections_rows:
        print("No connected Xero connections found.")
        return

    for row in connections_rows:
        conn_id = int(row["id"])
        tenant_id = str(row["xero_tenant_id"]) if row["xero_tenant_id"] else None
        client = XeroApiClient(connection_id=conn_id, store=store)
        print(f"\nConnection {conn_id} (tenant {tenant_id}):")
        try:
            # Use a minimal 1-invoice page just to get headers back
            resp = client.request_response(
                method="GET",
                path="/api.xro/2.0/Invoices",
                params={"page": 1, "pageSize": 1},
                tenant_id=tenant_id,
            )
            found = False
            for h in _RATE_LIMIT_HEADERS:
                v = resp.headers.get(h)
                if v:
                    print(f"  {h}: {v}")
                    found = True
            if not found:
                print("  (no rate-limit headers returned)")
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            print(f"  HTTP {status}: {exc}")
            if exc.response is not None:
                for h in _RATE_LIMIT_HEADERS:
                    v = exc.response.headers.get(h)
                    if v:
                        print(f"  {h}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--check-limits", action="store_true", help="Print Xero rate-limit headers and exit")
    args = parser.parse_args()

    sql = get_sql_client()
    store = XeroStore(sql_client=sql)

    if args.check_limits:
        _check_limits(sql)
        return

    invoices = _load_missing(sql, args.limit)
    print(f"Invoices needing PDFs: {len(invoices)}")

    if args.dry_run or not invoices:
        for r in invoices[:10]:
            print(f"  {r['invoice_number']}  status={r['invoice_status']}  date={r['invoice_date']}  amount_due={r['amount_due']}")
        if len(invoices) > 10:
            print(f"  ... and {len(invoices) - 10} more")
        return

    blob = BlobWriter()
    sync_run_id = str(uuid.uuid4())

    # Group by connection_id to reuse client per connection
    connections: dict[int, XeroApiClient] = {}
    ok = errors = 0

    for i, row in enumerate(invoices, 1):
        conn_id = int(row["xero_connection_id"])
        if conn_id not in connections:
            connections[conn_id] = XeroApiClient(connection_id=conn_id, store=store)
        client = connections[conn_id]

        try:
            pdf_bytes, content_type, file_name = _fetch_pdf_with_retry(
                client,
                invoice_id=str(row["source_id"]),
                tenant_id=str(row["xero_tenant_id"]),
            )
            blob_path = blob.write_pdf(
                tenant_id=str(row["xero_tenant_id"]),
                invoice_source_id=str(row["source_id"]),
                pdf_bytes=pdf_bytes,
                content_type=content_type,
            )
            _save(sql, sync_run_id, row, blob_path, content_type, file_name)
            ok += 1
            print(f"  [{i}/{len(invoices)}] {row['invoice_number']} -> {blob_path}")
        except Exception as exc:
            errors += 1
            print(f"  [{i}/{len(invoices)}] {row['invoice_number']} FAILED: {exc}")

        # Xero rate limit: ~60 req/min, so pace at ~50/min to be safe
        time.sleep(1.2)

    print(f"\nDone: {ok} uploaded, {errors} failed")


if __name__ == "__main__":
    main()
