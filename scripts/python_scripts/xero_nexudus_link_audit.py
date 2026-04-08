"""
Audit how well Xero overdue invoices link to Nexudus invoice/customer data.

Run this after the new Nexudus billing schema is applied and the bronze/silver
sync has populated `silver.nexudus_coworker_invoices` and `silver.nexudus_coworkers`.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.azure_clients.sql_client import get_sql_client
from shared.integrations.xero_nexudus_overdue import XeroNexudusOverdueLinker


def _load_xero_overdue(sql, limit: int | None) -> list[dict]:
    top = f"TOP {limit}" if limit else ""
    return sql.execute_query(
        f"""
        SELECT {top}
            xi.xero_connection_id,
            xi.xero_tenant_id,
            xi.invoice_number,
            xi.reference,
            xi.contact_name,
            xi.due_date,
            xi.amount_due,
            xt.location_source_id
        FROM silver.xero_invoices xi
        LEFT JOIN silver.xero_tenants xt
          ON xt.xero_connection_id = xi.xero_connection_id
         AND xt.xero_tenant_id = xi.xero_tenant_id
        WHERE xi.invoice_status = 'AUTHORISED'
          AND xi.amount_due > 0
          AND xi.due_date < CAST(GETUTCDATE() AS DATE)
        ORDER BY xi.due_date ASC
        """
    )


def _load_nexudus_invoices(sql) -> list[dict]:
    return sql.execute_query(
        """
        SELECT
            source_id,
            coworker_id,
            coworker_name,
            coworker_billing_email,
            location_source_id,
            invoice_number,
            payment_reference,
            bill_to_name
        FROM silver.nexudus_coworker_invoices
        """
    )


def _load_nexudus_coworkers(sql) -> list[dict]:
    return sql.execute_query(
        """
        SELECT
            source_id,
            full_name,
            email,
            billing_email,
            billing_name
        FROM silver.nexudus_coworkers
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Limit overdue Xero invoices")
    parser.add_argument("--show-unmatched", action="store_true")
    args = parser.parse_args()

    sql = get_sql_client()
    xero_overdue = _load_xero_overdue(sql, args.limit)
    nexudus_invoices = _load_nexudus_invoices(sql)
    nexudus_coworkers = _load_nexudus_coworkers(sql)

    linker = XeroNexudusOverdueLinker()
    linked = linker.link_invoices(xero_overdue, nexudus_invoices, nexudus_coworkers)

    matched = [row for row in linked if row["link_matched"]]
    unmatched = [row for row in linked if not row["link_matched"]]

    print(json.dumps({
        "xero_overdue_count": len(xero_overdue),
        "matched_count": len(matched),
        "unmatched_count": len(unmatched),
        "match_rate": round((len(matched) / len(xero_overdue)) * 100, 2) if xero_overdue else 0.0,
    }, default=str, indent=2))

    print("\nMatched samples:")
    for row in matched[:10]:
        print(json.dumps(row, default=str))

    if args.show_unmatched:
        print("\nUnmatched samples:")
        for row in unmatched[:20]:
            print(json.dumps(row, default=str))


if __name__ == "__main__":
    main()
