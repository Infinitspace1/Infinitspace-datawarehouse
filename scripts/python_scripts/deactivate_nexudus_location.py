"""Soft-delete one Nexudus location and its location-scoped silver records.

Default target: Kingsbourne House / London - Holborn - 229-231 High Holborn.
Run without --apply for a dry-run count. Run with --apply to update SQL and
refresh downstream AVA/finance materializations.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.azure_clients.sql_client import get_sql_client


DEFAULT_LOCATION_SOURCE_ID = 1414964752


@dataclass(frozen=True)
class TargetTable:
    name: str
    key_column: str
    has_soft_delete: bool = True


TARGET_TABLES = (
    TargetTable("silver.nexudus_locations", "source_id"),
    TargetTable("silver.nexudus_products", "location_source_id"),
    TargetTable("silver.nexudus_contracts", "location_source_id"),
    TargetTable("silver.nexudus_extra_services", "location_source_id"),
    TargetTable("silver.nexudus_resources", "location_source_id"),
    TargetTable("silver.nexudus_coworkers", "location_source_id"),
    TargetTable("silver.nexudus_coworker_invoices", "location_source_id"),
    TargetTable("silver.nexudus_coworker_invoice_lines", "location_source_id"),
    TargetTable("silver.nexudus_tariffs", "location_source_id"),
    TargetTable("silver.nexudus_financial_accounts", "location_source_id"),
    TargetTable("silver.nexudus_calendar_events", "location_source_id"),
    TargetTable("silver.nexudus_event_attendees", "location_source_id"),
    TargetTable("silver.nexudus_event_products", "location_source_id"),
)

MATERIALIZED_TABLES = (
    TargetTable("ava.product_availability", "location_source_id", has_soft_delete=False),
    TargetTable("gold.finance_dashboard_user_access", "location_source_id", has_soft_delete=False),
    TargetTable("gold.finance_dashboard_invoice_worklist", "location_source_id", has_soft_delete=False),
    TargetTable("gold.finance_dashboard_revenue_occupancy", "location_source_id", has_soft_delete=False),
)


def _object_exists(cursor, table_name: str) -> bool:
    return bool(
        cursor.execute(
            "SELECT CASE WHEN OBJECT_ID(?, 'U') IS NULL THEN 0 ELSE 1 END",
            (table_name,),
        ).fetchone()[0]
    )


def _column_exists(cursor, table_name: str, column_name: str) -> bool:
    return bool(
        cursor.execute(
            "SELECT CASE WHEN COL_LENGTH(?, ?) IS NULL THEN 0 ELSE 1 END",
            (table_name, column_name),
        ).fetchone()[0]
    )


def _count_rows(cursor, table: TargetTable, location_source_id: int) -> int | None:
    if not _object_exists(cursor, table.name):
        return None
    if not _column_exists(cursor, table.name, table.key_column):
        return None

    predicate = f"{table.key_column} = ?"
    if table.has_soft_delete and _column_exists(cursor, table.name, "is_deleted"):
        predicate += " AND is_deleted = 0"

    return int(
        cursor.execute(
            f"SELECT COUNT(1) FROM {table.name} WHERE {predicate}",
            (location_source_id,),
        ).fetchone()[0]
    )


def _soft_delete_rows(cursor, table: TargetTable, location_source_id: int) -> int | None:
    if not _object_exists(cursor, table.name):
        return None
    if not _column_exists(cursor, table.name, table.key_column):
        return None
    if not _column_exists(cursor, table.name, "is_deleted"):
        return None

    deleted_at_set = (
        ",\n            deleted_at = COALESCE(deleted_at, GETUTCDATE())"
        if _column_exists(cursor, table.name, "deleted_at")
        else ""
    )
    cursor.execute(
        f"""
        UPDATE {table.name}
        SET is_deleted = 1{deleted_at_set}
        WHERE {table.key_column} = ?
          AND is_deleted = 0
        """,
        (location_source_id,),
    )
    return cursor.rowcount if cursor.rowcount is not None else 0


def _delete_materialized_rows(cursor, table: TargetTable, location_source_id: int) -> int | None:
    if not _object_exists(cursor, table.name):
        return None
    if not _column_exists(cursor, table.name, table.key_column):
        return None

    cursor.execute(
        f"DELETE FROM {table.name} WHERE {table.key_column} = ?",
        (location_source_id,),
    )
    return cursor.rowcount if cursor.rowcount is not None else 0


def _execute_if_exists(cursor, object_name: str, object_type: str, sql: str) -> bool:
    exists = bool(
        cursor.execute(
            "SELECT CASE WHEN OBJECT_ID(?, ?) IS NULL THEN 0 ELSE 1 END",
            (object_name, object_type),
        ).fetchone()[0]
    )
    if exists:
        cursor.execute(sql)
    return exists


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--location-source-id", type=int, default=DEFAULT_LOCATION_SOURCE_ID)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--skip-refresh", action="store_true")
    args = parser.parse_args()

    sql = get_sql_client()
    location_source_id = args.location_source_id

    with sql.get_connection() as conn:
        cursor = conn.cursor()

        location_rows = cursor.execute(
            """
            SELECT source_id, name, address, city, is_deleted
            FROM silver.nexudus_locations
            WHERE source_id = ?
            """,
            (location_source_id,),
        ).fetchall()
        print("Target location:")
        for row in location_rows:
            print(
                f"  source_id={row.source_id} name={row.name!r} "
                f"address={row.address!r} city={row.city!r} is_deleted={row.is_deleted}"
            )
        if not location_rows:
            raise RuntimeError(f"No silver.nexudus_locations row found for {location_source_id}")

        print("\nActive silver rows matching target:")
        for table in TARGET_TABLES:
            count = _count_rows(cursor, table, location_source_id)
            print(f"  {table.name}: {'missing/skipped' if count is None else count}")

        print("\nMaterialized rows matching target:")
        for table in MATERIALIZED_TABLES:
            count = _count_rows(cursor, table, location_source_id)
            print(f"  {table.name}: {'missing/skipped' if count is None else count}")

        if not args.apply:
            print("\nDry-run only. Re-run with --apply to update SQL.")
            return 0

        print("\nApplying soft-delete:")
        for table in TARGET_TABLES:
            count = _soft_delete_rows(cursor, table, location_source_id)
            print(f"  {table.name}: {'missing/skipped' if count is None else count}")

        print("\nClearing materialized rows for immediate removal:")
        for table in MATERIALIZED_TABLES:
            count = _delete_materialized_rows(cursor, table, location_source_id)
            print(f"  {table.name}: {'missing/skipped' if count is None else count}")

        if not args.skip_refresh:
            print("\nRefreshing downstream materializations:")
            ava_refreshed = _execute_if_exists(
                cursor,
                "ava.sp_refresh_product_availability",
                "P",
                "EXEC ava.sp_refresh_product_availability",
            )
            print(f"  ava.sp_refresh_product_availability: {'ran' if ava_refreshed else 'missing/skipped'}")

            finance_refreshed = _execute_if_exists(
                cursor,
                "gold.sp_refresh_finance_dashboard",
                "P",
                "EXEC gold.sp_refresh_finance_dashboard",
            )
            print(f"  gold.sp_refresh_finance_dashboard: {'ran' if finance_refreshed else 'missing/skipped'}")

        print("\nApplied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
