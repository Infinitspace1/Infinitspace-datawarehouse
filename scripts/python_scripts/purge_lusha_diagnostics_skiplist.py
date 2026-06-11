"""One-off: purge the Lusha diagnostics skip-list (has_contact=1 rows).

Why (2026-06-11): the persist bug dropped Lusha contacts for agencies whose
buildings already existed in SQL, yet those agencies keep has_contact=1 rows in
bronze.location_scraper_lusha_diagnostics, and filter_new_agencies skips any
such agency forever. With the persist fix deployed (contacts now linked on the
update path too), deleting those rows lets the next weekly run re-enrich the
affected agencies once. Agencies whose buildings are already fully covered are
still skipped by the filter's building-coverage check, so the re-enrichment is
self-limiting (~214 agencies as of 2026-06-11, see
quantify_lusha_diagnostics_purge.py).

Usage:
  python purge_lusha_diagnostics_skiplist.py --dry-run            # counts only
  python purge_lusha_diagnostics_skiplist.py                      # purge all
  python purge_lusha_diagnostics_skiplist.py --city munich --city berlin
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.azure_clients.sql_client import get_sql_client


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="show what would be deleted")
    parser.add_argument(
        "--city", action="append", default=None,
        help="restrict to one or more cities (repeatable); default: all cities",
    )
    args = parser.parse_args()

    where = "has_contact = 1"
    params: tuple = ()
    if args.city:
        placeholders = ", ".join("?" for _ in args.city)
        where += f" AND city IN ({placeholders})"
        params = tuple(c.lower().strip() for c in args.city)

    sql = get_sql_client()

    rows = sql.execute_query(
        f"""
        SELECT city, COUNT(*) AS rows_to_delete,
               COUNT(DISTINCT LOWER(LTRIM(RTRIM(agency_name)))) AS agencies
        FROM bronze.location_scraper_lusha_diagnostics
        WHERE {where}
        GROUP BY city ORDER BY city
        """,
        params,
    )
    total_rows = sum(r["rows_to_delete"] for r in rows)
    print("Skip-list rows matching the purge scope:")
    for r in rows:
        print(f"  {r['city']:<12} rows={r['rows_to_delete']:<5} agencies={r['agencies']}")
    print(f"  TOTAL rows: {total_rows}")

    if args.dry_run:
        print("\nDry run — nothing deleted.")
        return
    if total_rows == 0:
        print("\nNothing to delete.")
        return

    with sql.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"DELETE FROM bronze.location_scraper_lusha_diagnostics WHERE {where}", params
        )
        deleted = cursor.rowcount
        conn.commit()
    print(f"\nDeleted {deleted} rows. The next weekly run will re-enrich the affected agencies.")


if __name__ == "__main__":
    main()
