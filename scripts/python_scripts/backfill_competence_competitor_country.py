"""
scripts/python_scripts/backfill_competence_competitor_country.py

One-off backfill of silver.competence_competitors.country (+ country_code) for
rows that predate the country cleanup step.

The nightly competence_sync now derives country (name + code) from each
competitor's per-country parent list, but only for rows it re-processes
(watermark-driven) — so existing rows stay empty until they next change. This
script fills the whole current table in one pass.

It reuses the SAME pure resolver the sync uses
(shared.firebase.transformers.competence.resolve_competitor_country), so
backfilled values match exactly what the sync would write. The competitor's own
observed code already lives in silver.country_code, so it is passed back in as
the "own" code (own > list code > list-id prefix).

Run order:
  1. scripts/sql_scripts/competence_competitor_country_migration.sql   (adds the column)
  2. this script

Usage:
  # See how many rows would change (no writes)
  python scripts/python_scripts/backfill_competence_competitor_country.py --dry-run

  # Backfill rows missing country or country_code (default)
  python scripts/python_scripts/backfill_competence_competitor_country.py

  # Recompute every row (e.g. after extending the country map)
  python scripts/python_scripts/backfill_competence_competitor_country.py --all
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

# Sample output below may include accented country/city names; force UTF-8 so
# Windows consoles defaulting to cp1252 don't raise UnicodeEncodeError.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from shared.azure_clients.sql_client import get_sql_client
from shared.firebase.transformers.competence import resolve_competitor_country

BATCH_SIZE = 500

_UPDATE_SQL = """
    UPDATE silver.competence_competitors
    SET country = ?, country_code = ?, last_synced_at = GETUTCDATE()
    WHERE source_id = ?
"""


def _load_list_country_map(sql) -> dict:
    rows = sql.execute_query(
        "SELECT source_id, country, country_code FROM silver.competence_lists"
    )
    return {r["source_id"]: (r["country"], r["country_code"]) for r in rows}


def _load_competitors(sql, recompute_all: bool, limit) -> list[dict]:
    top = f"TOP {limit}" if limit else ""
    where = "" if recompute_all else "WHERE (country IS NULL OR country_code IS NULL)"
    return sql.execute_query(
        f"""
        SELECT {top} source_id, list_source_id, country, country_code
        FROM silver.competence_competitors
        {where}
        ORDER BY source_id
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only evaluate the first N rows (testing).")
    parser.add_argument("--all", action="store_true",
                        help="Recompute every row (default: only rows missing country or country_code).")
    args = parser.parse_args()

    sql = get_sql_client()

    list_country = _load_list_country_map(sql)
    print(f"Loaded {len(list_country)} parent lists for country lookup.")

    rows = _load_competitors(sql, args.all, args.limit)
    print(f"Competitors to evaluate: {len(rows)}")

    updates: list[tuple] = []
    unresolved = 0
    for r in rows:
        list_name, list_code = list_country.get(r["list_source_id"], (None, None))
        name, code = resolve_competitor_country(
            r["country_code"], r["list_source_id"], list_name, list_code
        )
        if name is None and code is None:
            unresolved += 1
            continue
        if name == r["country"] and code == r["country_code"]:
            continue  # already correct
        updates.append((name, code, r["source_id"]))

    print(f"Rows needing update: {len(updates)}   (unresolved: {unresolved})")

    if args.dry_run or not updates:
        for name, code, sid in updates[:15]:
            print(f"  {sid:<50} -> country={name!r:<18} code={code!r}")
        if len(updates) > 15:
            print(f"  ... and {len(updates) - 15} more")
        if unresolved:
            print(f"  ({unresolved} rows could not be resolved — parent list has no country "
                  f"and the list id carries no ISO2 prefix)")
        return

    written = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        sql.execute_many(_UPDATE_SQL, batch)
        written += len(batch)
        print(f"  updated {written}/{len(updates)}")

    print(f"\nDone: {written} rows updated, {unresolved} unresolved.")


if __name__ == "__main__":
    main()
