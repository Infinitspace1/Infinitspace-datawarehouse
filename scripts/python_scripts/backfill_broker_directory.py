"""
One-off backfill of silver.location_scraper_broker_directory from the full
bronze.location_scraper_raw LoopNet archive, then (optionally) re-materialize
the latest globe run per LoopNet city so recovered emails land in globe/gold.

The nightly path self-enriches (every LoopNet globe materialization upserts
the pairs it sees) — this script seeds the directory with history that predates
the feature.

Usage:
  python scripts/python_scripts/backfill_broker_directory.py             # dry-run
  python scripts/python_scripts/backfill_broker_directory.py --write
  python scripts/python_scripts/backfill_broker_directory.py --write --rematerialize
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv()

from shared.azure_clients.sql_client import get_sql_client
from shared.location_scraper.broker_directory import (
    extract_broker_records,
    upsert_broker_records,
)

_PAGE = 5000

_READ_RAW_PAGE = """
SELECT payload_json FROM bronze.location_scraper_raw
WHERE source = 'loopnet'
ORDER BY id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
"""

# Same freshness rule as gold.sp_refresh_location_scraper_map_markers: the
# latest globe run per (source, run_city) is the one the dashboard serves.
_LATEST_LOOPNET_RUNS = """
WITH run_freshness AS (
    SELECT run_city, run_id, MAX(inserted_at) AS latest_inserted_at
    FROM silver.location_scraper_globe_v2
    WHERE source = 'loopnet'
    GROUP BY run_city, run_id
)
SELECT run_city, run_id
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY run_city
               ORDER BY latest_inserted_at DESC, run_id DESC
           ) AS rn
    FROM run_freshness
) ranked
WHERE rn = 1
"""


def collect_records(sql) -> list[dict]:
    """All distinct (name, email) records across the LoopNet raw archive."""
    by_key: dict[tuple[str, str], dict] = {}
    scanned = 0
    offset = 0
    while True:
        rows = sql.execute_query(_READ_RAW_PAGE, (offset, _PAGE))
        if not rows:
            break
        for row in rows:
            scanned += 1
            try:
                payload = json.loads(row["payload_json"])
            except (ValueError, TypeError):
                continue
            for rec in extract_broker_records(payload):
                key = (rec["name_normalized"], rec["email"])
                existing = by_key.get(key)
                if existing is None:
                    by_key[key] = rec
                else:
                    # Keep the most complete company/phone we have seen.
                    existing["company"] = existing["company"] or rec["company"]
                    existing["phone"] = existing["phone"] or rec["phone"]
        offset += _PAGE
    print(f"raw rows scanned: {scanned}")
    return list(by_key.values())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="upsert into the directory (default: dry-run)")
    parser.add_argument(
        "--rematerialize",
        action="store_true",
        help="after the backfill, re-materialize the latest globe run per LoopNet city + refresh gold",
    )
    args = parser.parse_args()

    sql = get_sql_client()
    records = collect_records(sql)
    names = {r["name_normalized"] for r in records}
    print(f"distinct (name, email) pairs: {len(records)}  distinct names: {len(names)}")

    if not args.write:
        print("\nDRY-RUN — nothing written. Re-run with --write to populate the directory.")
        return

    upserted = upsert_broker_records(sql, records)
    print(f"upserted: {upserted} records")

    if args.rematerialize:
        from shared.location_scraper.activities.materialize_globe import (
            materialize_globe_run,
            refresh_globe_quality,
            refresh_gold_map_markers,
        )

        runs = sql.execute_query(_LATEST_LOOPNET_RUNS)
        print(f"\nre-materializing {len(runs)} latest LoopNet runs...")
        for row in runs:
            result = materialize_globe_run({"run_id": row["run_id"]})
            refresh_globe_quality({"run_id": row["run_id"]})
            print(f"  {row['run_city']}: {row['run_id']} -> {result}")
        gold = refresh_gold_map_markers({})
        print(f"gold refresh: {gold}")


if __name__ == "__main__":
    main()
