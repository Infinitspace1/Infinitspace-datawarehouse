"""Re-materialize silver.location_scraper_globe_v2 for every existing IS24 run.

The raw Apify payloads are preserved in bronze.location_scraper_raw, so this
script reapplies the latest extraction/cascade logic to historical runs without
re-scraping. Run this once after applying:
  scripts/sql_scripts/location_scraper_globe_v2_price_breakdown.sql

Usage:
  .\\.venv\\Scripts\\python.exe scripts\\python_scripts\\rematerialize_globe_is24.py
  .\\.venv\\Scripts\\python.exe scripts\\python_scripts\\rematerialize_globe_is24.py --dry-run
  .\\.venv\\Scripts\\python.exe scripts\\python_scripts\\rematerialize_globe_is24.py --city stuttgart
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client
from shared.location_scraper.activities.materialize_globe import materialize_globe_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rematerialize")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", help="Restrict to a single run_city (e.g. stuttgart)")
    parser.add_argument("--dry-run", action="store_true", help="List runs without re-materializing")
    args = parser.parse_args()

    sql = get_sql_client()
    where = "WHERE source = 'immobilienscout'"
    params: tuple = ()
    if args.city:
        where += " AND city = ?"
        params = (args.city.lower(),)
    rows = sql.execute_query(
        f"""
        SELECT run_id, city, COUNT(*) AS items, MIN(inserted_at) AS first_at
        FROM bronze.location_scraper_raw
        {where}
        GROUP BY run_id, city
        ORDER BY first_at ASC
        """,
        params,
    )
    log.info("Found %d IS24 run(s).", len(rows))
    if args.dry_run:
        for r in rows:
            log.info("  %s | city=%s | items=%d | first_at=%s", r["run_id"], r["city"], r["items"], r["first_at"])
        return 0

    failures = 0
    for r in rows:
        run_id = r["run_id"]
        try:
            result = materialize_globe_run({"run_id": run_id})
            log.info("ok run=%s city=%s items=%d rows_written=%s", run_id, r["city"], r["items"], result.get("rows_written"))
        except Exception as exc:
            failures += 1
            log.exception("FAILED run=%s city=%s err=%s", run_id, r["city"], exc)
    log.info("Done. failures=%d/%d", failures, len(rows))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
