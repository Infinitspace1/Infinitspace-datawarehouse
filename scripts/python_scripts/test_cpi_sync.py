"""
scripts/python_scripts/test_cpi_sync.py

Local validation for functions/cpi_sync.py.

    .\\venv\\Scripts\\python.exe scripts\\python_scripts\\test_cpi_sync.py
        Fetches from the three live statistics APIs and prints what WOULD be
        written. No database access at all.

    .\\venv\\Scripts\\python.exe scripts\\python_scripts\\test_cpi_sync.py --write
        Runs the real run_cpi_sync() (bronze + silver) and then verifies what
        landed in silver.cpi_series.

The schema must be applied first:
    .\\venv\\Scripts\\python.exe scripts\\python_scripts\\apply_schema_script.py scripts/sql_scripts/cpi_series_schema.sql
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:  # noqa: BLE001 - older interpreters / redirected stdout
    pass

from shared.cpi.client import fetch_series  # noqa: E402
from shared.cpi.transformers.series import transform_observation  # noqa: E402


def _dry_run(months: int) -> None:
    rows = fetch_series(months=months)
    print(f"\nFetched {len(rows)} observations over the last {months} months\n")
    header = f"{'source_id':26}{'code':6}{'period':9}{'level':>11}{'rate %':>9}  status"
    print(header)
    print("-" * len(header))
    for r in rows:
        level = "-" if r["index_level"] is None else f"{r['index_level']:.2f}"
        rate = "-" if r["annual_rate_pct"] is None else f"{r['annual_rate_pct']:.1f}"
        print(f"{r['source_id']:26}{r['index_code']:6}{r['period']:9}"
              f"{level:>11}{rate:>9}  {r['status']}")

    print("\nTransformed sample (what a silver row looks like):")
    for key, value in transform_observation(rows[-1], bronze_id=0, sync_run_id="dry-run").items():
        print(f"  {key:18} {value!r}")

    # The guard that matters most - see _assert_every_provider_reported.
    providers = sorted({r["provider"] for r in rows})
    print(f"\nProviders reporting: {', '.join(providers)}")
    if len(providers) < 3:
        print("  !! a provider returned nothing - a retired endpoint answers 200 "
              "with an empty value set, so check the series ids")


def _verify() -> None:
    from shared.azure_clients.sql_client import get_sql_client

    sql = get_sql_client()
    rows = sql.execute_query("""
        SELECT geo, index_code, COUNT(*) AS periods,
               MIN(period) AS first_period, MAX(period) AS last_period,
               SUM(CASE WHEN status = 'provisional' THEN 1 ELSE 0 END) AS provisional
        FROM silver.cpi_series
        GROUP BY geo, index_code
        ORDER BY geo
    """)
    print("\nsilver.cpi_series:")
    for r in rows:
        print(f"  {r['geo']:4} {r['index_code']:5} {r['periods']:>3} periods "
              f"{r['first_period']}..{r['last_period']}  provisional={r['provisional']}")

    latest = sql.execute_query("""
        SELECT geo, index_code, period, index_level, annual_rate_pct, status
        FROM silver.cpi_series s
        WHERE period = (SELECT MAX(period) FROM silver.cpi_series WHERE geo = s.geo)
        ORDER BY geo
    """)
    print("\nLatest published month per country:")
    for r in latest:
        print(f"  {r['geo']:4} {r['index_code']:5} {r['period']}  "
              f"level={r['index_level']}  rate={r['annual_rate_pct']}%  {r['status']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the CPI sync locally.")
    parser.add_argument("--write", action="store_true",
                        help="run the real sync (bronze + silver) instead of a dry run")
    parser.add_argument("--months", type=int, default=6,
                        help="rolling window for the dry run (default 6)")
    args = parser.parse_args()

    if not args.write:
        _dry_run(args.months)
        print("\nDry run only - nothing was written. Re-run with --write to sync.")
        return

    from functions.cpi_sync import run_cpi_sync

    summary = asyncio.run(run_cpi_sync())
    print(f"\nSync complete: {summary}")
    _verify()


if __name__ == "__main__":
    main()
