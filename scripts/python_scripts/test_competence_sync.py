"""
scripts/python_scripts/test_competence_sync.py

Local end-to-end check for the Firebase competence_new sync.

  --dry-run (default): connect to Firestore, read competence_new, print list +
      competitor counts and a transformed sample. No SQL writes — needs only
      FIREBASE_CREDENTIALS.
  --write: full path — Firestore -> bronze -> silver -> reconcile. Needs SQL too
      (and the tables from scripts/sql_scripts/competence_schema.sql).

Usage:
    python scripts/python_scripts/test_competence_sync.py
    python scripts/python_scripts/test_competence_sync.py --write
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

# Box-drawing output below is UTF-8; force it so Windows consoles that default
# to cp1252 don't raise UnicodeEncodeError.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def _section(title: str):
    print(f"\n{'─'*60}\n  {title}\n{'─'*60}")


def read_firestore():
    _section("1. Connect to Firestore + read competence_new")
    from shared.firebase.client import get_firestore_client
    from shared.firebase.competence import read_competence

    db = get_firestore_client()
    records = read_competence(db)
    n_lists = len(records)
    n_comps = sum(len(r["competitors"]) for r in records)
    print(f"\n  Lists: {n_lists}   Competitors: {n_comps}")
    for r in records[:10]:
        data = r["data"]
        print(f"    {r['list_id']:<26} country={(data.get('country_code') or '?'):<4} "
              f"schema_v={data.get('schema_version')}  competitors={len(r['competitors'])}")
    if n_lists > 10:
        print(f"    ... and {n_lists - 10} more lists")
    return records


def dry_run_transform(records):
    _section("2. Transform dry-run (no SQL writes)")
    from shared.firebase.transformers.competence import (
        transform_competence_list,
        transform_competitor,
    )

    sample = next((r for r in records if r["competitors"]), records[0] if records else None)
    if not sample:
        print("\n  No competence_new lists found.")
        return

    lst = transform_competence_list(sample["data"], sample["list_id"], 0, "dry-run")
    print(f"\n  List: {lst['source_id']}  '{lst['competitor_list_name']}'  "
          f"country={lst['country_code']}  status={lst['status']}  count={lst['competitor_count']}")

    if sample["competitors"]:
        sid, comp = sample["competitors"][0]
        c = transform_competitor(comp, sid, sample["list_id"], 0, "dry-run")
        print("\n  Competitor sample:")
        print(f"    source_id = {c['source_id']}")
        print(f"    title     = {c['title']}")
        print(f"    category  = {c['category_name']}")
        print(f"    city      = {c['city']}   country = {c['country_code']}")
        print(f"    website   = {c['website']}")
        print(f"    lat/lng   = {c['latitude']}, {c['longitude']}")
        print(f"    last_seen = {c['last_seen_at']}")


def run_sync(mode: str):
    """Run the real production path so --write is genuinely incremental
    (RunTracker rows advance the watermark exactly like the deployed timer)."""
    import asyncio

    from functions.competence_sync import run_competence_sync

    _section(f"3. Running competence sync (mode={mode})")
    summary = asyncio.run(run_competence_sync(mode=mode))
    print(f"\n  summary: {summary}")
    _verify()


def _verify():
    _section("4. Verification (silver.competence_competitors by country)")
    from shared.azure_clients.sql_client import get_sql_client

    sql = get_sql_client()
    rows = sql.execute_query(
        """
        SELECT country_code,
               COUNT(*) AS total,
               SUM(CASE WHEN is_deleted = 0 THEN 1 ELSE 0 END) AS active,
               COUNT(website) AS with_website
        FROM silver.competence_competitors
        GROUP BY country_code
        ORDER BY active DESC
        """
    )
    print(f"\n  {'Country':<10} {'Total':>7} {'Active':>7} {'w/Website':>10}")
    print(f"  {'-'*38}")
    for r in rows:
        print(f"  {(r['country_code'] or '?'):<10} {r['total']:>7} {r['active']:>7} {r['with_website']:>10}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true",
                        help="Run the sync (bronze + silver). Default mode is incremental.")
    parser.add_argument("--full", action="store_true",
                        help="Full read + soft-delete reconcile (the weekly path).")
    args = parser.parse_args()

    if args.write:
        run_sync("full" if args.full else "incremental")
    else:
        records = read_firestore()
        dry_run_transform(records)
        print("\n  Dry run complete. Add --write (incremental) or --write --full.")


if __name__ == "__main__":
    main()
