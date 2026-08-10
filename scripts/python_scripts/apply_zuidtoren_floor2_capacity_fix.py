"""
Apply the Zuidtoren floor-2 capacity fix (2026-08-10).

Bug: Zuidtoren showed 134.9% occupancy for Aug-2026. Floor 17 was retired in
Nexudus (available_to = 2026-07-31) and its tenants moved onto the new floor-2
desks (2-01..2-13, available_from = 2026-07-31 22:00 UTC = Aug 1 local) — but
the capacity CTEs still carried the temporary "exclude Taurusavenue 3 AND name
LIKE '2-%'" filter from the refurbishment period, so the 166 floor-2
workstations were missing from the denominator while their contracts (102 ws)
counted as occupied.

This script re-deploys the three fixed objects:
  - gold.vw_landlord_contract_book_monthly   (landlord_dashboard_schema.sql)
  - gold.vw_landlord_membership_book_monthly (landlord_dashboard_revenue_schema.sql)
  - gold.sp_refresh_finance_dashboard        (core_finance_dashboard_schema.sql)

then verifies Zuidtoren, rematerializes the t_landlord_* tables (same code the
03:00 UTC cron runs), and rebuilds today's finance-dashboard snapshot.

Idempotent: CREATE OR ALTER + the cron's own TRUNCATE/INSERT refresh.

Run:
    .\\venv\\Scripts\\python.exe scripts\\python_scripts\\apply_zuidtoren_floor2_capacity_fix.py
"""
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client

SQL_DIR = ROOT / "scripts" / "sql_scripts"
TARGETS = [
    (SQL_DIR / "landlord_dashboard_schema.sql",
     "VIEW", "gold.vw_landlord_contract_book_monthly"),
    (SQL_DIR / "landlord_dashboard_revenue_schema.sql",
     "VIEW", "gold.vw_landlord_membership_book_monthly"),
    (SQL_DIR / "core_finance_dashboard_schema.sql",
     "PROCEDURE", "gold.sp_refresh_finance_dashboard"),
]

ZUIDTOREN = 1414964753


def extract_ddl(sql_text: str, kind: str, fq_name: str) -> str:
    """Single GO-terminated batch containing CREATE OR ALTER <kind> <fq_name>,
    without the trailing GO (pyodbc doesn't accept GO)."""
    pattern = re.compile(
        r"(CREATE\s+OR\s+ALTER\s+" + kind + r"\s+" + re.escape(fq_name) + r"\b.*?)(?:^|\n)GO\b",
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )
    m = pattern.search(sql_text)
    if not m:
        raise RuntimeError(f"Could not locate DDL for {fq_name}")
    return m.group(1).strip()


def main() -> None:
    db = get_sql_client()

    print("-- 1. deploy fixed objects " + "-" * 40)
    for path, kind, name in TARGETS:
        ddl = extract_ddl(path.read_text(encoding="utf-8"), kind, name)
        db.execute_non_query(ddl)
        print(f"   applied {name}")

    print("\n-- 2. verify Zuidtoren (contract book) " + "-" * 28)
    rows = db.execute_query("""
        SELECT period, total_workstation_capacity AS cap, occupied_workstations AS occ,
               CAST(occupancy_pct AS FLOAT) AS pct
        FROM gold.vw_landlord_contract_book_monthly
        WHERE location_source_id = ? AND period IN ('2026-06','2026-07','2026-08','2026-09')
        ORDER BY period
    """, (ZUIDTOREN,))
    for r in rows:
        print(f"   {r['period']}  cap={r['cap']:>3}  occ={r['occ']:>3}  pct={r['pct']}")
    aug = next((r for r in rows if r["period"] == "2026-08"), None)
    if aug and float(aug["pct"]) > 100:
        raise RuntimeError("Aug 2026 still over 100% — fix did not take, aborting "
                           "before rematerializing.")

    print("\n-- 3. all locations, current month (sanity) " + "-" * 23)
    for r in db.execute_query("""
        SELECT location_name, total_workstation_capacity AS cap,
               occupied_workstations AS occ, CAST(occupancy_pct AS FLOAT) AS pct
        FROM gold.vw_landlord_contract_book_monthly
        WHERE period = '2026-08' ORDER BY location_name
    """):
        print(f"   {r['location_name']:<45} cap={r['cap']:>4} occ={r['occ']:>4} pct={r['pct']}")

    print("\n-- 4. rematerialize t_landlord_* tables " + "-" * 27)
    from functions.landlord_materialize_dashboard import _REFRESHES, _refresh_one
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for spec in _REFRESHES:
        written, secs = _refresh_one(db, spec, now_iso)
        print(f"   {spec['target']}: {written} rows in {secs:.1f}s")

    print("\n-- 5. rebuild today's finance-dashboard snapshot " + "-" * 18)
    db.execute_non_query("EXEC gold.sp_refresh_finance_dashboard")
    for r in db.execute_query("""
        SELECT location_name, total_workstation_capacity AS cap,
               occupied_workstations AS occ, CAST(occupancy_pct AS FLOAT) AS pct
        FROM gold.finance_dashboard_revenue_occupancy
        WHERE as_of_date_utc = CAST(GETUTCDATE() AS DATE)
          AND location_source_id = ?
    """, (ZUIDTOREN,)):
        print(f"   snapshot: cap={r['cap']} occ={r['occ']} pct={r['pct']}")

    print("\nDone. Dashboard reads the refreshed tables immediately "
          "(in-process caches expire within 60s).")


if __name__ == "__main__":
    main()
