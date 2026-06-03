"""Verify the Location Scraper surface-unit migration applied cleanly.

Checks, per layer:
  1. the new columns exist (surface_m2 / surface_display / surface_unit);
  2. the rename happened (bronze: no leftover available_surface_m2);
  3. the history backfill ran (no NULL surface_unit; loopnet -> sqft, else m²);
  4. gold keeps only buildings with a contact email.

Run:
    python scripts/python_scripts/verify_surface_unit_migration.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv

load_dotenv(ROOT / ".env")
from shared.azure_clients.sql_client import get_sql_client

sql = get_sql_client()
ok = True


def col_exists(table: str, col: str) -> bool:
    rows = sql.execute_query(
        "SELECT COL_LENGTH(?, ?) AS l", (table, col)
    )
    return rows and rows[0]["l"] is not None


def check(label: str, condition: bool, detail: str = "") -> None:
    global ok
    mark = "OK  " if condition else "FAIL"
    if not condition:
        ok = False
    print(f"  [{mark}] {label}{(' — ' + detail) if detail else ''}")


# --- 1. columns exist ------------------------------------------------------
print("=== columns present ===")
check("bronze.surface_m2", col_exists("bronze.n8n_location_scraper_listings", "surface_m2"))
check("bronze.surface_display", col_exists("bronze.n8n_location_scraper_listings", "surface_display"))
check("bronze.surface_unit", col_exists("bronze.n8n_location_scraper_listings", "surface_unit"))
check(
    "bronze: old available_surface_m2 gone",
    not col_exists("bronze.n8n_location_scraper_listings", "available_surface_m2"),
)
check("silver.surface_display", col_exists("silver.location_scraper_globe_v2", "surface_display"))
check("silver.surface_unit", col_exists("silver.location_scraper_globe_v2", "surface_unit"))
check("gold.total_surface_display", col_exists("gold.location_scraper_map_markers", "total_surface_display"))
check("gold.surface_unit", col_exists("gold.location_scraper_map_markers", "surface_unit"))

# --- 2. backfill: no NULL units -------------------------------------------
print("\n=== backfill completeness (expect 0 NULL units) ===")
for table in ("bronze.n8n_location_scraper_listings", "silver.location_scraper_globe_v2"):
    n = sql.execute_query(
        f"SELECT COUNT(*) AS n FROM {table} WHERE surface_unit IS NULL", ()
    )[0]["n"]
    check(f"{table}: NULL surface_unit = {n}", n == 0, "" if n == 0 else "run the backfill")

# --- 3. unit distribution by source ---------------------------------------
print("\n=== silver: surface_unit by source ===")
rows = sql.execute_query(
    """
    SELECT source, surface_unit, COUNT(*) AS n
    FROM silver.location_scraper_globe_v2
    GROUP BY source, surface_unit
    ORDER BY source, surface_unit
    """,
    (),
)
for r in rows:
    print(f"    {r['source']:18} {str(r['surface_unit']):6} {r['n']}")
# loopnet must be sqft, every other source must be m2
bad = [r for r in rows if (r["source"] == "loopnet") != (r["surface_unit"] == "sqft")]
check("loopnet=sqft and others=m2", not bad, "" if not bad else f"{len(bad)} mismatched group(s)")

# --- 4. loopnet sample: display ≈ m² / 0.092903 ---------------------------
print("\n=== loopnet sample (surface_display should be sqft ≈ m²/0.092903) ===")
sample = sql.execute_query(
    """
    SELECT TOP 5 external_id, surface_m2, surface_display, surface_unit
    FROM silver.location_scraper_globe_v2
    WHERE source = 'loopnet' AND surface_m2 IS NOT NULL
    ORDER BY inserted_at DESC
    """,
    (),
)
for r in sample:
    expected_sqft = round(float(r["surface_m2"]) / 0.092903)
    got = round(float(r["surface_display"])) if r["surface_display"] is not None else None
    flag = "ok" if got is not None and abs(got - expected_sqft) <= 2 else "??"
    print(
        f"    {str(r['external_id'])[:16]:16} m2={r['surface_m2']:>9} "
        f"display={r['surface_display']:>10} unit={r['surface_unit']:5} "
        f"(expected~{expected_sqft}) {flag}"
    )

# --- 5. gold: email-only filter -------------------------------------------
print("\n=== gold map markers: email-only filter ===")
total = sql.execute_query("SELECT COUNT(*) AS n FROM gold.location_scraper_map_markers", ())[0]["n"]
no_email = sql.execute_query(
    "SELECT COUNT(*) AS n FROM gold.location_scraper_map_markers WHERE lusha_email_1 IS NULL", ()
)[0]["n"]
print(f"    total markers: {total}   without email: {no_email}")
check("every gold marker has an email", no_email == 0,
      "" if no_email == 0 else "re-run EXEC gold.sp_refresh_location_scraper_map_markers")

print("\n" + ("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED — see above"))
sys.exit(0 if ok else 1)
