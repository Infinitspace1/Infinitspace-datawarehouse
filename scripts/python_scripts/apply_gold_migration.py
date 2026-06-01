"""Apply scripts/sql_scripts/location_scraper_gold_map_markers_price_breakdown.sql
then refresh the gold map markers via the new sproc."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from shared.azure_clients.sql_client import get_sql_client

sql_path = ROOT / "scripts" / "sql_scripts" / "location_scraper_gold_map_markers_price_breakdown.sql"
print(f"Applying {sql_path.name}...")

content = sql_path.read_text(encoding="utf-8")
# Split on GO (line by itself, case-insensitive)
batches: list[str] = []
buf: list[str] = []
for line in content.splitlines():
    if line.strip().upper() == "GO":
        body = "\n".join(buf).strip()
        if body:
            batches.append(body)
        buf = []
    else:
        buf.append(line)
body = "\n".join(buf).strip()
if body:
    batches.append(body)
print(f"  -> {len(batches)} batch(es)")

sql = get_sql_client()
for i, batch in enumerate(batches, 1):
    head = batch.splitlines()[0][:80]
    print(f"  [{i}] executing: {head}...")
    sql.execute_non_query(batch, ())
print("Migration applied.")

print()
print("Refreshing gold.location_scraper_map_markers ...")
sql.execute_non_query("EXEC gold.sp_refresh_location_scraper_map_markers", ())

rows = sql.execute_query("SELECT COUNT(*) AS n FROM gold.location_scraper_map_markers", ())
print(f"  -> {rows[0]['n']} markers total")

rows = sql.execute_query("""
SELECT
    source,
    COUNT(*) AS markers,
    SUM(CASE WHEN price_per_m2 IS NOT NULL THEN 1 ELSE 0 END) AS with_per_m2,
    SUM(CASE WHEN additional_costs_per_m2 IS NOT NULL THEN 1 ELSE 0 END) AS with_nebenkosten,
    SUM(CASE WHEN divisible_from_m2 IS NOT NULL THEN 1 ELSE 0 END) AS with_divisible,
    SUM(CASE WHEN price_monthly_is_estimated = 1 THEN 1 ELSE 0 END) AS price_estimated
FROM gold.location_scraper_map_markers
GROUP BY source
ORDER BY source
""", ())
print()
print(f"{'source':20} {'markers':>8} {'per_m2':>8} {'nebenk':>8} {'divisib':>8} {'estim.':>8}")
for r in rows:
    print(f"{r['source']:20} {r['markers']:>8} {r['with_per_m2']:>8} {r['with_nebenkosten']:>8} {r['with_divisible']:>8} {r['price_estimated']:>8}")
