"""Show which IS24 runs have been re-materialized so far."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from shared.azure_clients.sql_client import get_sql_client

sql = get_sql_client()
rows = sql.execute_query("""
SELECT
    b.run_id,
    b.city,
    COUNT(*) AS bronze_items,
    ISNULL((SELECT COUNT(*) FROM silver.location_scraper_globe_v2 s WHERE s.run_id = b.run_id), 0) AS silver_items,
    ISNULL((SELECT MAX(s.refreshed_at) FROM silver.location_scraper_globe_v2 s WHERE s.run_id = b.run_id), CAST('1900-01-01' AS DATETIME2)) AS last_refresh,
    ISNULL((SELECT COUNT(*) FROM silver.location_scraper_globe_v2 s WHERE s.run_id = b.run_id AND s.additional_costs_per_m2 IS NOT NULL), 0) AS new_cols_filled
FROM bronze.location_scraper_raw b
WHERE b.source = 'immobilienscout'
GROUP BY b.run_id, b.city
ORDER BY last_refresh DESC, b.run_id
""", ())
print(f"{'run_id':45} {'city':12} {'bronze':>7} {'silver':>7} {'newcol':>7} {'last_refresh'}")
for r in rows:
    print(f"{r['run_id']:45} {r['city']:12} {r['bronze_items']:>7} {r['silver_items']:>7} {r['new_cols_filled']:>7} {r['last_refresh']}")
