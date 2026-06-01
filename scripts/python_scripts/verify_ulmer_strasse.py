"""Verify the Ulmer Strasse Stuttgart marker has the full price breakdown."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from shared.azure_clients.sql_client import get_sql_client

sql = get_sql_client()
rows = sql.execute_query("""
SELECT TOP 5
    external_id, address, district,
    price_per_m2, additional_costs_per_m2, total_price_per_m2,
    min_surface_m2, max_surface_m2, total_surface_m2,
    divisible_from_m2,
    min_price_monthly, max_price_monthly, price_monthly_is_estimated,
    price_kind, currency, listing_count
FROM gold.location_scraper_map_markers
WHERE source = 'immobilienscout' AND run_city = 'stuttgart'
  AND (address LIKE '%Ulmer%' OR address LIKE '%Wangen%' OR district LIKE '%Wangen%')
ORDER BY total_surface_m2 DESC
""", ())
print(f"Found {len(rows)} matching markers for Ulmer/Wangen in Stuttgart.")
for r in rows:
    print("---")
    for k, v in r.items():
        print(f"  {k:30} = {v}")
