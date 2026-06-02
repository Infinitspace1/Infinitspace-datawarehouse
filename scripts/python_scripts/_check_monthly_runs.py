import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dotenv import load_dotenv; load_dotenv()
from shared.azure_clients.sql_client import get_sql_client
sql = get_sql_client()

print("=== monthly-* runs for 2026-06 ===")
rows = sql.execute_query("""
    SELECT run_id, city, source, status, run_date, created_at, updated_at,
           buildings_found, buildings_new, buildings_updated
    FROM bronze.n8n_location_scraper_logs
    WHERE run_id LIKE 'monthly-%-2026-06'
    ORDER BY created_at DESC
""")
if not rows:
    print("(no rows) -> monthly timer did NOT create any run rows this month")
for r in rows:
    print(r)

print("\n=== most recent 12 runs overall ===")
for r in sql.execute_query("""
    SELECT TOP 12 run_id, city, status, run_date, created_at, buildings_found
    FROM bronze.n8n_location_scraper_logs
    ORDER BY created_at DESC
"""):
    print(r)
