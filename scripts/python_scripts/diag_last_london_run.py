"""Check the latest London scraper runs (status, volumes, timing)."""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.azure_clients.sql_client import get_db

db = get_db()
rows = db.fetch_all(
    """
    SELECT TOP 5 run_id, status, buildings_found, buildings_new,
           created_at, updated_at, LEFT(error_message, 150) AS err
    FROM bronze.n8n_location_scraper_logs
    WHERE city = 'london'
    ORDER BY created_at DESC
    """
)
for r in rows:
    print(r)

raw = db.fetch_all(
    """
    SELECT TOP 3 run_id, COUNT(*) AS raw_items, MAX(inserted_at) AS last_insert
    FROM bronze.location_scraper_raw
    WHERE city = 'london'
    GROUP BY run_id
    ORDER BY MAX(inserted_at) DESC
    """
)
print()
for r in raw:
    print(r)
