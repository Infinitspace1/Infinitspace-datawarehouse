"""Inspect gold.location_scraper_map_markers structure + the refresh sproc."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from shared.azure_clients.sql_client import get_sql_client

sql = get_sql_client()

print("=== gold.location_scraper_map_markers COLUMNS ===")
cols = sql.execute_query("""
SELECT c.name, t.name AS type_name, c.max_length, c.is_nullable
FROM sys.columns c
JOIN sys.types t ON t.user_type_id = c.user_type_id
WHERE c.object_id = OBJECT_ID(N'gold.location_scraper_map_markers')
ORDER BY c.column_id
""", ())
for c in cols:
    print(f"  {c['name']:35} {c['type_name']:15} max_len={c['max_length']:5} null={c['is_nullable']}")
print(f"  total: {len(cols)} columns")

print()
print("=== gold.sp_refresh_location_scraper_map_markers DEFINITION ===")
src = sql.execute_query("""
SELECT OBJECT_DEFINITION(OBJECT_ID(N'gold.sp_refresh_location_scraper_map_markers')) AS body
""", ())
if src and src[0]["body"]:
    print(src[0]["body"])
else:
    print("(sproc not found or definition is hidden)")

print()
print("=== row count ===")
n = sql.execute_query("SELECT COUNT(*) AS n FROM gold.location_scraper_map_markers", ())
print(f"  rows: {n[0]['n']}")
