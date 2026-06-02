import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dotenv import load_dotenv; load_dotenv()
from shared.azure_clients.sql_client import get_sql_client
sql = get_sql_client()
for r in sql.execute_query("""
    SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='bronze' AND TABLE_NAME='n8n_location_scraper_logs'
    ORDER BY ORDINAL_POSITION
"""):
    print(r['COLUMN_NAME'], '|', r['DATA_TYPE'])
