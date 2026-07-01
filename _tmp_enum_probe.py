"""Compare raw payload shape: working NY weekly vs failing phoenix on-demand."""
import json
from collections import Counter
from shared.azure_clients.sql_client import Database

db = Database()

def shape_report(run_id, label):
    rows = db.fetch_all(f"""
        SELECT TOP 8 payload_json
        FROM bronze.location_scraper_raw
        WHERE run_id = '{run_id}'
    """)
    print(f"\n=== {label}  (run_id={run_id})  sampled={len(rows)} ===")
    has_header = has_spaces = 0
    for r in rows:
        p = json.loads(dict(r)["payload_json"])
        if isinstance(p.get("header"), dict): has_header += 1
        if p.get("spaces"): has_spaces += 1
    if rows:
        p0 = json.loads(dict(rows[0])["payload_json"])
        print("sample keys:", sorted(p0.keys()))
    print(f"has header.dict: {has_header}/{len(rows)}   has spaces: {has_spaces}/{len(rows)}")

# find the most recent NY weekly run_id that has raw rows
ny = db.fetch_all("""
    SELECT TOP 1 run_id FROM bronze.location_scraper_raw
    WHERE run_id LIKE 'weekly-new-york-%' ORDER BY run_id DESC
""")
if ny:
    shape_report(dict(ny[0])["run_id"], "NEW YORK weekly (working)")
else:
    print("no NY weekly raw rows found")

shape_report("ondemand-phoenix-20260626", "PHOENIX on-demand (0 buildings)")
