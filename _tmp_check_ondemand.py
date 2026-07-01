"""Throwaway diagnostic for the on-demand LoopNet runs."""
import json
from shared.azure_clients.sql_client import Database
from shared.location_scraper.adapters.loopnet import (
    available_surface_sqft_from_payload,
)

db = Database()

print("=== recent runs for EXISTING loopnet cities (last 20) ===")
for r in db.fetch_all("""
    SELECT TOP 20 city, status, buildings_found, run_id, created_at
    FROM bronze.n8n_location_scraper_logs
    WHERE city IN ('london','new york','san francisco','palo alto',
                   'los angeles','austin','seattle')
    ORDER BY created_at DESC
"""):
    d = dict(r)
    print(f"{str(d['city']):14s} | {str(d['status']):10s} | found={d['buildings_found']:>4} "
          f"| {d['created_at']} | {d['run_id']}")

print("\n=== Phoenix raw payload surface inspection (5 samples) ===")
rows = db.fetch_all("""
    SELECT TOP 5 payload_json
    FROM bronze.location_scraper_raw
    WHERE run_id = 'ondemand-phoenix-20260626'
""")
for i, r in enumerate(rows):
    d = dict(r)
    try:
        p = json.loads(d["payload_json"])
    except Exception as e:
        print(f"[{i}] payload parse error: {e}")
        continue
    header = p.get("header") if isinstance(p.get("header"), dict) else {}
    spaces = p.get("spaces") or []
    sqft = available_surface_sqft_from_payload(p)
    print(f"[{i}] keys={sorted(p.keys())[:12]}")
    print(f"     header.subtext={header.get('subtext')!r}  #spaces={len(spaces)}  parsed_sqft={sqft}")
    if spaces and isinstance(spaces[0], dict):
        print(f"     space[0].size={spaces[0].get('size')!r}")
