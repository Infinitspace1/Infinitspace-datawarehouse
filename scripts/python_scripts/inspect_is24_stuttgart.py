"""Quick one-off: pull the raw payload for the Ulmer Strasse IS24 listing
and print a price-relevant subset so we can see why price isn't extracted."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


PRICE_KEY_HINTS = (
    "price", "rent", "miete", "cost", "neben", "sqm", "sqmonth",
    "calculated", "base", "betrieb", "warm", "kalt", "amount",
)


def find_price_paths(obj, prefix=""):
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            path = f"{prefix}/{k}" if prefix else k
            if any(h in kl for h in PRICE_KEY_HINTS):
                if not isinstance(v, (dict, list)) or (isinstance(v, dict) and len(v) < 6):
                    out.append((path, v))
            out.extend(find_price_paths(v, path))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.extend(find_price_paths(v, f"{prefix}/{i}"))
    return out


def main() -> int:
    sql = get_sql_client()
    print("Querying latest IS24 Stuttgart rows (no full-text scan)...")
    rows = sql.execute_query(
        """
        SELECT TOP 10 run_id, item_index, payload_json
        FROM bronze.location_scraper_raw
        WHERE source = 'immobilienscout' AND city = 'stuttgart'
        ORDER BY inserted_at DESC, item_index ASC
        """,
        (),
    )
    print(f"  -> {len(rows)} rows")
    # Filter in Python for Ulmer
    ulmer = [r for r in rows if "Ulmer Stra" in (r["payload_json"] or "")]
    if ulmer:
        print(f"  -> filtered to {len(ulmer)} Ulmer rows")
        rows = ulmer
    else:
        print("  -> no Ulmer match in last 10 Stuttgart rows; using whatever returned")
        rows = rows[:3]
    if not rows:
        print("Falling back to latest IS24 row anywhere.")
        rows = sql.execute_query(
            """
            SELECT TOP 1 run_id, item_index, payload_json
            FROM bronze.location_scraper_raw
            WHERE source = 'immobilienscout'
            ORDER BY inserted_at DESC
            """,
            (),
        )
    if not rows:
        print("No IS24 rows at all.")
        return 1
    for r in rows:
        print("=" * 80)
        print(f"run_id = {r['run_id']} | item_index = {r['item_index']}")
        payload = json.loads(r["payload_json"])
        print("TOP-LEVEL KEYS:")
        for k in sorted(payload.keys()):
            v = payload[k]
            t = type(v).__name__
            n = len(v) if hasattr(v, "__len__") else ""
            print(f"  - {k} ({t} {n})")
        print("PRICE-RELATED FIELDS (heuristic):")
        for path, val in find_price_paths(payload):
            s = str(val)
            if len(s) > 200:
                s = s[:200] + "..."
            print(f"  {path} = {s}")
        out = ROOT / f"tmp_is24_raw_{r['item_index']}.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"saved -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
