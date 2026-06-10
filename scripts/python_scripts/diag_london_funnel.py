"""One-off diagnostic: where do London (LoopNet) buildings drop out of the funnel?"""
import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.azure_clients.sql_client import get_db
from shared.location_scraper.adapters.loopnet import available_surface_m2_from_payload


QUERIES = [
    (
        "Run logs london",
        """
        SELECT TOP 10 run_id, status, buildings_found, buildings_new, created_at, updated_at,
               LEFT(error_message, 120) AS err
        FROM bronze.n8n_location_scraper_logs
        WHERE city = 'london'
        ORDER BY created_at DESC
        """,
    ),
    (
        "RAW: items bruts par run london",
        """
        SELECT run_id, COUNT(*) AS raw_items
        FROM bronze.location_scraper_raw
        WHERE city = 'london'
        GROUP BY run_id
        ORDER BY MAX(inserted_at) DESC
        """,
    ),
    (
        "Run quality london",
        """
        SELECT run_id, raw_item_count, normalized_count, with_coords_count
        FROM bronze.location_scraper_run_quality
        WHERE city = 'london'
        """,
    ),
]


def main() -> None:
    db = get_db()
    for title, sql in QUERIES:
        print(f"\n=== {title} ===")
        try:
            rows = db.fetch_all(sql)
            if not rows:
                print("(0 rows)")
                continue
            cols = list(rows[0].keys())
            print(" | ".join(cols))
            for row in rows:
                print(" | ".join(str(row[c]) for c in cols))
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR: {exc}")

    # Surface distribution of the latest unlimited run: how many raw items
    # fall under the 1500 m2 floor?
    print("\n=== Distribution surface (run du 2026-06-10) ===")
    rows = db.fetch_all(
        """
        SELECT payload_json
        FROM bronze.location_scraper_raw
        WHERE run_id = 'london_2026-06-10T08-20-00-111826'
        """
    )
    buckets = {"<500": 0, "500-1000": 0, "1000-1500": 0, ">=1500": 0, "no_surface": 0}
    for r in rows:
        payload = json.loads(r["payload_json"])
        m2 = available_surface_m2_from_payload(payload)
        if m2 is None:
            buckets["no_surface"] += 1
        elif m2 < 500:
            buckets["<500"] += 1
        elif m2 < 1000:
            buckets["500-1000"] += 1
        elif m2 < 1500:
            buckets["1000-1500"] += 1
        else:
            buckets[">=1500"] += 1
    print(f"total raw: {len(rows)}")
    for k, v in buckets.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
