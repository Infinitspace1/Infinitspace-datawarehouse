"""One-off validation #5: abotapi/loopnet-scraper in SEARCH mode with native
minSqft filter (URL mode stopped after page 1 — likely a pagination bug with
query-param URLs; search mode builds its own URLs so pagination should work).

Expected if the filter maps to space-available: ~383 listings.
Validates: count, pagination, surface parseability, broker contact coverage,
overlap with the 42-building memo23 baseline.
"""
import json
import os
import re
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.location_scraper.clients import apify

ACTOR_ID = "abotapi/loopnet-scraper"

RUN_INPUT = {
    "mode": "search",
    "site": "co-uk",
    "locations": ["london-england--united-kingdom"],
    "listingType": "for-lease",
    "propertyType": "office-space",
    "minSqft": 16146,
    "fetchDetails": True,
    "maxListings": 450,
    "maxPages": 200,
    "proxy": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]},
}

SF_TO_M2 = 0.092903


def surface_sqft(item: dict) -> float | None:
    """Best-effort available sqft from explicit fields, then size/title text."""
    for key in ("sizeSqftMax", "sizeSqft"):
        v = item.get(key)
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    for text in (item.get("size"), item.get("title")):
        if not text:
            continue
        nums = re.findall(r"(\d[\d,]*)\s*sq ft", str(text), flags=re.IGNORECASE)
        if nums:
            return max(float(n.replace(",", "")) for n in nums)
    return None


def main() -> None:
    print(f"Actor: {ACTOR_ID} (search mode, minSqft=16146)")
    info = apify.start_run(ACTOR_ID, RUN_INPUT)
    run_id, dataset_id = info["run_id"], info["dataset_id"]
    print(f"run_id={run_id} dataset_id={dataset_id}")

    while True:
        status = apify.get_run_status(run_id)
        print(f"  status={status['status']}")
        if status["finished"]:
            break
        time.sleep(30)

    if not status["succeeded"]:
        print("RUN FAILED - aborting analysis.")
        return

    items = apify.fetch_dataset(dataset_id)
    print(f"\nTotal items: {len(items)}")
    if not items:
        return

    distinct_props = {str(i.get("propertyId") or i.get("id")) for i in items}
    print(f"Distinct propertyIds: {len(distinct_props)}")

    buckets = Counter()
    with_phone = 0
    with_email = 0
    with_coords = 0
    qualifying = 0
    for item in items:
        sf = surface_sqft(item)
        m2 = sf * SF_TO_M2 if sf else None
        if m2 is None:
            buckets["no_surface"] += 1
        elif m2 < 1500:
            buckets["<1500"] += 1
        elif m2 < 5000:
            buckets["1500-5000"] += 1
            qualifying += 1
        else:
            buckets[">=5000"] += 1
            qualifying += 1
        if item.get("brokerPhone"):
            with_phone += 1
        if any("email" in k.lower() and v for k, v in item.items()):
            with_email += 1
        if item.get("latitude") and item.get("longitude"):
            with_coords += 1

    print("\nSurface distribution (available m2, parsed):")
    for k, v in sorted(buckets.items()):
        print(f"  {k}: {v}")
    print(f"\nQualifying (>=1500 m2): {qualifying}")
    print(f"with broker phone: {with_phone}/{len(items)}")
    print(f"with any email field: {with_email}/{len(items)}")
    print(f"with coordinates: {with_coords}/{len(items)}")

    # Overlap with memo23 baseline (propertyId space may differ between actors)
    baseline_path = os.path.join(os.path.dirname(__file__), "loopnet_filtered_test_items.json")
    if os.path.exists(baseline_path):
        with open(baseline_path, encoding="utf-8") as fh:
            baseline_items = json.load(fh)
        base_props = {str(b.get("propertyId")) for b in baseline_items}
        overlap = distinct_props & base_props
        print(f"\npropertyId overlap with memo23 500-item run: {len(overlap)}")

    out_path = os.path.join(os.path.dirname(__file__), "loopnet_abotapi_search_items.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(items, fh)
    print(f"\nRaw items saved to {out_path}")


if __name__ == "__main__":
    main()
