"""One-off validation #6: abotapi URL mode with explicit per-page URLs.

URL mode respects LoopNet's min-space-size filter but only walks ONE page per
URL. Workaround: pass every page of the filtered search explicitly
(/for-rent/{n}/?min-space-size=16146, n=1..16 -> ~383 placards).

Validates: full coverage (~383 distinct buildings), surface parse coverage,
broker fields, duplicates across page URLs.
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
BASE = "https://www.loopnet.co.uk/search/office-space/london-england--united-kingdom/for-rent"
PAGES = 16

RUN_INPUT = {
    "mode": "url",
    "urls": [f"{BASE}/?min-space-size=16146"]
    + [f"{BASE}/{n}/?min-space-size=16146" for n in range(2, PAGES + 1)],
    "fetchDetails": True,
    "maxListings": 0,  # unlimited; the filtered search is ~383
    "maxPages": 1,     # each URL = exactly its own page (pagination is broken anyway)
    "proxy": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]},
}

SF_TO_M2 = 0.092903


def sqft(item: dict) -> float | None:
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
    print(f"Actor: {ACTOR_ID} (URL mode, {len(RUN_INPUT['urls'])} page URLs)")
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

    by_id: dict[str, dict] = {}
    for item in items:
        by_id.setdefault(str(item.get("id")), item)
    print(f"Distinct listing ids: {len(by_id)}")
    distinct_buildings = {str(i.get("propertyId")) for i in by_id.values() if i.get("propertyId")}
    print(f"Distinct propertyIds (buildings): {len(distinct_buildings)}")

    buckets = Counter()
    with_phone = with_company = with_coords = with_postcode = 0
    for item in by_id.values():
        sf = sqft(item)
        m2 = sf * SF_TO_M2 if sf else None
        if m2 is None:
            buckets["no_surface"] += 1
        elif m2 < 1500:
            buckets["<1500"] += 1
        elif m2 < 5000:
            buckets["1500-5000"] += 1
        else:
            buckets[">=5000"] += 1
        with_phone += bool(item.get("brokerPhone"))
        with_company += bool(item.get("brokerCompany"))
        with_coords += bool(item.get("latitude") and item.get("longitude"))
        with_postcode += bool(item.get("zipCode"))

    print("\nSurface distribution (available m2, parsed):")
    for k, v in sorted(buckets.items()):
        print(f"  {k}: {v}")
    n = len(by_id)
    print(f"\nbroker company: {with_company}/{n} | broker phone: {with_phone}/{n}")
    print(f"coordinates: {with_coords}/{n} | postcode: {with_postcode}/{n}")

    out_path = os.path.join(os.path.dirname(__file__), "loopnet_abotapi_pages_items.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(items, fh)
    print(f"\nRaw items saved to {out_path}")


if __name__ == "__main__":
    main()
