"""One-off validation #7: abotapi search mode + sortBy=size-desc + minSqft.

Search mode paginates correctly (walked all 22 unfiltered pages in test #5)
but filters client-side, so it only saw the 40 big buildings inside the
~550-placard server window. Sorted by size DESCENDING, that window should
start with ALL the big buildings — the client-side minSqft filter then keeps
the full qualifying set (~383 per the live site) in a single run.
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
    "sortBy": "size-desc",
    "fetchDetails": True,
    "maxListings": 450,
    "maxPages": 40,
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
    print(f"Actor: {ACTOR_ID} (search mode, size-desc, minSqft=16146)")
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

    buckets = Counter()
    with_phone = with_company = with_coords = 0
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

    print("\nSurface distribution (available m2, parsed):")
    for k, v in sorted(buckets.items()):
        print(f"  {k}: {v}")
    n = len(by_id)
    print(f"\nbroker company: {with_company}/{n} | phone: {with_phone}/{n} | coords: {with_coords}/{n}")

    out_path = os.path.join(os.path.dirname(__file__), "loopnet_abotapi_sortdesc_items.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(items, fh)
    print(f"\nRaw items saved to {out_path}")


if __name__ == "__main__":
    main()
