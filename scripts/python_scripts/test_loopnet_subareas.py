"""One-off validation #3: London sub-area fan-out.

The actor ignores URL query params AND its own BuildingSizeRange inputs for
UK searches (it rebuilds a bounding-box mobile-API search capped at 500).
Remaining lever: multiple startUrls, one per London submarket — each sub-box
returns < 500 (except City of London, 725 on the site, still capped) and the
union should surface far more >=1500 m2 buildings than the single-box 42.

Validates:
  - the actor geocodes neighbourhood slugs (soho_london-lnd--united-kingdom)
  - per-URL item counts vs the live site counts
  - deduped union: how many distinct buildings, how many >= 1500 m2
  - regression: all 42 baseline buildings must reappear
  - broker email coverage on qualifying buildings
"""
import json
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.location_scraper.adapters.loopnet import (
    LoopnetAdapter,
    available_surface_m2_from_payload,
)
from shared.location_scraper.clients import apify
from shared.location_scraper.config import LOOPNET_ACTOR_ID

AREAS = [
    "city-of-london_london-lnd--united-kingdom",   # 725 on site (will cap at 500)
    "soho_london-lnd--united-kingdom",             # 154
    "canary-wharf_london-lnd--united-kingdom",     # 69
    "kings-cross_london-lnd--united-kingdom",      # 66
    "farringdon_london-lnd--united-kingdom",       # 329
    "clerkenwell_london-lnd--united-kingdom",      # 189
    "victoria_london-lnd--united-kingdom",         # 172
    "hammersmith_london-lnd--united-kingdom",      # 255
    "stratford__london-lnd--united-kingdom",       # 30
    "croydon_london-lnd--united-kingdom",          # 129
    "white-city__london-lnd--united-kingdom",      # 4
]

RUN_INPUT = {
    "startUrls": [
        {"url": f"https://www.loopnet.co.uk/search/office-space/{a}/for-rent/"}
        for a in AREAS
    ],
    "includeListingDetails": True,
    "maxConcurrency": 20,
    "minConcurrency": 1,
    "proxy": {"useApifyProxy": True},
    "maxItems": 600,
}

BASELINE_PATH = os.path.join(
    os.path.dirname(__file__), "loopnet_filtered_test_items.json"
)


def main() -> None:
    print(f"{len(AREAS)} sub-area start URLs")
    info = apify.start_run(LOOPNET_ACTOR_ID, RUN_INPUT)
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
    print(f"\nTotal items (with duplicates across areas): {len(items)}")

    adapter = LoopnetAdapter()
    by_id: dict[str, dict] = {}
    for item in items:
        pid = str(item.get("propertyId") or item.get("listingUrl") or id(item))
        by_id.setdefault(pid, item)
    print(f"Distinct buildings (propertyId): {len(by_id)}")

    buckets = Counter()
    qualifying_ids = set()
    with_email = 0
    for pid, item in by_id.items():
        m2 = available_surface_m2_from_payload(item)
        if m2 is None:
            buckets["no_surface"] += 1
        elif m2 < 1500:
            buckets["<1500"] += 1
        elif m2 < 5000:
            buckets["1500-5000"] += 1
        else:
            buckets[">=5000"] += 1
        listing = adapter.normalize(item, "london")
        if listing:
            qualifying_ids.add(listing.external_id)
            if listing.email:
                with_email += 1

    print("\nSurface distribution (available m2, deduped):")
    for k, v in sorted(buckets.items()):
        print(f"  {k}: {v}")
    print(f"\nQualifying buildings (>=1500 m2): {len(qualifying_ids)}")
    print(f"  with broker email: {with_email}")

    if os.path.exists(BASELINE_PATH):
        with open(BASELINE_PATH, encoding="utf-8") as fh:
            baseline_items = json.load(fh)
        baseline_ids = set()
        for item in baseline_items:
            listing = adapter.normalize(item, "london")
            if listing:
                baseline_ids.add(listing.external_id)
        missing = baseline_ids - qualifying_ids
        gained = qualifying_ids - baseline_ids
        print(f"\nBaseline qualifying: {len(baseline_ids)}")
        print(f"Missing vs baseline: {len(missing)} {sorted(missing)[:10]}")
        print(f"NEW qualifying buildings: {len(gained)}")

    out_path = os.path.join(
        os.path.dirname(__file__), "loopnet_subareas_test_items.json"
    )
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(items, fh)
    print(f"\nRaw items saved to {out_path}")


if __name__ == "__main__":
    main()
