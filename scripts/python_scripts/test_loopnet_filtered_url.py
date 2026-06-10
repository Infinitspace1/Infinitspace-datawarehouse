"""One-off validation: run the LoopNet actor with the space-available-filtered
start URL (min-space-size=16146 sq ft = 1500 m2) on the new loopnet.co.uk
domain, and check the dataset quality BEFORE changing the pipeline code.

Checks:
  - actor accepts the URL and the run succeeds
  - item count (expected ~383 from the live site, well under LoopNet's 500 cap)
  - surface distribution (the filter should make nearly all items >= 1500 m2)
  - how many items the existing adapter normalizes successfully
  - broker email coverage (gold only keeps buildings with an email)
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

START_URL = (
    "https://www.loopnet.co.uk/search/office-space/"
    "london-england--united-kingdom/for-rent/?min-space-size=16146"
)

RUN_INPUT = {
    "startUrls": [{"url": START_URL}],
    "includeListingDetails": True,
    "maxConcurrency": 20,
    "minConcurrency": 1,
    "proxy": {"useApifyProxy": True},
    # Candidate prod config: bypass LoopNet's 500-result cap...
    "moreResults": True,
    # ...but keep a hard safety net for THIS TEST in case the URL filter is
    # ignored and the actor starts walking all 750+ London listings.
    "maxItems": 600,
}


def main() -> None:
    print(f"Start URL: {START_URL}")
    print("Starting actor run...")
    info = apify.start_run(LOOPNET_ACTOR_ID, RUN_INPUT)
    run_id, dataset_id = info["run_id"], info["dataset_id"]
    print(f"run_id={run_id} dataset_id={dataset_id}")

    while True:
        status = apify.get_run_status(run_id)
        print(f"  status={status['status']}")
        if status["finished"]:
            break
        time.sleep(20)

    if not status["succeeded"]:
        print("RUN FAILED — aborting analysis.")
        return

    items = apify.fetch_dataset(dataset_id)
    print(f"\nTotal items: {len(items)}")

    adapter = LoopnetAdapter()
    buckets = Counter()
    normalized = 0
    with_email = 0
    with_phone = 0
    cities = Counter()
    sample = None
    for item in items:
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
            normalized += 1
            if listing.email:
                with_email += 1
            if listing.phone:
                with_phone += 1
            cities[listing.city] += 1
            if sample is None:
                sample = listing

    print("\nSurface distribution (available m2):")
    for k, v in sorted(buckets.items()):
        print(f"  {k}: {v}")

    print(f"\nNormalized by adapter (>=1500 m2 + valid): {normalized}")
    print(f"  with broker email: {with_email}")
    print(f"  with broker phone: {with_phone}")
    print(f"\nTop cities in payload: {cities.most_common(10)}")

    if sample:
        print("\nSample normalized listing:")
        print(json.dumps(sample.to_dict(), indent=2, default=str)[:1500])

    out_path = os.path.join(os.path.dirname(__file__), "loopnet_filtered_test_items.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(items, fh)
    print(f"\nRaw items saved to {out_path}")


if __name__ == "__main__":
    main()
