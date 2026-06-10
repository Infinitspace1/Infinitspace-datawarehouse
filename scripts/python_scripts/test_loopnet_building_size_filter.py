"""One-off validation #2: run the LoopNet actor with its native
BuildingSizeRangeMin input (the URL query params proved to be ignored — the
actor rebuilds the search via LoopNet's internal API and only forwards its own
filter inputs).

BuildingSizeRangeMin = 16146 sqft (1500 m2) is recall-safe: available space is
always <= total building size, so every building with >= 1500 m2 available
passes the filter. The goal is to shrink the search under LoopNet's 500-result
cap so we see ALL qualifying buildings instead of the first-500 window.

Also verifies no regression: every propertyId normalized from the baseline
unfiltered run (loopnet_filtered_test_items.json) must appear in this run.
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
    "london-england--united-kingdom/for-rent/"
)

RUN_INPUT = {
    "startUrls": [{"url": START_URL}],
    "includeListingDetails": True,
    "maxConcurrency": 20,
    "minConcurrency": 1,
    "proxy": {"useApifyProxy": True},
    # Native actor filter, forwarded to LoopNet's internal API (sqft).
    "BuildingSizeRangeMin": 16146,
    # Safety net for this test only.
    "maxItems": 600,
}

BASELINE_PATH = os.path.join(
    os.path.dirname(__file__), "loopnet_filtered_test_items.json"
)


def main() -> None:
    print(f"Start URL: {START_URL}")
    print(f"BuildingSizeRangeMin: {RUN_INPUT['BuildingSizeRangeMin']} sqft")
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
        print("RUN FAILED - aborting analysis.")
        return

    items = apify.fetch_dataset(dataset_id)
    print(f"\nTotal items: {len(items)}")

    adapter = LoopnetAdapter()
    buckets = Counter()
    normalized_ids = set()
    with_email = 0
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
            normalized_ids.add(listing.external_id)
            if listing.email:
                with_email += 1

    print("\nSurface distribution (available m2):")
    for k, v in sorted(buckets.items()):
        print(f"  {k}: {v}")
    print(f"\nNormalized by adapter (>=1500 m2): {len(normalized_ids)}")
    print(f"  with broker email: {with_email}")

    # Regression check vs baseline unfiltered run
    if os.path.exists(BASELINE_PATH):
        with open(BASELINE_PATH, encoding="utf-8") as fh:
            baseline_items = json.load(fh)
        baseline_ids = set()
        for item in baseline_items:
            listing = adapter.normalize(item, "london")
            if listing:
                baseline_ids.add(listing.external_id)
        missing = baseline_ids - normalized_ids
        gained = normalized_ids - baseline_ids
        print(f"\nBaseline qualifying buildings: {len(baseline_ids)}")
        print(f"Missing from filtered run: {len(missing)} {sorted(missing)[:10]}")
        print(f"New buildings found: {len(gained)}")
    else:
        print("\n(no baseline file, skipping regression check)")

    out_path = os.path.join(
        os.path.dirname(__file__), "loopnet_building_size_test_items.json"
    )
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(items, fh)
    print(f"\nRaw items saved to {out_path}")


if __name__ == "__main__":
    main()
