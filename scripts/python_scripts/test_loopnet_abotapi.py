"""One-off validation #4: alternative actor abotapi/loopnet-scraper in URL mode.

The memo23 actor structurally caps dense markets at 500 (internal mobile API,
fixed-size bounding box, all filters ignored). abotapi claims true URL
pass-through — if it walks the real SRP pages, our space-available filter
(?min-space-size=16146 -> 383 results on the live site) will finally apply.

Validates: respected filter (count ~383), output field shape, broker contact
coverage (email?), surface values, and overlap with the 42 memo23 baseline.
"""
import json
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.location_scraper.clients import apify

ACTOR_ID = "abotapi/loopnet-scraper"

START_URL = (
    "https://www.loopnet.co.uk/search/office-space/"
    "london-england--united-kingdom/for-rent/?min-space-size=16146"
)

RUN_INPUT = {
    "mode": "url",
    "urls": [START_URL],
    "fetchDetails": True,
    "maxListings": 450,  # safety: expected ~383
    "maxPages": 200,
    "proxy": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]},
}


def main() -> None:
    print(f"Actor: {ACTOR_ID}")
    print(f"URL: {START_URL}")
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

    # Field inventory across all items
    field_presence = Counter()
    for item in items:
        for k, v in item.items():
            if v not in (None, "", [], {}):
                field_presence[k] += 1
    print("\nField coverage (non-empty count / total):")
    for k, v in field_presence.most_common():
        print(f"  {k}: {v}/{len(items)}")

    print("\nSample item:")
    print(json.dumps(items[0], indent=1, default=str)[:3000])

    out_path = os.path.join(os.path.dirname(__file__), "loopnet_abotapi_test_items.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(items, fh)
    print(f"\nRaw items saved to {out_path}")


if __name__ == "__main__":
    main()
