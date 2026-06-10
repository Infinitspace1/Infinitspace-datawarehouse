"""One-off validation #9: memo23 actor with INDIVIDUAL listing URLs.

If memo23 accepts loopnet.co.uk listing detail URLs and returns its usual rich
payload (incl. brokerEmail), the full-coverage pipeline becomes:
  1. enumerate the ~383 filtered listing URLs (16 SRP pages, browser-grade fetch)
  2. feed them to memo23 as startUrls -> existing payload format, emails included
  3. existing adapter/normalize path unchanged.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.location_scraper.adapters.loopnet import LoopnetAdapter
from shared.location_scraper.clients import apify
from shared.location_scraper.config import LOOPNET_ACTOR_ID

# 3 listings that the current pipeline MISSES (beyond the 500-window),
# confirmed present on page 3 of the filtered search.
TEST_URLS = [
    "https://www.loopnet.co.uk/listing/10-queen-street-pl-london/34548634/",
    "https://www.loopnet.co.uk/listing/21-southampton-row-london/34501482/",
    "https://www.loopnet.co.uk/listing/86-petty-france-london/34026682/",
]

RUN_INPUT = {
    "startUrls": [{"url": u} for u in TEST_URLS],
    "includeListingDetails": True,
    "maxConcurrency": 5,
    "minConcurrency": 1,
    "proxy": {"useApifyProxy": True},
    "maxItems": 10,
}


def main() -> None:
    print(f"memo23 with {len(TEST_URLS)} individual listing URLs")
    info = apify.start_run(LOOPNET_ACTOR_ID, RUN_INPUT)
    run_id, dataset_id = info["run_id"], info["dataset_id"]
    print(f"run_id={run_id} dataset_id={dataset_id}")

    while True:
        status = apify.get_run_status(run_id)
        print(f"  status={status['status']}")
        if status["finished"]:
            break
        time.sleep(15)

    if not status["succeeded"]:
        print("RUN FAILED.")
        return

    items = apify.fetch_dataset(dataset_id)
    print(f"\nTotal items: {len(items)}")
    adapter = LoopnetAdapter()
    for item in items:
        listing = adapter.normalize(item, "london")
        print(
            f"- propertyId={item.get('propertyId')} address={item.get('address')!r} "
            f"brokerEmail={item.get('brokerEmail')!r} brokerName={item.get('brokerName')!r} "
            f"normalized={'OK m2=' + str(listing.surface_m2) if listing else 'DROPPED'}"
        )

    out_path = os.path.join(os.path.dirname(__file__), "loopnet_memo23_listing_url_items.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(items, fh)
    print(f"\nRaw items saved to {out_path}")


if __name__ == "__main__":
    main()
