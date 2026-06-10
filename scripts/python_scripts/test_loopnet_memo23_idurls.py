"""One-off validation #11: memo23 with slug-less /Listing/{id}/ URLs (the form
abotapi returns). Probe #9 proved memo23 handles slugged listing URLs; the
enumeration hands over slug-less ones, so verify those too."""
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.location_scraper.adapters.loopnet import LoopnetAdapter
from shared.location_scraper.clients import apify
from shared.location_scraper.config import LOOPNET_ACTOR_ID

TEST_URLS = [
    "https://www.loopnet.co.uk/Listing/34548634/",
    "https://www.loopnet.co.uk/Listing/34501482/",
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
    info = apify.start_run(LOOPNET_ACTOR_ID, RUN_INPUT)
    print(f"run_id={info['run_id']}")
    while True:
        status = apify.get_run_status(info["run_id"])
        print(f"  status={status['status']}")
        if status["finished"]:
            break
        time.sleep(15)
    if not status["succeeded"]:
        print("RUN FAILED.")
        return
    items = apify.fetch_dataset(info["dataset_id"])
    print(f"Total items: {len(items)}")
    adapter = LoopnetAdapter()
    for item in items:
        listing = adapter.normalize(item, "london")
        print(
            f"- propertyId={item.get('propertyId')} address={item.get('address')!r} "
            f"brokerEmail={item.get('brokerEmail')!r} "
            f"normalized={'OK' if listing else 'DROPPED'}"
        )


if __name__ == "__main__":
    main()
