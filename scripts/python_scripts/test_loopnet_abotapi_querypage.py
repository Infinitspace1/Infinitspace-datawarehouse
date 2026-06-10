"""One-off validation #10: abotapi URL mode with ?page=N query-param pagination.

LoopNet honors `?page=N` exactly like the `/N/` path segment (browser-verified)
and abotapi's URL rebuild keeps the query string — only the path page was
lost. So URLs `...?min-space-size=16146&page=N` should fetch the right pages.

Expect: pages 1-3 -> ~75 distinct listings, including the known page-3 ids.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.location_scraper.clients import apify

ACTOR_ID = "abotapi/loopnet-scraper"
BASE = (
    "https://www.loopnet.co.uk/search/office-space/"
    "london-england--united-kingdom/for-rent/?min-space-size=16146"
)
EXPECTED_PAGE3_IDS = {"34548634", "34501482", "34026682", "33969263"}

RUN_INPUT = {
    "mode": "url",
    "urls": [BASE, f"{BASE}&page=2", f"{BASE}&page=3"],
    "fetchDetails": False,
    "maxListings": 0,
    "maxPages": 1,
    "proxy": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]},
}


def main() -> None:
    info = apify.start_run(ACTOR_ID, RUN_INPUT)
    run_id, dataset_id = info["run_id"], info["dataset_id"]
    print(f"run_id={run_id}")

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
    ids = {str(i.get("id")) for i in items}
    print(f"\nTotal items: {len(items)} | distinct ids: {len(ids)}")
    found = ids & EXPECTED_PAGE3_IDS
    print(f"Known page-3 ids found: {sorted(found)}")
    print(f"Verdict: {'QUERY-PARAM PAGINATION WORKS' if len(found) >= 2 and len(ids) > 50 else 'STILL BROKEN'}")


if __name__ == "__main__":
    main()
