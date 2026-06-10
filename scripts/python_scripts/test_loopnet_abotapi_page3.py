"""One-off validation #8: abotapi URL mode, single run, single URL = PAGE 3 of
the filtered search. If the run returns the page-3 ids (34548634, 34501482,
34026682, 33969263 confirmed server-side via browser), then '1 run per page'
is a viable full-coverage strategy (16 runs -> ~383 buildings).
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
URL = (
    "https://www.loopnet.co.uk/search/office-space/"
    "london-england--united-kingdom/for-rent/3/?min-space-size=16146"
)
EXPECTED_PAGE3_IDS = {"34548634", "34501482", "34026682", "33969263"}

RUN_INPUT = {
    "mode": "url",
    "urls": [URL],
    "fetchDetails": False,  # ids suffice for this probe
    "maxListings": 0,
    "maxPages": 1,
    "proxy": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]},
}


def main() -> None:
    print(f"URL: {URL}")
    info = apify.start_run(ACTOR_ID, RUN_INPUT)
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
    ids = {str(i.get("id")) for i in items}
    print(f"\nTotal items: {len(items)}")
    print(f"Expected page-3 ids found: {sorted(ids & EXPECTED_PAGE3_IDS)}")
    print(f"Verdict: {'PAGE 3 SERVED CORRECTLY' if ids & EXPECTED_PAGE3_IDS else 'GOT WRONG PAGE (probably page 1 again)'}")
    print(f"First 10 ids: {sorted(ids)[:10]}")


if __name__ == "__main__":
    main()
