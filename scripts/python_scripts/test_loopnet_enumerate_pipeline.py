"""End-to-end validation of the new LoopNet full-coverage path, exercising the
exact production activity functions (no Durable runtime needed):

  1. resolve_source('london')              -> filtered loopnet.co.uk URL
  2. enumerate_listing_urls(start_url)     -> ~383 listing URLs (web-scraper)
  3. start_apify_run(config+listing_urls)  -> memo23 on those URLs
  4. normalize with the existing adapter   -> expect ~383 listings w/ emails

Run:  .\\.venv\\Scripts\\python.exe scripts\\python_scripts\\test_loopnet_enumerate_pipeline.py
"""
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.location_scraper.activities import enumerate_loopnet
from shared.location_scraper.activities import scrape as scrape_act
from shared.location_scraper.activities.resolve import resolve_source
from shared.location_scraper.adapters.loopnet import LoopnetAdapter
from shared.location_scraper.clients import apify


def main() -> None:
    # 1. Resolve (same as ls_resolve_source)
    cfg = resolve_source("london", None, "local-validation", unlimited_items=True)
    print(f"start_url: {cfg.start_url}")

    # 2. Enumerate (same as ls_enumerate_loopnet_urls)
    t0 = time.time()
    urls = enumerate_loopnet.enumerate_listing_urls(cfg.start_url)
    print(f"enumerated {len(urls)} listing urls in {time.time() - t0:.0f}s")
    if not urls:
        print("ENUMERATION EMPTY — would fall back to broad search. Aborting.")
        return
    cfg.listing_urls = urls

    # 3. Start memo23 on the listing URLs (same as ls_start_apify_run)
    run_info = scrape_act.start_apify_run(cfg.to_dict())
    print(f"memo23 run_id={run_info['run_id']}")
    while True:
        status = apify.get_run_status(run_info["run_id"])
        print(f"  status={status['status']}")
        if status["finished"]:
            break
        time.sleep(30)
    if not status["succeeded"]:
        print("MEMO23 RUN FAILED")
        return

    # 4. Normalize with the unchanged adapter
    items = apify.fetch_dataset(run_info["dataset_id"])
    adapter = LoopnetAdapter()
    buckets = Counter()
    normalized = []
    for item in items:
        listing = adapter.normalize(item, "london")
        if listing is None:
            buckets["dropped(<1500m2_or_no_surface)"] += 1
            continue
        normalized.append(listing)
        buckets["with_email" if listing.email else "without_email"] += 1

    print(f"\nmemo23 items: {len(items)}")
    print(f"normalized (>=1500 m2): {len(normalized)}")
    for k, v in buckets.most_common():
        print(f"  {k}: {v}")
    print(f"distinct buildings: {len({l.external_id for l in normalized})}")


if __name__ == "__main__":
    main()
