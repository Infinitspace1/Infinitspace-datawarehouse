"""
Thin wrapper around apify-client.

Provides:
  - async start (fire-and-forget): start_run() → {run_id, dataset_id}
  - status poll: get_run_status() → {finished, succeeded}
  - dataset fetch: fetch_dataset() → list[dict]
  - sync run (for short actors like Google Search): run_sync() → list[dict]
"""
from __future__ import annotations

import logging
import os
from typing import Any

from apify_client import ApifyClient

logger = logging.getLogger(__name__)

_APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")


def _client() -> ApifyClient:
    return ApifyClient(_APIFY_TOKEN)


def _run_field(run: Any, *names: str) -> Any:
    """
    Read a field from an Apify run regardless of client version.

    Older apify-client returns a plain dict (subscriptable); newer versions
    return a typed ``Run`` object (attribute access only). ``names`` lists the
    candidate keys/attributes in priority order (dict key, then snake_case attr).
    """
    for name in names:
        if isinstance(run, dict):
            if name in run:
                return run[name]
        elif hasattr(run, name):
            return getattr(run, name)
    raise KeyError(f"Apify run is missing any of {names!r} (type={type(run).__name__})")


def start_run(actor_id: str, run_input: dict, memory_mbytes: int | None = None) -> dict:
    """
    Start an Apify actor run without waiting for it to finish.
    Returns {"run_id": str, "dataset_id": str}.

    `memory_mbytes` overrides the actor's default allocation. The LoopNet
    actor defaults to 512 MB and is OOM-killed (exit 137) when handed a large
    listing-URL list — 460 New York URLs died at exactly 512 MB on 2026-07-23.
    """
    kwargs = {"run_input": run_input}
    if memory_mbytes:
        kwargs["memory_mbytes"] = memory_mbytes
    run = _client().actor(actor_id).start(**kwargs)
    run_id = _run_field(run, "id")
    dataset_id = _run_field(run, "defaultDatasetId", "default_dataset_id")
    logger.info("Apify run started: actor=%s run_id=%s", actor_id, run_id)
    return {
        "run_id": run_id,
        "dataset_id": dataset_id,
    }


def get_run_status(run_id: str) -> dict:
    """
    Poll a run's status.
    Returns {"finished": bool, "succeeded": bool, "status": str}.
    """
    run = _client().run(run_id).get()
    try:
        status = _run_field(run, "status") or "UNKNOWN"
    except KeyError:
        status = "UNKNOWN"
    finished = status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT")
    return {
        "finished": finished,
        "succeeded": status == "SUCCEEDED",
        "status": status,
    }


def fetch_dataset(dataset_id: str, limit: int | None = None) -> list[dict[str, Any]]:
    """Download all items from an Apify dataset."""
    dataset = _client().dataset(dataset_id)
    iterator = dataset.iterate_items(limit=limit) if limit else dataset.iterate_items()
    items = list(iterator)
    logger.info("Fetched %d items from dataset %s", len(items), dataset_id)
    return items


def iterate_dataset(dataset_id: str, limit: int | None = None):
    """Yield items from an Apify dataset one at a time.

    Unlike ``fetch_dataset`` this never materialises the full list in memory —
    the caller can stream items straight to storage. ``dataset.iterate_items``
    paginates under the hood, so peak memory stays at roughly one page rather
    than the whole dataset. Used by the streaming raw-persist path that keeps
    large datasets out of the Durable orchestrator (worker OOM fix)."""
    dataset = _client().dataset(dataset_id)
    iterator = dataset.iterate_items(limit=limit) if limit else dataset.iterate_items()
    count = 0
    for item in iterator:
        count += 1
        yield item
    logger.info("Iterated %d items from dataset %s", count, dataset_id)


def run_sync(actor_id: str, run_input: dict, limit: int = 100) -> list[dict[str, Any]]:
    """
    Start an actor and block until completion, then return dataset items.
    Use only for short-lived actors (e.g. Google Search scraper) called from
    within a Durable activity — not from the orchestrator itself.
    """
    run = _client().actor(actor_id).call(run_input=run_input)
    dataset_id = _run_field(run, "defaultDatasetId", "default_dataset_id")
    return fetch_dataset(dataset_id, limit=limit)
