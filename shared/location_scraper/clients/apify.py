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


def start_run(actor_id: str, run_input: dict) -> dict:
    """
    Start an Apify actor run without waiting for it to finish.
    Returns {"run_id": str, "dataset_id": str}.
    """
    run = _client().actor(actor_id).start(run_input=run_input)
    logger.info("Apify run started: actor=%s run_id=%s", actor_id, run["id"])
    return {
        "run_id": run["id"],
        "dataset_id": run["defaultDatasetId"],
    }


def get_run_status(run_id: str) -> dict:
    """
    Poll a run's status.
    Returns {"finished": bool, "succeeded": bool, "status": str}.
    """
    run = _client().run(run_id).get()
    status = run.get("status", "UNKNOWN")
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


def run_sync(actor_id: str, run_input: dict, limit: int = 100) -> list[dict[str, Any]]:
    """
    Start an actor and block until completion, then return dataset items.
    Use only for short-lived actors (e.g. Google Search scraper) called from
    within a Durable activity — not from the orchestrator itself.
    """
    run = _client().actor(actor_id).call(run_input=run_input)
    dataset_id = run["defaultDatasetId"]
    return fetch_dataset(dataset_id, limit=limit)
