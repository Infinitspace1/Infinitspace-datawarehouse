"""
Activity: assess_run_health

Grades a finished Apify run before the orchestrator commits to it, so a run
that came back nearly empty is retried instead of being written to SQL as a
normal ``completed`` week. See ``shared/location_scraper/run_health.py`` for
why (upstream detail-fetch losses + enumeration returning no URLs).
"""
from __future__ import annotations

import logging

from shared.azure_clients.sql_client import get_sql_client
from shared.location_scraper import run_health
from shared.location_scraper.clients import apify as apify_client

logger = logging.getLogger(__name__)

# Recent raw volumes for the same city. Ordered by finished_at so a re-run of an
# old run_id cannot masquerade as the latest data point; zero-item runs are
# excluded because a failed week must not lower the bar for the next one.
_BASELINE_SQL = """
SELECT TOP (?) raw_item_count
FROM bronze.location_scraper_run_quality
WHERE city = ?
  AND source = ?
  AND run_id <> ?
  AND raw_item_count > 0
ORDER BY finished_at DESC
"""


def recent_raw_item_counts(
    city: str,
    source: str,
    exclude_run_id: str,
    limit: int | None = None,
) -> list[int]:
    """Raw item counts of the city's most recent scrapes (best-effort)."""
    top = limit or run_health.baseline_runs()
    try:
        rows = get_sql_client().execute_query(
            _BASELINE_SQL, (top, city, source, exclude_run_id)
        )
    except Exception:
        logger.exception(
            "Could not read the run baseline for city=%s source=%s; "
            "grading on the detail-loss signal alone",
            city,
            source,
        )
        return []
    return [int(row["raw_item_count"]) for row in (rows or [])]


def assess_run_health(payload: dict) -> dict:
    """Return the health verdict for one finished Apify run.

    payload = {
        "run_id": str, "city": str, "source": str,
        "apify_run_id": str, "raw_item_count": int,
        "used_enumeration": bool, "enumerated_url_count": int,
    }
    """
    run_id = payload["run_id"]
    city = payload.get("city") or ""
    source = payload.get("source") or ""

    detail_health = run_health.parse_detail_health(
        apify_client.get_run_log(payload.get("apify_run_id") or "")
    )
    baseline_counts = recent_raw_item_counts(city, source, run_id)

    verdict = run_health.assess_run(
        raw_item_count=int(payload.get("raw_item_count") or 0),
        baseline_counts=baseline_counts,
        detail_health=detail_health,
        used_enumeration=bool(payload.get("used_enumeration")),
        enumerated_url_count=int(payload.get("enumerated_url_count") or 0),
    )
    logger.info(
        "Run health run_id=%s city=%s status=%s raw=%d baseline=%d "
        "detail_lost=%d/%d reason=%r",
        run_id,
        city,
        verdict["status"],
        verdict["raw_item_count"],
        verdict["baseline"],
        verdict["detail_lost"],
        verdict["detail_attempted"],
        verdict["reason"],
    )
    return verdict
