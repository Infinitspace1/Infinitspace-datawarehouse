"""
Health verdict for one location-scraper run.

Pure functions only -- safe to call from the Durable orchestrator (they are
re-executed on every replay and must stay deterministic).

Why this exists (2026-08-18)
----------------------------
A LoopNet run can "succeed" while returning almost nothing, and until now that
was written to SQL as ``completed`` with no signal. Two independent upstream
failures produce it:

  1. The memo23 actor's LoopNet mobile API returns 403 (App Check) for EVERY
     listing, so each one falls back to the actor's paid unblocker chain. When
     that chain is throttled the listing is dropped. Measured on the 2026-08-17
     weekly wave: London lost 34/48 listings, Los Angeles 252/329, Austin
     78/89 -- while the same London city scraped at 10:36 UTC on 2026-07-31
     lost only 21/395. The candidate count is unchanged; only the recovery rate
     collapses, and it collapses inside the nightly batch window.
  2. The enumeration actor (LoopNet gb/ca) can return 0 URLs when the
     residential edge refuses it, which silently degrades the city to the
     legacy broad search.
  3. That same unblocker chain runs out of quota outright (HTTP 401). Then even
     the SEARCH stage fails on loopnet.com, which has no enumeration path to
     fall back on, and the city returns nothing at all.

(1) and (2) are transient, so the orchestrator retries them; (3) is a spent
shared budget, so retrying only burns actor starts and the verdict says so.
This module decides which case a finished run is in, and what to record.
"""
from __future__ import annotations

import os
from typing import Any, Optional

# Log markers emitted by the memo23 LoopNet actor (0ZCQONxB3BdyOzrbD). Verified
# 2026-08-18 across 6 runs: lost + recovered == attempted in every one, and
# recovered == the actor's own "Total items saved" count.
_MARKER_ATTEMPTED = "mobile API 403"
_MARKER_LOST = "unblocker fallback unavailable/failed"
_MARKER_RECOVERED = "recovered via website unblocker"

# The actor's unblocker providers are a resource shared by every user of the
# actor, and it runs dry: on 2026-08-18 a 317-listing London run drained it, and
# every city started afterwards logged "scrapingbee is quota-throttled (HTTP
# 401)" and returned 0 items — including the search stage on loopnet.com, which
# has no enumeration path to fall back on. Retrying that is pure waste (each
# attempt still bills an actor start), so it ends the attempt loop immediately.
_MARKER_PROVIDER_QUOTA = "quota-throttled"

# Share of candidate listings the actor may drop before the run is degraded.
# Healthy runs sit at 0-7%; the 2026-08-17 wave sat at 24-77%.
DEFAULT_DETAIL_LOSS_THRESHOLD = 0.20
# A run must reach this share of the city's recent best raw item count.
DEFAULT_BASELINE_RATIO = 0.6
# How many previous runs of the city define that baseline.
DEFAULT_BASELINE_RUNS = 8
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_MINUTES = 30


def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def detail_loss_threshold() -> float:
    return _env_float("LOCATION_SCRAPER_DETAIL_LOSS_THRESHOLD", DEFAULT_DETAIL_LOSS_THRESHOLD)


def baseline_ratio() -> float:
    return _env_float("LOCATION_SCRAPER_BASELINE_RATIO", DEFAULT_BASELINE_RATIO)


def baseline_runs() -> int:
    return _env_int("LOCATION_SCRAPER_BASELINE_RUNS", DEFAULT_BASELINE_RUNS)


def max_attempts() -> int:
    return _env_int("LOCATION_SCRAPER_MAX_ATTEMPTS", DEFAULT_MAX_ATTEMPTS)


def retry_delay_minutes() -> int:
    return _env_int("LOCATION_SCRAPER_RETRY_DELAY_MINUTES", DEFAULT_RETRY_DELAY_MINUTES)


def parse_detail_health(log_text: Optional[str]) -> dict[str, Any]:
    """Count listings the actor tried, lost and recovered, from its run log.

    Returns ``loss_rate=None`` when the log carries no detail-fetch markers at
    all (other sources, or an actor version that stopped logging them) -- the
    caller then falls back to the volume baseline alone rather than assuming
    the run is healthy or broken.
    """
    text = log_text or ""
    attempted = text.count(_MARKER_ATTEMPTED)
    lost = text.count(_MARKER_LOST)
    recovered = text.count(_MARKER_RECOVERED)

    # A listing that hit the API but never produced an outcome line was dropped
    # too, so the denominator is whichever is larger and everything that is not
    # an explicit recovery counts as lost.
    total = max(attempted, lost + recovered)
    lost_effective = total - recovered
    loss_rate = (lost_effective / total) if total else None
    return {
        "attempted": total,
        "lost": lost_effective,
        "recovered": recovered,
        "loss_rate": loss_rate,
        "provider_exhausted": _MARKER_PROVIDER_QUOTA in text,
    }


def assess_run(
    *,
    raw_item_count: int,
    baseline_counts: Optional[list[int]] = None,
    detail_health: Optional[dict[str, Any]] = None,
    used_enumeration: bool = False,
    enumerated_url_count: int = 0,
) -> dict[str, Any]:
    """Decide whether a finished scrape is healthy enough to keep.

    ``baseline_counts`` are the raw item counts of the city's recent runs; the
    best of them is the reference, because a degraded week must not lower the
    bar for the next one.
    """
    detail = detail_health or {}
    loss_rate = detail.get("loss_rate")
    baseline = max(baseline_counts or [0])
    ratio = baseline_ratio()
    floor = int(baseline * ratio)

    provider_exhausted = bool(detail.get("provider_exhausted"))

    reasons: list[str] = []
    if raw_item_count <= 0:
        reasons.append("no items returned")
    if used_enumeration and enumerated_url_count <= 0:
        reasons.append("enumeration returned no URLs (fell back to the broad search)")
    if loss_rate is not None and loss_rate > detail_loss_threshold():
        reasons.append(
            f"actor dropped {detail.get('lost', 0)}/{detail.get('attempted', 0)} "
            f"listings ({loss_rate:.0%}) on the detail fetch"
        )
    if baseline > 0 and raw_item_count < floor:
        reasons.append(
            f"{raw_item_count} items vs a {baseline}-item baseline (floor {floor})"
        )

    # The exhausted quota is a retry signal, NOT a quality one: a run that
    # delivered its city and happened to drain the budget on the way out is
    # still a good run (London 2026-08-18 09:32, 267 items, 16% loss). It only
    # explains a run that IS bad, so it is appended rather than triggering.
    if reasons and provider_exhausted:
        reasons.append(
            "the actor's unblocker quota is exhausted (HTTP 401) — retrying will not help"
        )

    # ...and it only makes a retry pointless when the run actually depended on
    # the unblocker. Since 2026-08-19 LoopNet is scraped through the free
    # mobile-API search stage (no detail fetch), so the marker can show up in
    # the log of a run that never needed it -- New York 2026-08-19 returned 272
    # items with 0 detail attempts and the quota marker present. Treating that
    # as "do not retry" would silently disarm the retry loop.
    retry_useless = provider_exhausted and (
        int(detail.get("attempted") or 0) > 0 or raw_item_count <= 0
    )

    return {
        "ok": not reasons,
        "status": "ok" if not reasons else "degraded",
        # Nothing a retry can do: the shared upstream budget is spent AND the
        # run needed it, so the caller stops instead of burning actor starts.
        "retry_useless": retry_useless,
        "reason": "; ".join(reasons),
        "raw_item_count": int(raw_item_count),
        "baseline": int(baseline),
        "baseline_floor": floor,
        "detail_attempted": int(detail.get("attempted") or 0),
        "detail_lost": int(detail.get("lost") or 0),
        "detail_loss_rate": loss_rate,
        "used_enumeration": bool(used_enumeration),
        "enumerated_url_count": int(enumerated_url_count),
    }
