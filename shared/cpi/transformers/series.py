"""
shared/cpi/transformers/series.py

Pure transform: one bronze CPI observation -> one flat silver row.

The client already normalises the three providers into a common shape, so this
is validation plus provenance rather than reshaping. Kept as a separate pure
function anyway, matching the house convention, so it is unit-testable with no
network and no database.
"""
from __future__ import annotations

_STATUSES = ("definitive", "provisional")


def _decimal(value):
    """None stays None. Anything present must be numeric - a string that slipped
    through would be silently truncated by the driver."""
    if value is None:
        return None
    return float(value)


def transform_observation(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Flat silver row for one (provider, geo, period) observation.

    Raises ValueError when the row could not be keyed or carries no figure at
    all - the silver writer counts those as per-record errors rather than
    failing the whole run.
    """
    source_id = (raw.get("source_id") or "").strip()
    if not source_id:
        raise ValueError("missing source_id")

    period = (raw.get("period") or "").strip()
    if len(period) != 7 or period[4] != "-":
        raise ValueError(f"period must be YYYY-MM, got {period!r}")

    level = _decimal(raw.get("index_level"))
    rate = _decimal(raw.get("annual_rate_pct"))
    if level is None and rate is None:
        # Both providers publish the level and the rate together; a row with
        # neither is an empty observation, not a zero.
        raise ValueError(f"{source_id}: no index_level and no annual_rate_pct")

    status = (raw.get("status") or "definitive").strip().lower()
    if status not in _STATUSES:
        raise ValueError(f"{source_id}: unknown status {status!r}")

    return {
        "source_id": source_id,
        "bronze_id": bronze_id,
        "sync_run_id": sync_run_id,
        "provider": (raw.get("provider") or "").strip(),
        "geo": (raw.get("geo") or "").strip(),
        "index_code": (raw.get("index_code") or "").strip(),
        "index_name": (raw.get("index_name") or "").strip() or None,
        "base_year": (raw.get("base_year") or "").strip() or None,
        "period": period,
        "index_level": level,
        "annual_rate_pct": rate,
        "status": status,
        "source_url": (raw.get("source_url") or "").strip() or None,
        "published_at": (raw.get("published_at") or "").strip() or None,
    }
