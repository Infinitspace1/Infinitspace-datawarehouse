"""
shared/replyio/client.py

Reply.io API client for fetching sequence steps and step performance stats.

API docs:
  - Steps:  GET /v3/sequences/{id}/steps
  - Stats:  GET /v1/Stats/CampaignStep
"""
import logging
import os
from datetime import date, datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

BASE_V1 = "https://api.reply.io/v1"
BASE_V3 = "https://api.reply.io/v3"

# AB test sequence metadata — mirrors ab_test_config.py in the backend.
# Update here whenever sequences are added or renamed.
SEQUENCE_META = {
    1573349: {"ab_variant": "control",   "sequence_type": None,              "language": "en"},
    1653021: {"ab_variant": "variant_a", "sequence_type": "location_driven", "language": "en"},
    1653022: {"ab_variant": "variant_b", "sequence_type": "name_forward",    "language": "en"},
    1653023: {"ab_variant": "variant_c", "sequence_type": "question_first",  "language": "en"},
}

SEQUENCE_IDS = list(SEQUENCE_META.keys())


def _get_api_key() -> str:
    key = os.getenv("REPLY_IO_API_KEY")
    if not key:
        raise EnvironmentError("REPLY_IO_API_KEY is not set")
    return key


def _headers() -> dict:
    return {"x-api-key": _get_api_key()}


def date_window(target: date | None = None) -> tuple[date, int, int]:
    """Return (run_date, from_unix, to_unix) for the given date (defaults to yesterday UTC)."""
    today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    if target is None:
        target = (today_utc - timedelta(days=1)).date()
    start = datetime(target.year, target.month, target.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return target, int(start.timestamp()), int(end.timestamp())


def fetch_sequence_steps(sequence_id: int) -> list[dict]:
    """Return list of step dicts for a sequence."""
    url = f"{BASE_V3}/sequences/{sequence_id}/steps"
    r = requests.get(url, headers=_headers(), timeout=30)
    r.raise_for_status()
    data = r.json()
    # v3 may wrap in a key — handle both list and {"steps": [...]}
    if isinstance(data, list):
        return data
    return data.get("steps") or data.get("data") or []


def fetch_step_stats(campaign_id: int, step_id: int, from_ts: int, to_ts: int) -> dict:
    """Return step performance stats for a date window."""
    r = requests.get(
        f"{BASE_V1}/Stats/CampaignStep",
        headers=_headers(),
        params={
            "campaignId": campaign_id,
            "stepId": step_id,
            "from": from_ts,
            "to": to_ts,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()
