"""
shared/nexudus/helpdesk.py

Shared help-desk plumbing used by BOTH ingestion paths, so the poll and the
webhook can never drift apart:

  * functions/nexudus_helpdesk_sync.py     -- 15-minute reconciling poll
  * functions/nexudus_helpdesk_webhook.py  -- Nexudus push (actions 45 / 46)

Endpoints (the API area is `support`, not the `community` the admin nav
would suggest):

  GET /api/support/helpdeskmessages     -- the tickets
  GET /api/support/helpdeskcomments     -- the reply thread
  GET /api/support/helpdeskdepartments  -- routing categories (per location)

Incremental filtering
---------------------
The generic `UpdatedSince` param that the rest of the Nexudus pipeline uses
is SILENTLY IGNORED on these endpoints — it returns the full table and looks
like it worked (verified 2026-08-20: 2,887 rows returned for every value).
The Nexudus-native `from_{Entity}_UpdatedOn` convention is what actually
filters, and it honours minute-level precision:

    from_HelpDeskMessage_UpdatedOn=2026-08-04T00:00:00Z  -> 83 rows
    from_HelpDeskMessage_UpdatedOn=2026-08-04T15:00:00Z  -> 79 rows

Why a poll exists at all when there are webhooks
------------------------------------------------
Nexudus's eWebhookAction enum has 90 entries and only TWO touch the help
desk: 45 `HelDeskMessageCreated` (Nexudus's own typo) and 46
`HelpDeskCommentCreated`. There is no updated / closed / assigned event, so
`Closed`, `ClosedOn`, `OwnerId` and `FirstResponseTimeInMinutes` would never
change after creation on a webhook-only design. The poll is what keeps ticket
lifecycle correct; the webhook is what makes new tickets land in seconds.

The poll is also the backstop for webhook delivery failure, which is a real
risk and not a theoretical one: Nexudus disables a webhook automatically
after 10 consecutive failures, and this tenant's original help-desk hooks
(pointed at a third party) died that way in Oct 2023 and went unnoticed.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Entity registry ─────────────────────────────────────────────

MESSAGES = "helpdesk_messages"
COMMENTS = "helpdesk_comments"
DEPARTMENTS = "helpdesk_departments"


@dataclass(frozen=True)
class HelpdeskEntity:
    name: str            # our entity key (meta.sync_runs, queue messages)
    endpoint: str        # Nexudus list endpoint
    updated_filter: str  # Nexudus-native incremental filter key
    bronze_method: str   # BronzeWriter method name
    min_ids: int         # reconcile safety floor


ENTITIES: dict[str, HelpdeskEntity] = {
    MESSAGES: HelpdeskEntity(
        name=MESSAGES,
        endpoint="support/helpdeskmessages",
        updated_filter="from_HelpDeskMessage_UpdatedOn",
        bronze_method="write_helpdesk_messages",
        min_ids=100,
    ),
    COMMENTS: HelpdeskEntity(
        name=COMMENTS,
        endpoint="support/helpdeskcomments",
        updated_filter="from_HelpDeskComment_UpdatedOn",
        bronze_method="write_helpdesk_comments",
        min_ids=100,
    ),
    DEPARTMENTS: HelpdeskEntity(
        name=DEPARTMENTS,
        endpoint="support/helpdeskdepartments",
        updated_filter="from_HelpDeskDepartment_UpdatedOn",
        bronze_method="write_helpdesk_departments",
        min_ids=5,
    ),
}

# Nexudus webhook action codes -> our entity key.
# 45 is spelled "HelDesk" in the Nexudus enum; that is not a typo here.
WEBHOOK_ACTIONS: dict[int, str] = {
    45: MESSAGES,   # HelDeskMessageCreated
    46: COMMENTS,   # HelpDeskCommentCreated
}

# Nexudus `resource` query-string values -> our entity key.
WEBHOOK_RESOURCES: dict[str, str] = {
    "helpdeskmessage": MESSAGES,
    "helpdeskcomment": COMMENTS,
    "helpdeskdepartment": DEPARTMENTS,
}


def lookback_minutes() -> int:
    """Minutes of `UpdatedOn` history the poll re-reads on every run.

    Deliberately far wider than the 15-minute poll interval: the window is
    what makes the poll self-healing. A missed run, a deploy, or a Nexudus
    outage shorter than this window costs nothing, because the next poll
    re-reads the whole period and the SHA-256 hash check means unchanged
    rows are not rewritten. At this volume (~10-20 changed rows/day) a 24h
    window is a couple of API calls.
    """
    try:
        return max(1, int(os.getenv("NEXUDUS_HELPDESK_LOOKBACK_MINUTES", "1440")))
    except (TypeError, ValueError):
        return 1440


def incremental_params(entity: HelpdeskEntity, has_previous_run: bool) -> dict:
    """Build the fetch params: full sweep on first run, lookback window after."""
    if not has_previous_run:
        logger.info("%s: no previous successful run — doing a full fetch", entity.name)
        return {}
    since = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes())
    stamp = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info("%s: incremental fetch since %s", entity.name, stamp)
    return {entity.updated_filter: stamp}


# ── Webhook payload handling ────────────────────────────────────

_ID_KEYS = ("Id", "id", "ID")
_NESTED_KEYS = ("Value", "Data", "Record", "Entity", "Item")


def extract_source_id(payload: Any) -> Optional[int]:
    """Pull the record id out of a Nexudus webhook body.

    Nexudus does not publish the webhook payload schema, so this accepts the
    record either flat or wrapped in a `Value`/`Data`/`Record` envelope. It
    only ever needs the id — the canonical record is then re-fetched from the
    API so bronze stores exactly the same shape the poll would have written.
    """
    if isinstance(payload, list):
        payload = payload[0] if payload else None
    if not isinstance(payload, dict):
        return None

    for key in _ID_KEYS:
        value = payload.get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass

    for key in _NESTED_KEYS:
        nested = payload.get(key)
        if isinstance(nested, (dict, list)):
            found = extract_source_id(nested)
            if found is not None:
                return found
    return None


def resolve_entity(
    payload: Any,
    resource: Optional[str] = None,
    action: Optional[str] = None,
) -> Optional[str]:
    """Decide which help-desk entity a webhook call refers to.

    Priority: explicit `resource` query param -> numeric/named action ->
    payload shape. The existing Nexudus hooks on this tenant call
    `...?action_name=HelDeskMessageCreated&resource=HelpDeskMessage`, so the
    query string is normally decisive; the payload sniff is the fallback.
    """
    if resource:
        key = resource.strip().lower().rstrip("s")
        if key in WEBHOOK_RESOURCES:
            return WEBHOOK_RESOURCES[key]

    if action:
        raw = action.strip()
        if raw.isdigit():
            mapped = WEBHOOK_ACTIONS.get(int(raw))
            if mapped:
                return mapped
        lowered = raw.lower()
        # Matches both "HelDeskMessageCreated" and "HelpDeskMessageCreated".
        if "comment" in lowered:
            return COMMENTS
        if "department" in lowered:
            return DEPARTMENTS
        if "desk" in lowered and "message" in lowered:
            return MESSAGES

    record = payload[0] if isinstance(payload, list) and payload else payload
    if isinstance(record, dict):
        for key in _NESTED_KEYS:
            nested = record.get(key)
            if isinstance(nested, dict):
                record = nested
                break
        if "HelpDeskMessageId" in record:
            return COMMENTS
        if "HelpDeskDepartmentId" in record or "Subject" in record:
            return MESSAGES
    return None


# ── Signature verification ──────────────────────────────────────

SIGNATURE_HEADER = "X-Nexudus-Hook-Signature"


def signature_mode() -> str:
    """off | warn | enforce  (default: warn).

    Nexudus documents that it sends `X-Nexudus-Hook-Signature`, computed from
    a shared secret and the request body, but does NOT publish the algorithm.
    `warn` therefore computes our best guess (HMAC-SHA256 of the raw body,
    accepted as hex or base64), logs whether it matched, and still accepts the
    request — so the very first real delivery tells you in App Insights
    whether the guess is right. Flip to `enforce` once it is confirmed.

    The endpoint is not unprotected in the meantime: it requires an Azure
    Functions key in the URL, which is what actually authenticates the caller.
    """
    mode = (os.getenv("NEXUDUS_WEBHOOK_SIGNATURE_MODE") or "warn").strip().lower()
    return mode if mode in {"off", "warn", "enforce"} else "warn"


def compute_signature(body: bytes, secret: str) -> tuple[str, str]:
    """Return (hex, base64) HMAC-SHA256 digests of the raw request body."""
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    return mac.hexdigest(), base64.b64encode(mac.digest()).decode("ascii")


def verify_signature(body: bytes, provided: Optional[str]) -> tuple[bool, str]:
    """Check the webhook signature. Returns (accept, reason).

    `accept` is what the caller should act on; it already folds in the
    configured mode, so a `warn`-mode mismatch returns True with a reason
    explaining what happened.
    """
    mode = signature_mode()
    secret = os.getenv("NEXUDUS_WEBHOOK_SECRET")

    if mode == "off":
        return True, "signature check disabled"
    if not secret:
        return True, "no NEXUDUS_WEBHOOK_SECRET configured — relying on the function key"
    if not provided:
        if mode == "enforce":
            return False, f"missing {SIGNATURE_HEADER} header"
        return True, f"missing {SIGNATURE_HEADER} header (warn mode — accepted)"

    hex_digest, b64_digest = compute_signature(body, secret)
    candidate = provided.strip()
    matched = any(
        hmac.compare_digest(candidate, expected)
        for expected in (hex_digest, b64_digest)
    )
    if matched:
        return True, "signature verified"
    if mode == "enforce":
        return False, "signature mismatch"
    return True, "signature mismatch (warn mode — accepted; confirm the algorithm before enforcing)"


# ── Webhook health ──────────────────────────────────────────────

WEBHOOK_ENDPOINT = "sys/webhooks"


async def check_webhook_health(client) -> dict[str, Any]:
    """Report whether the help-desk webhooks are alive on the Nexudus side.

    Nexudus auto-disables a webhook after 10 consecutive delivery failures and
    tells nobody. On this tenant that had already happened once: actions 45
    and 46 were registered in Oct 2023 against a third-party receiver, failed
    11 times, switched themselves off, and sat dead for ~3 years.

    Called once per poll (one cheap API call) so the condition surfaces in
    App Insights within 15 minutes instead of never. Returns a summary dict;
    never raises — health reporting must not be able to fail the sync.
    """
    summary: dict[str, Any] = {"configured": 0, "active": 0, "inactive": [], "error": None}
    try:
        hooks = await client.get_all(WEBHOOK_ENDPOINT)
    except Exception as exc:  # noqa: BLE001
        summary["error"] = str(exc)
        logger.warning("Could not read Nexudus webhook health: %s", exc)
        return summary

    for hook in hooks:
        if hook.get("Action") not in WEBHOOK_ACTIONS:
            continue
        summary["configured"] += 1
        if hook.get("Active"):
            summary["active"] += 1
        else:
            summary["inactive"].append({
                "action": hook.get("Action"),
                "name": hook.get("Name"),
                "business_id": hook.get("BusinessId"),
                "error_count": hook.get("ErrorCount"),
                "last_trigger": hook.get("LastTrigger"),
            })

    if summary["inactive"]:
        logger.warning(
            "Nexudus help-desk webhook(s) DISABLED at source — push ingestion is "
            "down and only the 15-minute poll is delivering data. Re-enable in "
            "Settings > Integrations > Webhooks. Details: %s",
            summary["inactive"],
        )
    elif summary["configured"] == 0:
        logger.info(
            "No Nexudus help-desk webhooks configured (actions 45/46) — running "
            "poll-only. Latency is the poll interval rather than seconds."
        )
    return summary
