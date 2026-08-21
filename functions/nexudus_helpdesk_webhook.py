"""
functions/nexudus_helpdesk_webhook.py

HTTP POST /api/nexudus/helpdesk-webhook

Receives Nexudus help-desk push events so new customer requests and replies
land in the warehouse within seconds instead of on the next poll.

Register in Nexudus under Settings > Integrations > Webhooks, scoped to the
**(beyond Global)** business (id 1376491116) so it covers every location —
NOT to a single site. The tenant's original help-desk hooks were scoped to
London Aldgate alone, which would have silently missed 45% of tickets.

  action 45  HelDeskMessageCreated    ("HelDesk" is Nexudus's own spelling)
  action 46  HelpDeskCommentCreated

Auth: FUNCTION key (?code=... in the URL Nexudus calls), plus optional
HMAC verification of the X-Nexudus-Hook-Signature header — see
shared/nexudus/helpdesk.py::verify_signature for why that starts in
`warn` mode.

Flow
----
  1. Verify the signature (per NEXUDUS_WEBHOOK_SIGNATURE_MODE).
  2. Work out which entity this is and pull the record id out of the body.
  3. Re-fetch the CANONICAL record from the API by id. The webhook payload
     shape is undocumented, and storing it verbatim would put two different
     JSON shapes in the same bronze table — which would break the SHA-256
     hash dedup and make the transformers ambiguous. One extra GET buys
     guaranteed shape parity with the poll.
  4. For a comment, also re-fetch its parent ticket. This fills in the
     comment's location (comment payloads have no BusinessId) and, as a
     bonus, picks up the lifecycle fields a reply usually changes —
     FirstResponseTimeInMinutes, Closed, ClosedOn — which no webhook event
     would ever tell us about.
  5. Write bronze, promote to silver, return.

Why this almost always returns 200
----------------------------------
Nexudus disables a webhook permanently after 10 consecutive failures, and
that is exactly how this tenant's previous help-desk hooks died in October
2023 without anyone noticing. Because the 15-minute poll re-reads a 24-hour
window, a dropped webhook call is a latency blip, never data loss — so
keeping the hook ALIVE is worth more than making Nexudus retry. Errors are
therefore logged loudly and answered 200. The only rejection is a signature
failure while NEXUDUS_WEBHOOK_SIGNATURE_MODE=enforce, which is a real
security signal and should stop.

Response 200:
    {"ok": true, "entity": "...", "source_id": 123, "silver_written": 1,
     "duration_s": 0.42}
    {"ok": false, "reason": "...", "duration_s": 0.01}   # logged, still 200
Response 401:
    {"ok": false, "reason": "signature mismatch"}        # enforce mode only
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid

import azure.functions as func
from tenacity import RetryError

from shared.azure_clients.bronze_writer import BronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_writer_helpdesk_comments import SilverHelpdeskCommentsWriter
from shared.azure_clients.silver_writer_helpdesk_departments import SilverHelpdeskDepartmentsWriter
from shared.azure_clients.silver_writer_helpdesk_messages import SilverHelpdeskMessagesWriter
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient
from shared.nexudus.helpdesk import (
    COMMENTS,
    DEPARTMENTS,
    ENTITIES,
    MESSAGES,
    SIGNATURE_HEADER,
    extract_source_id,
    resolve_entity,
    verify_signature,
)

logger = logging.getLogger(__name__)

bp = func.Blueprint()

_SILVER_WRITERS = {
    MESSAGES: (SilverHelpdeskMessagesWriter, "helpdesk_messages"),
    COMMENTS: (SilverHelpdeskCommentsWriter, "helpdesk_comments"),
    DEPARTMENTS: (SilverHelpdeskDepartmentsWriter, "helpdesk_departments"),
}


def _json(payload: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload),
        status_code=status_code,
        mimetype="application/json",
    )


def _budget_seconds() -> float:
    """Wall-clock ceiling for the Nexudus round-trip inside the handler.

    A webhook must answer fast. Without a ceiling, one bad id costs ~35s:
    NexudusClient.get() is tenacity-retried on 5xx with exponential backoff,
    and Nexudus answers 500 (not 404) for an id that no longer exists, so the
    full 5-attempt ladder runs before the call gives up. Nexudus would score
    that as a failed delivery, and 10 consecutive failures disable the hook
    permanently — the precise outcome this design exists to avoid.

    Timing out here is safe: the 15-minute poll reconciles whatever was missed.
    """
    try:
        return max(1.0, float(os.getenv("NEXUDUS_WEBHOOK_TIMEOUT_SECONDS", "10")))
    except (TypeError, ValueError):
        return 10.0


async def _fetch_and_write(
    client: NexudusClient,
    writer: BronzeWriter,
    entity_key: str,
    source_id: int,
) -> bool:
    """Re-fetch one record by id and upsert it into bronze.

    Returns True only when the payload was genuinely NEW OR CHANGED, so the
    caller can skip the silver pass on a replay. That matters: each silver
    promotion runs a latest-row-per-source_id aggregate over the whole bronze
    table, and a duplicate delivery would otherwise pay that cost to write
    nothing. Nexudus retries deliveries, so replays are normal traffic.

    (If bronze is unchanged but silver somehow lacks the row, the 15-minute
    poll and the weekly reconcile both repair it.)

    Never raises for a missing/unfetchable record — returns False and leaves it
    to the poll.
    """
    entity = ENTITIES[entity_key]
    try:
        record = await client.get_one(f"{entity.endpoint}/{source_id}")
    except RetryError as exc:
        # Retries exhausted (Nexudus 5xx, incl. its answer for a dead id).
        logger.warning(
            "Help desk webhook: %s/%s unfetchable after retries (%s) — "
            "leaving it to the 15-minute poll",
            entity.endpoint, source_id, exc,
        )
        return False
    if not isinstance(record, dict) or not record.get("Id"):
        logger.warning(
            "Help desk webhook: %s/%s returned no usable record", entity.endpoint, source_id
        )
        return False
    changed, _written = getattr(writer, entity.bronze_method)([record])
    if not changed:
        logger.info(
            "Help desk webhook: %s/%s unchanged since last sync — skipping silver pass",
            entity.endpoint, source_id,
        )
    return bool(changed)


@bp.route(
    route="nexudus/helpdesk-webhook",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
async def nexudus_helpdesk_webhook(req: func.HttpRequest) -> func.HttpResponse:
    """Ingest one Nexudus help-desk push event."""
    t_start = time.monotonic()

    def elapsed() -> float:
        return round(time.monotonic() - t_start, 3)

    body = req.get_body() or b""

    # 1. Signature
    accepted, reason = verify_signature(body, req.headers.get(SIGNATURE_HEADER))
    if not accepted:
        logger.error("Help desk webhook rejected: %s", reason)
        return _json({"ok": False, "reason": reason}, status_code=401)
    if "mismatch" in reason or "missing" in reason:
        logger.warning("Help desk webhook signature: %s", reason)

    try:
        payload = json.loads(body.decode("utf-8")) if body else {}
    except (ValueError, UnicodeDecodeError) as exc:
        logger.error("Help desk webhook: unparseable body — %s", exc)
        return _json({"ok": False, "reason": "unparseable body", "duration_s": elapsed()})

    # 2. Route the event
    entity_key = resolve_entity(
        payload,
        resource=req.params.get("resource"),
        action=req.params.get("action_name") or req.params.get("action"),
    )
    source_id = extract_source_id(payload)

    if entity_key is None or source_id is None:
        # Not an error worth retrying — log the shape so the payload contract
        # can be pinned down from the first real delivery.
        logger.warning(
            "Help desk webhook: could not route event "
            "(entity=%s source_id=%s resource=%r action=%r keys=%s)",
            entity_key, source_id,
            req.params.get("resource"),
            req.params.get("action_name") or req.params.get("action"),
            sorted(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
        )
        return _json(
            {"ok": False, "reason": "could not determine entity or source_id",
             "duration_s": elapsed()}
        )

    logger.info("Help desk webhook: entity=%s source_id=%s", entity_key, source_id)

    run_id = uuid.uuid4()
    entities_touched: list[str] = []

    try:
        bearer_token = get_bearer_token()
        writer = BronzeWriter(run_id)

        async def _pull() -> None:
            async with NexudusClient(bearer_token) as client:
                # 3. Canonical re-fetch + bronze write
                if await _fetch_and_write(client, writer, entity_key, source_id):
                    entities_touched.append(entity_key)

                # 4. A reply also refreshes its parent ticket
                if entity_key == COMMENTS:
                    record = payload if isinstance(payload, dict) else {}
                    parent_id = record.get("HelpDeskMessageId")
                    if parent_id is None:
                        try:
                            comment = await client.get_one(
                                f"{ENTITIES[COMMENTS].endpoint}/{source_id}"
                            )
                            parent_id = (comment or {}).get("HelpDeskMessageId")
                        except RetryError:
                            parent_id = None
                    if parent_id:
                        try:
                            if await _fetch_and_write(
                                client, writer, MESSAGES, int(parent_id)
                            ):
                                entities_touched.insert(0, MESSAGES)
                        except Exception as exc:  # noqa: BLE001
                            logger.warning(
                                "Help desk webhook: parent ticket %s refresh failed — %s "
                                "(the 15-minute poll will pick it up)",
                                parent_id, exc,
                            )

        try:
            await asyncio.wait_for(_pull(), timeout=_budget_seconds())
        except asyncio.TimeoutError:
            logger.warning(
                "Help desk webhook: Nexudus fetch exceeded %ss for entity=%s source_id=%s "
                "— answering now, the 15-minute poll will reconcile",
                _budget_seconds(), entity_key, source_id,
            )

        # 5. Promote to silver. Messages first so comments can inherit location.
        silver_written = 0
        for key in entities_touched:
            writer_cls, result_key = _SILVER_WRITERS[key]
            async with RunTracker(
                "nexudus", key, "silver", triggered_by="webhook", metadata=str(run_id)
            ) as run:
                result = writer_cls(run_id).run()
                run.rows_read = int(result.get("rows_read") or 0)
                run.rows_written = int(result.get(result_key) or 0)
                silver_written += run.rows_written

        logger.info(
            "Help desk webhook complete: entity=%s source_id=%s touched=%s "
            "silver_written=%s duration_s=%s",
            entity_key, source_id, entities_touched, silver_written, elapsed(),
        )
        response = {
            "ok": True,
            "entity": entity_key,
            "source_id": source_id,
            "entities_touched": entities_touched,
            "silver_written": silver_written,
            "duration_s": elapsed(),
        }
        if not entities_touched:
            # Accepted, but nothing to do: a duplicate delivery, or the record
            # could not be fetched in the time budget. Said out loud so a
            # silent no-op is not mistaken for a successful ingest.
            response["note"] = "no change ingested (duplicate delivery or fetch unavailable)"
        return _json(response)

    except Exception as exc:  # noqa: BLE001 — see module docstring
        logger.error(
            "Help desk webhook failed for entity=%s source_id=%s — %s "
            "(answering 200 so Nexudus does not disable the hook; "
            "the 15-minute poll will reconcile)",
            entity_key, source_id, exc, exc_info=True,
        )
        return _json({"ok": False, "reason": str(exc), "duration_s": elapsed()})
