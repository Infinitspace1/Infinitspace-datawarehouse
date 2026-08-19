"""
Activity: alert_run_health

Emails the weekly location-scraper result when at least one city did not come
back clean. Silent when every city completed -- the point is to surface the
runs that used to look fine in SQL while carrying a tenth of the market.
"""
from __future__ import annotations

import html
import logging
import os

from shared.location_scraper.activities import log_run
from shared.notifications.graph_mailer import GraphMailerError, send_mail

logger = logging.getLogger(__name__)

DEFAULT_RECIPIENTS = "bryan.swannie@infinitspace.com,baptiste.valentin@infinitspace.com"

_HEALTHY = "completed"


def _recipients() -> list[str]:
    raw = (
        os.getenv("LOCATION_SCRAPER_ALERT_RECIPIENTS")
        or os.getenv("SYNC_REPORT_RECIPIENTS")
        or DEFAULT_RECIPIENTS
    )
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


def _row_html(row: dict) -> str:
    status = str(row.get("status") or "")
    colour = "#b00020" if status != _HEALTHY else "#0a7d32"
    return (
        "<tr>"
        f"<td style='padding:4px 10px'>{html.escape(str(row.get('city') or ''))}</td>"
        f"<td style='padding:4px 10px'>{html.escape(str(row.get('source') or ''))}</td>"
        f"<td style='padding:4px 10px;text-align:right'>{int(row.get('buildings_found') or 0)}</td>"
        f"<td style='padding:4px 10px;text-align:right'>{int(row.get('buildings_new') or 0)}</td>"
        f"<td style='padding:4px 10px;color:{colour}'>{html.escape(status)}</td>"
        f"<td style='padding:4px 10px'>{html.escape(str(row.get('error_message') or ''))}</td>"
        "</tr>"
    )


def alert_run_health(payload: dict) -> dict:
    """payload = {"period_key": "2026-W34"}. Returns a small summary dict."""
    period_key = payload.get("period_key") or ""
    try:
        rows = log_run.runs_for_period(period_key)
    except Exception:
        logger.exception("Could not read weekly run rows for %s; skipping alert", period_key)
        return {"sent": False, "reason": "query failed"}

    unhealthy = [r for r in rows if str(r.get("status") or "").lower() != _HEALTHY]
    if not unhealthy:
        logger.info("Weekly location scraper %s: all %d cities completed", period_key, len(rows))
        return {"sent": False, "cities_total": len(rows), "cities_unhealthy": 0}

    recipients = _recipients()
    if not recipients:
        logger.warning("No location scraper alert recipients configured; skipping")
        return {"sent": False, "reason": "no recipients"}

    body = (
        f"<p>{len(unhealthy)} of {len(rows)} cities did not complete cleanly in "
        f"<strong>{html.escape(period_key)}</strong>.</p>"
        "<p>Degraded means the scrape finished but returned far less than the city "
        "normally yields (upstream actor losses). Those cities are retried "
        "automatically by the mid-week pass; this mail is the signal that the "
        "upstream source needs attention if it keeps happening.</p>"
        "<table style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px'>"
        "<tr style='background:#f0f0f0'>"
        "<th style='padding:4px 10px;text-align:left'>City</th>"
        "<th style='padding:4px 10px;text-align:left'>Source</th>"
        "<th style='padding:4px 10px;text-align:right'>Found</th>"
        "<th style='padding:4px 10px;text-align:right'>New</th>"
        "<th style='padding:4px 10px;text-align:left'>Status</th>"
        "<th style='padding:4px 10px;text-align:left'>Reason</th></tr>"
        + "".join(_row_html(r) for r in sorted(rows, key=lambda r: str(r.get("city") or "")))
        + "</table>"
    )
    subject = f"[FAIL] Location scraper {period_key}: {len(unhealthy)}/{len(rows)} cities degraded"

    try:
        send_mail(subject=subject, html_body=body, to_recipients=recipients)
    except GraphMailerError:
        logger.exception("Could not send the location scraper health alert for %s", period_key)
        return {"sent": False, "reason": "mail failed", "cities_unhealthy": len(unhealthy)}

    logger.warning(
        "Location scraper %s: %d/%d cities unhealthy (%s)",
        period_key,
        len(unhealthy),
        len(rows),
        ", ".join(str(r.get("city")) for r in unhealthy),
    )
    return {"sent": True, "cities_total": len(rows), "cities_unhealthy": len(unhealthy)}
