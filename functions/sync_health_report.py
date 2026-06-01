"""
functions/sync_health_report.py

Timer trigger: emails a daily sync health report from meta.sync_runs and
meta.sync_errors. Runs at 06:00 UTC by default, after all ETL jobs finish
around 05:30 UTC.

Report content:
  - Subject tagged [OK] or [FAIL] for inbox triage
  - Per-entity table: status flag, rows read/written, duration, error message
  - Record-level error summary from meta.sync_errors

Env vars:
  SYNC_HEALTH_REPORT_SCHEDULE   -- NCRONTAB, default "0 0 6 * * *"
  SYNC_REPORT_RECIPIENTS        -- comma-separated list of addresses
  SYNC_REPORT_LOOKBACK_HOURS    -- window size, default 24
  GRAPH_TENANT_ID / GRAPH_CLIENT_ID / GRAPH_CLIENT_SECRET / GRAPH_SENDER_UPN
"""
from __future__ import annotations

import html
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import azure.functions as func

from shared.azure_clients.sql_client import get_sql_client
from shared.notifications.graph_mailer import GraphMailerError, send_mail

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("SYNC_HEALTH_REPORT_SCHEDULE", "0 0 6 * * *")
DEFAULT_RECIPIENTS = (
    "bryan.swannie@infinitspace.com,baptiste.valentin@infinitspace.com"
)

# Top-level orchestrators that must produce a sync_run row every day.
# Only covers function-level entries — if any is absent the function never started.
_EXPECTED_DAILY: frozenset[tuple[str, str, str]] = frozenset([
    ("nexudus", "bronze_sync", "bronze"),
    ("ava", "product_availability", "ava"),
    ("bamboohr", "bamboohr_employees", "bronze"),
    ("finance_dashboard", "finance_dashboard", "gold"),
    ("replyio", "sequence_steps", "bronze"),
    # Landlord dashboard materialization — runs 03:00 UTC daily. If it
    # silently stops firing, the dashboard would just go stale without
    # anyone noticing; this guards against that.
    ("landlord_dashboard", "materialize_dashboard", "gold"),
])


def _parse_recipients() -> list[str]:
    raw = os.getenv("SYNC_REPORT_RECIPIENTS", DEFAULT_RECIPIENTS)
    return [a.strip() for a in raw.split(",") if a.strip()]


def _fetch_runs(lookback_hours: int) -> list[dict[str, Any]]:
    """Return the latest run per (source_name, entity, layer) within the window."""
    sql = get_sql_client()
    since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    query = """
        WITH ranked AS (
            SELECT
                source_name, entity, layer, status,
                started_at, finished_at,
                rows_read, rows_written, rows_skipped,
                error_message,
                ROW_NUMBER() OVER (
                    PARTITION BY source_name, entity, layer
                    ORDER BY started_at DESC
                ) AS rn
            FROM meta.sync_runs
            WHERE started_at >= ?
        )
        SELECT source_name, entity, layer, status,
               started_at, finished_at,
               rows_read, rows_written, rows_skipped,
               error_message
        FROM ranked
        WHERE rn = 1
        ORDER BY
            CASE status WHEN 'failed' THEN 0 WHEN 'running' THEN 1 ELSE 2 END,
            source_name, entity, layer
    """
    return sql.execute_query(query, (since,))


def _fetch_error_summary(lookback_hours: int) -> list[dict[str, Any]]:
    """Return per-entity record-level error counts + sample messages."""
    sql = get_sql_client()
    since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    query = """
        SELECT
            entity,
            COUNT(*) AS error_count,
            MAX(error_message) AS sample_error
        FROM meta.sync_errors
        WHERE created_at >= ?
        GROUP BY entity
        ORDER BY COUNT(*) DESC
    """
    try:
        return sql.execute_query(query, (since,))
    except Exception as exc:
        logger.warning("Could not fetch meta.sync_errors summary: %s", exc)
        return []


def _find_missing_runs(
    runs: list[dict[str, Any]],
    expected: frozenset[tuple[str, str, str]],
) -> list[tuple[str, str, str]]:
    """Return expected (source, entity, layer) tuples absent from the current window."""
    seen = {(r["source_name"], r["entity"], r["layer"]) for r in runs}
    return sorted(expected - seen)


def _fmt_duration(started: datetime | None, finished: datetime | None) -> str:
    if not started or not finished:
        return "—"
    seconds = (finished - started).total_seconds()
    if seconds < 60:
        return f"{seconds:.0f}s"
    return f"{seconds / 60:.1f}m"


def _fmt_int(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def _render_html(
    runs: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    missing: list[tuple[str, str, str]],
    lookback_hours: int,
    as_of: datetime,
) -> tuple[str, int, int, int, int]:
    """Return (html_body, failed_count, running_count, success_count, missing_count)."""
    failed = [r for r in runs if r["status"] == "failed"]
    running = [r for r in runs if r["status"] == "running"]
    success = [r for r in runs if r["status"] == "success"]

    def flag(status: str) -> str:
        if status == "success":
            return '<span style="color:#1a7f37;font-size:16px;">&#10003;</span>'
        if status == "failed":
            return '<span style="color:#cf222e;font-size:16px;">&#10007;</span>'
        return '<span style="color:#9a6700;font-size:16px;">&#9203;</span>'

    rows_html = []
    for source, entity, layer in missing:
        rows_html.append(
            "<tr style='background:#fff8c5;'>"
            "<td style='padding:6px 10px;text-align:center;'>"
            "<span style='color:#9a6700;font-size:16px;'>&#9888;</span></td>"
            f"<td style='padding:6px 10px;'>{html.escape(source)}</td>"
            f"<td style='padding:6px 10px;'>{html.escape(entity)}</td>"
            f"<td style='padding:6px 10px;'>{html.escape(layer)}</td>"
            "<td style='padding:6px 10px;text-align:right;'>—</td>"
            "<td style='padding:6px 10px;text-align:right;'>—</td>"
            "<td style='padding:6px 10px;'>—</td>"
            "<td style='padding:6px 10px;color:#9a6700;font-family:monospace;font-size:12px;'>"
            "never started in this window</td>"
            "</tr>"
        )
    for r in runs:
        err = r.get("error_message") or ""
        if len(err) > 200:
            err = err[:200] + "…"
        rows_html.append(
            "<tr>"
            f"<td style='padding:6px 10px;text-align:center;'>{flag(r['status'])}</td>"
            f"<td style='padding:6px 10px;'>{html.escape(r['source_name'] or '')}</td>"
            f"<td style='padding:6px 10px;'>{html.escape(r['entity'] or '')}</td>"
            f"<td style='padding:6px 10px;'>{html.escape(r['layer'] or '')}</td>"
            f"<td style='padding:6px 10px;text-align:right;'>{_fmt_int(r.get('rows_read'))}</td>"
            f"<td style='padding:6px 10px;text-align:right;'>{_fmt_int(r.get('rows_written'))}</td>"
            f"<td style='padding:6px 10px;'>{_fmt_duration(r.get('started_at'), r.get('finished_at'))}</td>"
            f"<td style='padding:6px 10px;color:#cf222e;font-family:monospace;font-size:12px;'>{html.escape(err)}</td>"
            "</tr>"
        )

    table = (
        "<table style='border-collapse:collapse;font-family:-apple-system,Segoe UI,sans-serif;font-size:13px;border:1px solid #d0d7de;'>"
        "<thead style='background:#f6f8fa;'>"
        "<tr>"
        "<th style='padding:8px 10px;text-align:left;'></th>"
        "<th style='padding:8px 10px;text-align:left;'>Source</th>"
        "<th style='padding:8px 10px;text-align:left;'>Entity</th>"
        "<th style='padding:8px 10px;text-align:left;'>Layer</th>"
        "<th style='padding:8px 10px;text-align:right;'>Read</th>"
        "<th style='padding:8px 10px;text-align:right;'>Written</th>"
        "<th style='padding:8px 10px;text-align:left;'>Duration</th>"
        "<th style='padding:8px 10px;text-align:left;'>Error</th>"
        "</tr>"
        "</thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table>"
    )

    err_html = ""
    if errors:
        err_rows = []
        for e in errors:
            sample = (e.get("sample_error") or "")[:300]
            err_rows.append(
                "<tr>"
                f"<td style='padding:6px 10px;'>{html.escape(e['entity'] or '')}</td>"
                f"<td style='padding:6px 10px;text-align:right;'>{_fmt_int(e['error_count'])}</td>"
                f"<td style='padding:6px 10px;font-family:monospace;font-size:12px;'>{html.escape(sample)}</td>"
                "</tr>"
            )
        err_html = (
            "<h3 style='margin-top:24px;font-family:-apple-system,Segoe UI,sans-serif;'>Record-level errors</h3>"
            "<table style='border-collapse:collapse;font-family:-apple-system,Segoe UI,sans-serif;font-size:13px;border:1px solid #d0d7de;'>"
            "<thead style='background:#f6f8fa;'>"
            "<tr>"
            "<th style='padding:8px 10px;text-align:left;'>Entity</th>"
            "<th style='padding:8px 10px;text-align:right;'>Error count</th>"
            "<th style='padding:8px 10px;text-align:left;'>Sample message</th>"
            "</tr>"
            "</thead>"
            f"<tbody>{''.join(err_rows)}</tbody>"
            "</table>"
        )

    summary_color = "#cf222e" if failed or running or missing else "#1a7f37"
    missing_part = f" · {len(missing)} never started" if missing else ""
    summary = (
        f"<p style='font-family:-apple-system,Segoe UI,sans-serif;font-size:14px;'>"
        f"<b style='color:{summary_color};'>"
        f"{len(success)} ok · {len(failed)} failed · {len(running)} still running{missing_part}</b>"
        f"<br><span style='color:#57606a;'>Window: last {lookback_hours}h "
        f"(as of {as_of.strftime('%Y-%m-%d %H:%M UTC')})</span></p>"
    )

    no_runs_html = (
        "<p style='color:#9a6700;font-family:-apple-system,Segoe UI,sans-serif;'>"
        "<b>No sync runs recorded in this window.</b> The scheduler may be broken."
        "</p>"
        if not runs else ""
    )

    body = (
        "<html><body style='background:#ffffff;padding:16px;'>"
        "<h2 style='font-family:-apple-system,Segoe UI,sans-serif;margin:0 0 8px 0;'>"
        "InfinitSpace data warehouse &mdash; sync health report</h2>"
        f"{summary}"
        f"{no_runs_html}"
        f"{table}"
        f"{err_html}"
        "</body></html>"
    )

    return body, len(failed), len(running), len(success), len(missing)


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def sync_health_report(timer: func.TimerRequest) -> None:
    """Daily health report: email green/red flags for last 24h of sync runs."""
    logger.info("Sync health report started")

    lookback_hours = int(os.getenv("SYNC_REPORT_LOOKBACK_HOURS", "24"))
    recipients = _parse_recipients()
    if not recipients:
        logger.warning("No SYNC_REPORT_RECIPIENTS configured; skipping report")
        return

    try:
        runs = _fetch_runs(lookback_hours)
        errors = _fetch_error_summary(lookback_hours)
        missing = _find_missing_runs(runs, _EXPECTED_DAILY)
        as_of = datetime.now(timezone.utc)

        body, failed, running, success, n_missing = _render_html(
            runs, errors, missing, lookback_hours, as_of,
        )

        date_str = as_of.strftime("%Y-%m-%d")
        if failed or running or n_missing or not runs:
            parts = []
            if failed:
                parts.append(f"{failed} failed")
            if running:
                parts.append(f"{running} stuck running")
            if n_missing:
                parts.append(f"{n_missing} never started")
            if not runs:
                parts.append("no runs recorded")
            subject = f"[FAIL] InfinitSpace sync report {date_str} ({', '.join(parts)})"
        else:
            subject = f"[OK] InfinitSpace sync report {date_str} ({success} ok)"

        send_mail(subject=subject, html_body=body, to_recipients=recipients)
        logger.info(
            "Sync health report sent: subject=%r, recipients=%s, ok=%d, failed=%d, running=%d, missing=%d",
            subject, recipients, success, failed, running, n_missing,
        )

    except GraphMailerError as exc:
        logger.error("Failed to send sync health report: %s", exc, exc_info=True)
        raise
    except Exception as exc:
        logger.error("Sync health report failed: %s", exc, exc_info=True)
        raise
