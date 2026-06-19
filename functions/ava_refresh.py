"""
functions/ava_refresh.py

Blueprint: Timer trigger (daily) that rebuilds the ava.product_availability
table by executing the stored procedure ava.sp_refresh_product_availability.

Runs 30 minutes after silver sync completes (03:00 UTC by default) to ensure
all silver tables are up to date before the ava layer is refreshed.
"""
import html
import logging
import os

import azure.functions as func

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.run_tracker import RunTracker
from shared.notifications.graph_mailer import GraphMailerError, send_mail

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("AVA_REFRESH_SCHEDULE", "0 0 3 * * *")

# Categories that MUST have exactly one price per location. Hot desks and
# dedicated desks are single-tariff products — a location sells one monthly
# rate for each. Private offices (vary by capacity), meeting rooms (member /
# non-member tiers) and day passes are legitimately multi-priced and excluded.
_SINGLE_PRICE_CATEGORIES = ("hot_desk", "dedicated_desk")

# Recipients for the duplicate-price data-quality alert. Falls back to the
# sync-health report recipients, then a hard default.
_PRICE_ALERT_DEFAULT_RECIPIENTS = "bryan.swannie@infinitspace.com"

_CURRENCY_SYMBOLS = {"GBP": "£", "EUR": "€", "USD": "$"}


def _assert_ava_objects_exist(sql) -> None:
    missing: list[str] = []

    table_exists = sql.execute_scalar(
        "SELECT CASE WHEN OBJECT_ID('ava.product_availability', 'U') IS NULL THEN 0 ELSE 1 END"
    )
    if not table_exists:
        missing.append(
            "table ava.product_availability (run scripts/sql_scripts/ava_product_availability_schema.sql)"
        )

    proc_exists = sql.execute_scalar(
        "SELECT CASE WHEN OBJECT_ID('ava.sp_refresh_product_availability', 'P') IS NULL THEN 0 ELSE 1 END"
    )
    if not proc_exists:
        missing.append(
            "procedure ava.sp_refresh_product_availability "
            "(run scripts/sql_scripts/ava_sp_refresh_product_availability.sql)"
        )

    if missing:
        raise RuntimeError(
            "AVA refresh prerequisites are missing or inaccessible: " + "; ".join(missing)
        )


# ---------------------------------------------------------------------------
# Location plans refresh (ava.location_plans)
# ---------------------------------------------------------------------------
# Per-location list of the Nexudus plans (= tariffs) we want Ava to surface —
# a filtered serving view of silver.nexudus_tariffs (desks/offices and €0
# plans are dropped; see scripts/sql_scripts/ava_location_plans_schema.sql).
# Refreshed right after product availability in the same nightly function.
#
# This step is resilient to the schema not being applied yet: if the table /
# procedure are absent it logs a warning and skips, so deploying this code
# before applying the SQL never breaks the nightly ava run.


def _location_plans_objects_exist(sql) -> bool:
    """True only when BOTH ava.location_plans and its refresh proc exist."""
    return bool(sql.execute_scalar(
        """
        SELECT CASE
                 WHEN OBJECT_ID('ava.location_plans', 'U') IS NOT NULL
                  AND OBJECT_ID('ava.sp_refresh_location_plans', 'P') IS NOT NULL
                 THEN 1 ELSE 0
               END
        """
    ))


async def _refresh_location_plans() -> None:
    """Rebuild ava.location_plans via its stored procedure.

    Tracked in meta.sync_runs as ('ava', 'location_plans', 'ava'). Skips
    (with a warning) when the schema has not been applied yet.
    """
    sql = get_sql_client()
    if not _location_plans_objects_exist(sql):
        logger.warning(
            "ava.location_plans / ava.sp_refresh_location_plans not found — "
            "skipping plans refresh. Apply scripts/sql_scripts/ava_location_plans_schema.sql."
        )
        return

    async with RunTracker("ava", "location_plans", "ava") as run:
        before = sql.execute_scalar("SELECT COUNT(1) FROM ava.location_plans")
        sql.execute_non_query("EXEC ava.sp_refresh_location_plans")
        after = sql.execute_scalar("SELECT COUNT(1) FROM ava.location_plans")
        run.rows_written = int(after) if after is not None else 0
        logger.info(
            "AVA location plans refresh complete: %s rows → %s rows in ava.location_plans",
            before, after,
        )


# ---------------------------------------------------------------------------
# Data-quality audit: duplicate hot/dedicated desk prices
# ---------------------------------------------------------------------------
# Ava reads ava.product_availability straight through. If a location has more
# than one price for the same desk type, Ava sees conflicting rows and may quote
# the wrong one — or, worse, the inconsistency teaches her to fabricate prices
# for cells she can't pin down. A clean table has exactly one hot_desk and one
# dedicated_desk price per location, so this audit emails an alert the moment a
# conflict appears, naming the offending Nexudus products so they can be fixed
# at source.


def _fetch_duplicate_desk_prices(sql) -> list[dict]:
    """Return every hot_desk / dedicated_desk row that shares a (location,
    category) with at least one other row at a DIFFERENT price.

    A healthy gold table returns []. Each returned row carries the price plus
    the Nexudus product behind it so the recipient can fix the source.
    """
    placeholders = ", ".join(["?"] * len(_SINGLE_PRICE_CATEGORIES))
    query = f"""
        SELECT
            pa.location_name,
            pa.item_category,
            pa.currency_code,
            pa.price,
            pa.product_source_id,
            pa.item_name
        FROM ava.product_availability pa
        JOIN (
            SELECT location_name, item_category
            FROM ava.product_availability
            WHERE item_category IN ({placeholders})
            GROUP BY location_name, item_category
            HAVING COUNT(DISTINCT price) > 1
        ) dup
          ON dup.location_name = pa.location_name
         AND dup.item_category = pa.item_category
        WHERE pa.item_category IN ({placeholders})
        ORDER BY pa.location_name, pa.item_category, pa.price
    """
    params = tuple(_SINGLE_PRICE_CATEGORIES) * 2
    return sql.execute_query(query, params)


def _group_duplicate_rows(rows: list[dict]) -> list[dict]:
    """Group flat rows into one entry per (location_name, item_category).

    Pure function — unit-testable without a DB.
    """
    grouped: dict[tuple, dict] = {}
    for r in rows:
        key = (r.get("location_name"), r.get("item_category"))
        g = grouped.setdefault(key, {
            "location_name": r.get("location_name"),
            "item_category": r.get("item_category"),
            "currency_code": r.get("currency_code"),
            "items": [],
        })
        g["items"].append({
            "price": r.get("price"),
            "product_source_id": r.get("product_source_id"),
            "item_name": r.get("item_name"),
        })
    return list(grouped.values())


def _render_duplicate_price_html(rows: list[dict]) -> str:
    """Render the alert email body. Returns '' when there is nothing to flag."""
    groups = _group_duplicate_rows(rows)
    if not groups:
        return ""

    def sym(code) -> str:
        return _CURRENCY_SYMBOLS.get((code or "").upper(), "")

    blocks = []
    for g in sorted(
        groups, key=lambda x: (x["location_name"] or "", x["item_category"] or "")
    ):
        cur = sym(g["currency_code"])
        item_rows = []
        for it in g["items"]:
            price = it["price"]
            price_str = f"{cur}{price}" if price is not None else "—"
            item_rows.append(
                "<tr>"
                f"<td style='padding:4px 10px;text-align:right;font-weight:600;'>{html.escape(price_str)}</td>"
                f"<td style='padding:4px 10px;font-family:monospace;'>{html.escape(str(it['product_source_id'] or '—'))}</td>"
                f"<td style='padding:4px 10px;'>{html.escape(str(it['item_name'] or '—'))}</td>"
                "</tr>"
            )
        blocks.append(
            "<div style='margin:0 0 18px 0;'>"
            "<h3 style='margin:0 0 6px 0;font-family:-apple-system,Segoe UI,sans-serif;'>"
            f"{html.escape(g['location_name'] or '?')} &mdash; {html.escape(g['item_category'] or '?')}</h3>"
            "<table style='border-collapse:collapse;font-family:-apple-system,Segoe UI,sans-serif;font-size:13px;border:1px solid #d0d7de;'>"
            "<thead style='background:#f6f8fa;'><tr>"
            "<th style='padding:6px 10px;text-align:right;'>Price</th>"
            "<th style='padding:6px 10px;text-align:left;'>Nexudus product id</th>"
            "<th style='padding:6px 10px;text-align:left;'>Product name</th>"
            "</tr></thead>"
            f"<tbody>{''.join(item_rows)}</tbody>"
            "</table>"
            "</div>"
        )

    intro = (
        "<p style='font-family:-apple-system,Segoe UI,sans-serif;font-size:14px;'>"
        f"<b style='color:#cf222e;'>{len(groups)} location/desk-type combination(s)</b> in "
        "<code>ava.product_availability</code> have more than one price for the same desk "
        "type. Hot desks and dedicated desks must have exactly one monthly price per "
        "location. Until the duplicates are removed at source (Nexudus), Ava sees "
        "conflicting prices for these and may quote the wrong one."
        "</p>"
    )
    return (
        "<html><body style='background:#ffffff;padding:16px;'>"
        "<h2 style='font-family:-apple-system,Segoe UI,sans-serif;margin:0 0 8px 0;'>"
        "beyond pricing data &mdash; duplicate desk prices detected</h2>"
        f"{intro}{''.join(blocks)}"
        "</body></html>"
    )


def _price_alert_recipients() -> list[str]:
    raw = (
        os.getenv("AVA_PRICE_ALERT_RECIPIENTS")
        or os.getenv("SYNC_REPORT_RECIPIENTS")
        or _PRICE_ALERT_DEFAULT_RECIPIENTS
    )
    return [a.strip() for a in raw.split(",") if a.strip()]


def _run_duplicate_price_audit() -> None:
    """Detect hot/dedicated-desk price conflicts in the freshly-rebuilt table and
    email an alert. Best-effort: callers must guard against exceptions so the
    audit can never fail the refresh run."""
    sql = get_sql_client()
    rows = _fetch_duplicate_desk_prices(sql)
    if not rows:
        logger.info(
            "Desk-price audit: no duplicate hot/dedicated desk prices found"
        )
        return

    groups = _group_duplicate_rows(rows)
    recipients = _price_alert_recipients()
    if not recipients:
        logger.warning(
            "Desk-price audit: %d conflict(s) found but no recipients configured",
            len(groups),
        )
        return

    body = _render_duplicate_price_html(rows)
    subject = (
        f"[ACTION] beyond pricing — {len(groups)} location(s) with conflicting desk prices"
    )
    try:
        send_mail(subject=subject, html_body=body, to_recipients=recipients)
        logger.warning(
            "Desk-price audit: %d conflicting group(s) found, alert emailed to %s",
            len(groups), recipients,
        )
    except GraphMailerError as exc:
        logger.error("Desk-price audit: failed to send alert email: %s", exc)


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def refresh_ava_availability(timer: func.TimerRequest) -> None:
    """Rebuild ava.product_availability from silver tables via stored procedure."""
    logger.info("AVA refresh started")

    try:
        async with RunTracker("ava", "product_availability", "ava") as run:
            sql = get_sql_client()
            _assert_ava_objects_exist(sql)

            # Count rows before to detect if SP produced output
            before = sql.execute_scalar("SELECT COUNT(1) FROM ava.product_availability")

            sql.execute_non_query("EXEC ava.sp_refresh_product_availability")

            after = sql.execute_scalar("SELECT COUNT(1) FROM ava.product_availability")
            run.rows_written = int(after) if after is not None else 0

            logger.info(
                f"AVA refresh complete: {before} rows → {after} rows in ava.product_availability"
            )

        # Rebuild the per-location plans table (ava.location_plans). Wrapped so a
        # failure here is recorded in meta (by its own RunTracker) but does not
        # mask the successful product_availability refresh or skip the audit below.
        try:
            await _refresh_location_plans()
        except Exception as plans_err:
            logger.error(
                "AVA location plans refresh failed (non-fatal to product availability): %s",
                plans_err, exc_info=True,
            )

        # Post-refresh data-quality audit. Runs after the RunTracker block so the
        # refresh is already recorded as successful — a mail/query hiccup here
        # must never fail the run or mask a good rebuild.
        try:
            _run_duplicate_price_audit()
        except Exception as audit_err:
            logger.warning(
                "Desk-price audit failed (non-fatal): %s", audit_err, exc_info=True
            )

    except Exception as e:
        logger.error(f"AVA refresh failed: {e}", exc_info=True)
        raise
