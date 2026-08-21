"""
functions/cpi_sync.py

Timer trigger: pulls national consumer price indices for the UK, the Netherlands
and Germany from each country's official statistics API and writes bronze +
silver. Backs the CPI% uplift column on the Budget Tracking Tool's anniversaries
tab, which reads silver.cpi_series directly (same database).

Mirrors the self-contained eventbrite_sync pattern. Full rolling-window fetch by
design: statistics offices revise already-published months, so an incremental
watermark would miss revisions; bronze hash-dedup keeps unchanged months free.

No secret and no registration needed - all three endpoints are public and
unauthenticated - so this ships inside ENABLE_ETL_FUNCTIONS rather than behind
its own flag, which would default OFF and silently never run.

There is no reconcile step: a published month is never withdrawn, only revised,
and a revision is a MERGE on the same source_id.

Default schedule: 05:40 UTC (before the 06:00 health report, so a failure shows
up in the same morning's email).
"""
from __future__ import annotations

import logging
import os
import uuid

import azure.functions as func

from shared.azure_clients.cpi_bronze_writer import CpiBronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_writer_cpi import SilverCpiSeriesWriter
from shared.cpi.client import DEFAULT_MONTHS, SERIES, fetch_series

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("CPI_SYNC_SCHEDULE", "0 40 5 * * *")
WINDOW_MONTHS = int(os.getenv("CPI_WINDOW_MONTHS", str(DEFAULT_MONTHS)))


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def cpi_sync(timer: func.TimerRequest) -> None:
    await run_cpi_sync()


async def run_cpi_sync(run_id: uuid.UUID | None = None) -> dict:
    """Run the CPI sync.

    Shared by the timer trigger and the local validation script so every path
    runs the identical RunTracker-tracked logic. Returns a summary dict.
    """
    run_id = run_id or uuid.uuid4()
    logger.info("CPI sync started [run_id=%s, window=%s months]", run_id, WINDOW_MONTHS)

    summary: dict = {"observations_read": 0, "bronze_written": 0, "silver": {}}

    # ── Bronze ───────────────────────────────────────────────
    async with RunTracker("cpi", "series", "bronze", metadata=str(run_id)) as bronze_run:
        observations = fetch_series(months=WINDOW_MONTHS)
        _assert_every_provider_reported(observations)

        bronze_run.rows_read = len(observations)
        _changed, bronze_run.rows_written = CpiBronzeWriter(run_id).write_series(observations)
        bronze_run.rows_skipped = bronze_run.rows_read - bronze_run.rows_written
        summary["observations_read"] = bronze_run.rows_read
        summary["bronze_written"] = bronze_run.rows_written
        logger.info(
            "CPI bronze: %s fetched, %s written, %s unchanged",
            bronze_run.rows_read, bronze_run.rows_written, bronze_run.rows_skipped,
        )

    # ── Silver ───────────────────────────────────────────────
    async with RunTracker("cpi", "series", "silver", metadata=str(run_id)) as silver_run:
        result = SilverCpiSeriesWriter(run_id).run()
        silver_run.rows_read = result["rows_read"]
        silver_run.rows_written = result["series"]
        silver_run.rows_skipped = result["errors"]
        summary["silver"] = result
        logger.info("CPI silver complete: %s", result)

    logger.info("CPI sync complete [run_id=%s]: %s", run_id, summary)
    return summary


def _assert_every_provider_reported(observations: list[dict]) -> None:
    """Fail loudly when a provider returns nothing.

    The freshness guard that matters: both CBS 83131NED and Eurostat
    prc_hicp_manr were retired and now answer HTTP 200 with an EMPTY value set
    rather than an error. Without this, a retired endpoint would write zero rows,
    the run would go green, and the tool would quietly keep showing last year's
    figure forever.
    """
    seen = {o["provider"] for o in observations}
    missing = sorted({s["provider"] for s in SERIES} - seen)
    if missing:
        raise RuntimeError(
            f"CPI: no observations from {', '.join(missing)}. Check the series ids - "
            "a discontinued endpoint answers 200 with an empty value set."
        )
