"""
functions/finance_dashboard_refresh.py

Timer triggers: rebuild the finance dashboard gold tables from silver/meta data
by executing gold.sp_refresh_finance_dashboard.

Two scheduled runs:
  * 05:30 UTC -- primary nightly rebuild (after Xero 04:00 + BambooHR 05:00).
  * 10:00 UTC -- recheck rebuild. The 05:30 run intermittently captures a
    reduced invoice set: the early-morning data is briefly incomplete right
    after the 05:15 invoice reconcile, so the rebuild can publish far fewer
    rows than it should. Re-running a few hours later, once the data has
    settled, reliably republishes the full worklist. (A manual re-run of the
    same procedure on 2026-06-02 turned 55 rows back into 180 with no data
    change -- the underlying invoices were always present; only the rebuild
    under-read them.)

Guardrail:
  The rebuild (DELETE + INSERT across the three gold tables) runs inside one
  transaction. If the new invoice worklist collapses below a fraction
  (FINANCE_DASHBOARD_MIN_WORKLIST_RATIO, default 0.5) of the currently-published
  count, the transaction is ROLLED BACK -- the previous good list stays live --
  and the run is failed so the 06:00 health-report email flags it. This stops an
  under-build from silently wiping most of the worklist off the dashboard
  (observed 2026-06-02: 180 -> 55, hiding ~125 genuinely-overdue invoices,
  including INV-2026.04-7199). The 10:00 recheck then republishes the full list.
"""
from __future__ import annotations

import logging
import os

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

bp = func.Blueprint()

# Primary nightly rebuild.
SCHEDULE = os.getenv("FINANCE_DASHBOARD_REFRESH_SCHEDULE", "0 30 5 * * *")
# Second rebuild later in the morning, after the early-morning data has settled.
SCHEDULE_RECHECK = os.getenv("FINANCE_DASHBOARD_RECHECK_SCHEDULE", "0 0 10 * * *")

# Guardrail thresholds (tunable via app settings).
#   _MIN_RATIO     -- reject a rebuild whose worklist is below this fraction of
#                     the currently-published count.
#   _MIN_BASELINE  -- skip the guardrail when the live count is this small; too
#                     small to tell a glitch from a legitimately short list.
_MIN_RATIO = float(os.getenv("FINANCE_DASHBOARD_MIN_WORKLIST_RATIO", "0.5"))
_MIN_BASELINE = int(os.getenv("FINANCE_DASHBOARD_GUARDRAIL_MIN_BASELINE", "20"))

_REBUILD_SP = "EXEC gold.sp_refresh_finance_dashboard"
_WORKLIST_COUNT = "SELECT COUNT(1) FROM gold.finance_dashboard_invoice_worklist"
_ACCESS_COUNT = "SELECT COUNT(1) FROM gold.finance_dashboard_user_access"
_REVENUE_COUNT = "SELECT COUNT(1) FROM gold.finance_dashboard_revenue_occupancy"


class WorklistCollapseError(RuntimeError):
    """Raised when a rebuild would shrink the invoice worklist below the
    guardrail threshold. Raising rolls the rebuild back (keeping the last good
    list live) and surfaces the run as failed for alerting."""


def _is_collapse(baseline: int, new_count: int) -> bool:
    """True if new_count is a suspicious collapse relative to baseline.

    Skipped when baseline < _MIN_BASELINE -- we don't want to block a
    legitimately tiny list, and a small baseline can't distinguish a glitch
    from a healthy swing.
    """
    if baseline < _MIN_BASELINE:
        return False
    return new_count < baseline * _MIN_RATIO


def _rebuild_finance_dashboard(trigger: str) -> int:
    """Rebuild the finance dashboard gold tables, guarded against a collapsing
    worklist. Returns total rows published across the three gold tables.

    Raises WorklistCollapseError (after rolling back) when the worklist
    collapses below the guardrail threshold.
    """
    sql = get_sql_client()

    # Baseline = currently-published worklist size (its own committed read).
    baseline = int(sql.execute_scalar(_WORKLIST_COUNT) or 0)

    # The rebuild + its verification counts run in a single transaction so a
    # collapse can be rolled back before it ever becomes visible. get_connection
    # commits on clean exit and rolls back if we raise.
    with sql.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(_REBUILD_SP)

        cursor.execute(_WORKLIST_COUNT)
        new_worklist = int(cursor.fetchone()[0] or 0)
        cursor.execute(_ACCESS_COUNT)
        new_access = int(cursor.fetchone()[0] or 0)
        cursor.execute(_REVENUE_COUNT)
        new_revenue = int(cursor.fetchone()[0] or 0)

        if _is_collapse(baseline, new_worklist):
            # Raising rolls the transaction back: the live tables revert to the
            # last good rebuild. The collapse is NOT published.
            raise WorklistCollapseError(
                f"[{trigger}] worklist rebuild collapsed {baseline} -> "
                f"{new_worklist} (below {_MIN_RATIO:.0%} of baseline); rolled "
                f"back and kept the previous list "
                f"(user_access={new_access}, revenue_occupancy={new_revenue})"
            )

        logger.info(
            "[%s] finance dashboard rebuild published: invoice_worklist %s -> %s, "
            "user_access -> %s, revenue_occupancy -> %s",
            trigger, baseline, new_worklist, new_access, new_revenue,
        )
        return new_worklist + new_access + new_revenue


async def _run(trigger: str) -> None:
    logger.info("Finance dashboard gold refresh started (%s)", trigger)
    try:
        async with RunTracker(
            "finance_dashboard", "finance_dashboard", "gold", metadata=trigger,
        ) as run:
            run.rows_written = _rebuild_finance_dashboard(trigger)
    except WorklistCollapseError as exc:
        # Already rolled back; surface as a failed run so the health report
        # flags it. The 10:00 recheck will republish the full list.
        logger.error("Finance dashboard rebuild guardrail tripped: %s", exc)
        raise
    except Exception as exc:
        logger.error(
            "Finance dashboard gold refresh failed (%s): %s",
            trigger, exc, exc_info=True,
        )
        raise


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def refresh_finance_dashboard(timer: func.TimerRequest) -> None:
    """Primary nightly rebuild of the finance dashboard gold tables (05:30 UTC)."""
    await _run("05:30")


@bp.timer_trigger(schedule=SCHEDULE_RECHECK, arg_name="timer", run_on_startup=False)
async def refresh_finance_dashboard_recheck(timer: func.TimerRequest) -> None:
    """Recheck rebuild (10:00 UTC). Re-runs the guarded rebuild after the
    early-morning data has settled, so a worklist that the 05:30 run under-built
    (and the guardrail rolled back) is republished in full."""
    await _run("10:00 recheck")
