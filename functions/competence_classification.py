"""
functions/competence_classification.py

Nightly flexible-workspace classification of silver.competence_competitors. The APIFY scrape
only ever tags three flex-ish Google categories (Coworking space / Office space rental agency /
Business center), so the category can't separate real operators from brokers, virtual-office
services and noise — the website is the signal. This classifies the competitors the free rules
leave undecided with Anthropic (deduped by domain, metadata -> homepage escalate) and writes a
verdict per place_id to silver.competence_competitor_classification, feeding the clean
silver.competence_flex_competitors view.

Runs after competence_sync (which lands new competitors at 04:30 Mon-Sat). Incremental for
free: it only loads competitors not yet classified, so steady-state is a handful of new sites.
A per-run AI-unit cap bounds cost. Gated behind ENABLE_COMPETENCE_CLASSIFY (needs
ANTHROPIC_API_KEY). The one-off backfill of the existing operators is
scripts/python_scripts/backfill_competitor_classification.py.
"""
from __future__ import annotations

import logging
import os
import uuid

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.competence.classifier_service import CompetitorClassifier

logger = logging.getLogger(__name__)

bp = func.Blueprint()

# After competence_sync (04:30 Mon-Sat) so newly-synced competitors are classified the same night.
SCHEDULE = os.getenv("COMPETENCE_CLASSIFY_SCHEDULE", "0 30 5 * * 1-6")
# Cost guard: cap AI-classified operators per run (the backfill does the bulk; nightly is small).
MAX_AI_UNITS = int(os.getenv("COMPETENCE_CLASSIFY_MAX_AI_UNITS", "1500"))
# Auto-keep the "Coworking space" category for free. Off by default: a sample showed ~20% of
# that category is still non-flex, so we verify it with the AI like the others.
TRUST_COWORKING = os.getenv("COMPETENCE_CLASSIFY_TRUST_COWORKING", "false").strip().lower() in {"1", "true", "yes", "on"}


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def competence_classification(timer: func.TimerRequest) -> None:
    """Classify newly-synced competitors as flexible-workspace operators (or not)."""
    await run_competence_classification()


async def run_competence_classification(run_id: uuid.UUID | None = None) -> dict:
    """Classify the not-yet-classified competitors. Shared by the timer trigger and the local
    validation path so both run identical RunTracker-tracked logic. Returns a summary dict."""
    run_id = run_id or uuid.uuid4()
    logger.info("Competence classification started [run_id=%s]", run_id)
    async with RunTracker("competence", "competence_classification", "silver", metadata=str(run_id)) as run:
        classifier = CompetitorClassifier(
            trust_coworking_category=TRUST_COWORKING, max_ai_units=MAX_AI_UNITS,
        )
        report = classifier.classify()
        run.rows_read = report.units
        run.rows_written = report.rows_written
        run.rows_skipped = report.ai_unsure + report.ai_skipped_cap
        summary = report.as_dict()
        summary.pop("samples", None)
        logger.info("Competence classification complete [run_id=%s]: %s", run_id, summary)
    return summary
