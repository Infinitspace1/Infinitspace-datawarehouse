"""
function_app.py

Azure Functions entry point.

Deployment model:
  - Default publish: ETL-only surface (timers + queue worker).
  - Optional admin publish: enable admin/debug HTTP routes with app settings.
  - Optional real-estate publish: enable Real Estate HTTP API routes.
  - Optional location-scraper publish: enable Durable Functions scrape pipeline.

App settings:
  - ENABLE_ETL_FUNCTIONS=1                  default
  - ENABLE_ADMIN_FUNCTIONS=0                default
  - ENABLE_REAL_ESTATE_FUNCTIONS=0          default
  - ENABLE_LOCATION_SCRAPER_FUNCTIONS=0     default
  - ENABLE_COMPETENCE_FUNCTIONS=0           default (needs FIREBASE_CREDENTIALS)
  - ENABLE_COMPETENCE_CLASSIFY=0            default (needs ANTHROPIC_API_KEY)
  - ENABLE_HUBSPOT_FUNCTIONS=0              default (needs HUBSPOT_ACCESS_TOKEN)
  - ENABLE_EVENTBRITE_FUNCTIONS=0           default (needs EVENTBRITE_PRIVATE_TOKEN)
"""
from __future__ import annotations

import os

import azure.durable_functions as df
from dotenv import load_dotenv

load_dotenv()


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# DFApp extends FunctionApp and adds orchestration/activity trigger support.
# All existing func.Blueprint registrations remain fully compatible.
app = df.DFApp()


if _env_flag("ENABLE_ETL_FUNCTIONS", True):
    from functions.ava_refresh import bp as ava_bp
    from functions.bamboohr_sync import bp as bamboohr_bp
    from functions.bronze_nexudus import bp as bronze_bp
    from functions.finance_dashboard_refresh import bp as finance_dashboard_bp
    from functions.finance_invoice_worklist_refresh import bp as finance_worklist_refresh_bp
    from functions.nexudus_invoice_pdf_cache import bp as nexudus_pdf_bp
    from functions.nexudus_invoice_reconcile import bp as nexudus_invoice_reconcile_bp
    from functions.nexudus_silver_reconcile import bp as nexudus_silver_reconcile_bp
    from functions.silver_nexudus import bp as silver_bp
    from functions.silver_worker import bp as silver_worker_bp
    from functions.replyio_sync import bp as replyio_bp
    from functions.sync_health_report import bp as sync_health_report_bp
    from functions.xero_sync import bp as xero_sync_bp
    # Phase 3 (2026-05-28): monthly cron that freezes prior month's landlord
    # occupancy into silver.landlord_frozen_monthly_occupancy.
    from functions.landlord_freeze_monthly_occupancy import bp as landlord_freeze_bp
    # 15-min cron that materializes the slow gold.vw_landlord_*_monthly views
    # into tables so the dashboard reads stay sub-second on big locations.
    from functions.landlord_materialize_dashboard import bp as landlord_materialize_bp

    app.register_functions(bronze_bp)
    app.register_functions(silver_bp)
    app.register_functions(silver_worker_bp)
    app.register_functions(ava_bp)
    app.register_functions(nexudus_pdf_bp)
    app.register_functions(nexudus_invoice_reconcile_bp)
    app.register_functions(nexudus_silver_reconcile_bp)
    app.register_functions(xero_sync_bp)
    app.register_functions(bamboohr_bp)
    app.register_functions(finance_dashboard_bp)
    app.register_functions(finance_worklist_refresh_bp)
    app.register_functions(replyio_bp)
    app.register_functions(sync_health_report_bp)
    app.register_functions(landlord_freeze_bp)
    app.register_functions(landlord_materialize_bp)


if _env_flag("ENABLE_ADMIN_FUNCTIONS", False):
    from functions.admin_health import bp as admin_health_bp
    from functions.integrations_admin import bp as integrations_bp

    app.register_functions(admin_health_bp)
    app.register_functions(integrations_bp)


if _env_flag("ENABLE_REAL_ESTATE_FUNCTIONS", False):
    from functions.real_estate_costar import bp as real_estate_costar_bp
    from functions.real_estate_costar_worker import bp as real_estate_costar_worker_bp

    app.register_functions(real_estate_costar_bp)
    app.register_functions(real_estate_costar_worker_bp)


if _env_flag("ENABLE_LOCATION_SCRAPER_FUNCTIONS", False):
    from functions.location_scraper import bp as location_scraper_bp

    app.register_functions(location_scraper_bp)


# Daily Firebase competence_new -> bronze -> silver sync (TeamAndy competitors).
# Gated separately (default off) so the daily timer only runs once
# FIREBASE_CREDENTIALS is configured — otherwise it would fail every day.
if _env_flag("ENABLE_COMPETENCE_FUNCTIONS", False):
    from functions.competence_sync import bp as competence_bp

    app.register_functions(competence_bp)


# Nightly flexible-workspace classification of the synced competitors (LLM, deduped by domain).
# Separate flag (default off): it spends on the Anthropic API, so it stays off until the one-off
# backfill is done and ANTHROPIC_API_KEY is configured.
if _env_flag("ENABLE_COMPETENCE_CLASSIFY", False):
    from functions.competence_classification import bp as competence_classify_bp

    app.register_functions(competence_classify_bp)


# Daily HubSpot marketing emails -> bronze -> silver sync (content + KPI stats).
# Gated separately (default off) so the daily timer only runs once
# HUBSPOT_ACCESS_TOKEN is configured — otherwise it would fail every day.
if _env_flag("ENABLE_HUBSPOT_FUNCTIONS", False):
    from functions.hubspot_sync import bp as hubspot_bp

    app.register_functions(hubspot_bp)


# Daily Eventbrite events -> bronze -> silver sync (all orgs, all statuses).
# Gated separately (default off) so the daily timer only runs once
# EVENTBRITE_PRIVATE_TOKEN is configured — otherwise it would fail every day.
if _env_flag("ENABLE_EVENTBRITE_FUNCTIONS", False):
    from functions.eventbrite_sync import bp as eventbrite_bp

    app.register_functions(eventbrite_bp)
