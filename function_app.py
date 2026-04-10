"""
function_app.py

Azure Functions entry point.

Deployment model:
  - Default publish: ETL-only surface (timers + queue worker).
  - Optional admin publish: enable admin/debug HTTP routes with app settings.
  - Optional real-estate publish: enable Real Estate HTTP API routes.

App settings:
  - ENABLE_ETL_FUNCTIONS=1          default
  - ENABLE_ADMIN_FUNCTIONS=0        default
  - ENABLE_REAL_ESTATE_FUNCTIONS=0  default
"""
from __future__ import annotations

import os

import azure.functions as func
from dotenv import load_dotenv

load_dotenv()


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


app = func.FunctionApp()


if _env_flag("ENABLE_ETL_FUNCTIONS", True):
    from functions.ava_refresh import bp as ava_bp
    from functions.bamboohr_sync import bp as bamboohr_bp
    from functions.bronze_nexudus import bp as bronze_bp
    from functions.finance_dashboard_refresh import bp as finance_dashboard_bp
    from functions.silver_nexudus import bp as silver_bp
    from functions.silver_worker import bp as silver_worker_bp
    from functions.xero_sync import bp as xero_sync_bp

    app.register_functions(bronze_bp)
    app.register_functions(silver_bp)
    app.register_functions(silver_worker_bp)
    app.register_functions(ava_bp)
    app.register_functions(xero_sync_bp)
    app.register_functions(bamboohr_bp)
    app.register_functions(finance_dashboard_bp)


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
