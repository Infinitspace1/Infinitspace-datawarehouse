"""
functions/admin_health.py

Optional admin-only health and smoke-test routes.
These routes are registered only when ENABLE_ADMIN_FUNCTIONS=1.
"""
from __future__ import annotations

import os

import azure.functions as func

bp = func.Blueprint()


@bp.route(route="test-connections", auth_level=func.AuthLevel.ADMIN)
async def test_connections(req: func.HttpRequest) -> func.HttpResponse:
    results: list[str] = []

    try:
        from shared.nexudus.auth import get_bearer_token

        token = get_bearer_token()
        results.append(f"OK Nexudus: token obtained ({token[:10]}...)")
    except Exception as exc:
        results.append(f"ERROR Nexudus: {exc}")

    try:
        from shared.azure_clients.sql_client import get_sql_client

        sql = get_sql_client()
        version = sql.execute_scalar("SELECT @@VERSION")
        results.append(f"OK SQL: {str(version)[:80]}")
    except Exception as exc:
        results.append(f"ERROR SQL: {exc}")

    for env_name in ["NEXUDUS_USERNAME", "NEXUDUS_PASSWORD", "AZURE_SQL_CONNECTION_STRING"]:
        status = "SET" if os.getenv(env_name) else "MISSING"
        results.append(f"{env_name}: {status}")

    return func.HttpResponse("\n".join(results), mimetype="text/plain")
