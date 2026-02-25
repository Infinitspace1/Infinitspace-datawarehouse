"""
function_app.py

Main entry point for the Azure Function App.
Registers all function blueprints.
"""
from dotenv import load_dotenv

load_dotenv()

import azure.functions as func

from functions.bronze_nexudus import bp as bronze_bp
from functions.silver_nexudus import bp as silver_bp

app = func.FunctionApp()
app.register_functions(bronze_bp)
app.register_functions(silver_bp)

@app.route(route="test-connections", auth_level=func.AuthLevel.ADMIN)
async def test_connections(req: func.HttpRequest) -> func.HttpResponse:
    import os
    results = []

    # Test Nexudus
    try:
        from shared.nexudus.auth import get_bearer_token
        token = get_bearer_token()
        results.append(f"✅ Nexudus: token obtained ({token[:10]}...)")
    except Exception as e:
        results.append(f"❌ Nexudus: {e}")

    # Test SQL
    try:
        from shared.azure_clients.sql_client import get_sql_client
        sql = get_sql_client()
        version = sql.execute_scalar("SELECT @@VERSION")
        results.append(f"✅ SQL: {str(version)[:80]}")
    except Exception as e:
        results.append(f"❌ SQL: {e}")

    # Show env vars (existence only, not values)
    env_vars = ["NEXUDUS_USERNAME", "NEXUDUS_PASSWORD", "AZURE_SQL_CONNECTION_STRING"]
    for var in env_vars:
        status = "✅ SET" if os.getenv(var) else "❌ MISSING"
        results.append(f"  {var}: {status}")

    return func.HttpResponse("\n".join(results), mimetype="text/plain")