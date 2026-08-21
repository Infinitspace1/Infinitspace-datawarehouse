"""Test that the sql-agent-reader service principal can authenticate to Azure SQL.

Keeps the secret out of the repo and out of any transcript: pass it via env var.

    PowerShell:
        $env:AGENT_SP_SECRET = "<the client secret Value>"
        .\venv\Scripts\python.exe scripts\python_scripts\test_agent_sp_login.py

Separates two failure modes:
  * fails here  -> the problem is SQL-side (user, SID, or Entra config)
  * works here  -> the problem is the Power Platform connection config
"""
import os
import sys

import pyodbc

SERVER = "infinitspace-prod-northeurope-sqlsrv.database.windows.net"
DATABASE = "infinitspace-prod-main-db"
CLIENT_ID = "38dab0a7-76c8-4a2d-9220-6ac628942f1a"

secret = os.environ.get("AGENT_SP_SECRET")
if not secret:
    sys.exit("Set AGENT_SP_SECRET to the client secret Value first.")

conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server=tcp:{SERVER},1433;"
    f"Database={DATABASE};"
    f"UID={CLIENT_ID};"
    f"PWD={secret};"
    "Authentication=ActiveDirectoryServicePrincipal;"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

print(f"Connecting to {DATABASE} as service principal {CLIENT_ID} ...")
try:
    with pyodbc.connect(conn_str) as cnxn:
        cur = cnxn.cursor()
        cur.execute("SELECT USER_NAME() AS db_user, DB_NAME() AS db")
        row = cur.fetchone()
        print(f"  CONNECTED. db_user={row.db_user!r} database={row.db!r}")
        if row.db_user != "sql-agent-reader":
            print(f"  WARNING: expected db_user 'sql-agent-reader', got {row.db_user!r}")

        cur.execute("SELECT COUNT(*) FROM agent.vw_catalog")
        print(f"  agent.vw_catalog readable: {cur.fetchone()[0]} rows")

        cur.execute("SELECT TOP 3 location_name, city FROM agent.vw_nexudus_locations")
        for r in cur.fetchall():
            print(f"    {r.location_name} / {r.city}")

        try:
            cur.execute("SELECT TOP 1 * FROM ava.messages")
            print("  !! LEAK: ava.messages was readable")
        except pyodbc.Error:
            print("  ava.messages correctly denied")

    print("\nRESULT: SQL side is fine. The 401 is the Power Platform connection config.")
except pyodbc.Error as exc:
    print(f"  FAILED: {exc}")
    print("\nRESULT: SQL side is the problem, not the connector. Details above.")
