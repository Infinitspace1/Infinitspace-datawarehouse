# InfinitSpace Data Warehouse

Production ETL repository for the InfinitSpace data platform.

This project runs scheduled Azure Functions that ingest operational data into Azure SQL across a lakehouse-style flow:

- `bronze`: raw source payloads
- `silver`: typed and normalized entities
- `ava`: denormalized product availability for downstream use
- `gold`: production tables for downstream applications

## Current Pipelines

### Nexudus

- `nexudus_to_bronze`
  - Timer trigger
  - Default schedule: `02:00 UTC`
  - Fetches `locations`, `products`, `contracts`, `coworker_invoices`, `coworkers`, `resources`, `extra_services`
  - Writes raw rows to `bronze.nexudus_*`
  - Writes JSON snapshots to blob storage

- `bronze_to_silver`
  - Timer trigger
  - Default schedule: `02:30 UTC`
  - Enqueues one queue message per entity

- `silver_entity_worker`
  - Queue trigger
  - Processes one silver entity per message
  - Writes to `silver.nexudus_*`

- `refresh_ava_availability`
  - Timer trigger
  - Default schedule: `03:00 UTC`
  - Runs `ava.sp_refresh_product_availability`
  - Rebuilds `ava.product_availability`

### Xero

- `xero_invoice_sync`
  - Timer trigger
  - Default schedule: `04:00 UTC`
  - Reads all stored Xero tenants for the default connection
  - Writes raw accounts to `bronze.xero_accounts`
  - Writes raw invoices to `bronze.xero_invoices`
  - Writes typed accounts to `silver.xero_accounts`
  - Writes typed invoices to `silver.xero_invoices`
  - Writes line items to `silver.xero_invoice_line_items`
  - Refreshes the Xero tenant directory in `silver.xero_tenants`
  - Exposes the same directory through `xero.silver_tenants`
  - Optionally caches PDFs in `bronze.xero_invoice_pdfs`

- `refresh_finance_dashboard`
  - Timer trigger
  - Default schedule: `05:30 UTC`
  - Runs `gold.sp_refresh_finance_dashboard`
  - Rebuilds `gold.finance_dashboard_invoice_worklist`
  - Rebuilds `gold.finance_dashboard_user_access`

### Gold Production Tables

- `gold.finance_dashboard_user_access`
  - Maps BambooHR CM / ACM users to accessible Nexudus locations and Xero tenants
  - Encodes the Amsterdam shared-access rule for Republica, Herengracht, and Zuidtoren

- `gold.finance_dashboard_invoice_worklist`
  - Exposes unpaid Nexudus-backed Xero invoices for the finance dashboard website
  - Resolves company email from Nexudus billing/customer data when available
  - Classifies invoices into `recurrent` or `one_off`
  - Uses synced Xero account metadata, with manual overrides in `meta.finance_dashboard_xero_account_map`

## Function App Model

The repo now supports two deployment modes from the same codebase:

- ETL app, default
  - `ENABLE_ETL_FUNCTIONS=1`
  - `ENABLE_ADMIN_FUNCTIONS=0`
  - Exposes only the production ETL functions

- Admin app, optional
  - `ENABLE_ETL_FUNCTIONS=0`
  - `ENABLE_ADMIN_FUNCTIONS=1`
  - Exposes manual HTTP routes for Xero OAuth callback, debugging, and smoke tests

`function_app.py` registers functions based on those flags.

## Repository Structure

```text
Infinitspace-datawarehouse/
  function_app.py
  host.json
  requirements.txt
  README.md
  CLAUDE.md
  SQL_datawarehouse.md
  .env.example
  functions/
    bronze_nexudus.py
    silver_nexudus.py
    silver_worker.py
    ava_refresh.py
    xero_sync.py
    integrations_admin.py
    admin_health.py
  shared/
    azure_clients/
    nexudus/
    xero/
      tenant_directory.py
    gmaps/
  scripts/
    python_scripts/
    sql_scripts/
  tests/
    test_xero_tenant_directory.py
  docs/
  deploy/
```

## Local Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Minimum local configuration:

- `NEXUDUS_USERNAME`
- `NEXUDUS_PASSWORD`
- `AZURE_SQL_CONNECTION_STRING`
- `AZURE_STORAGE_ACCOUNT_NAME`
- `AZURE_STORAGE_CONTAINER_RAW_NEXUDUS`
- `XERO_CLIENT_ID`
- `XERO_CLIENT_SECRET`
- `XERO_REDIRECT_URI`
- `XERO_SCOPES`
- `INTEGRATIONS_ENCRYPTION_KEY`

For local queue-trigger testing, also set `AzureWebJobsStorage` in `local.settings.json`.

## Local Testing

### Nexudus Bronze

```powershell
.\venv\Scripts\python.exe scripts\python_scripts\test_local.py --step auth
.\venv\Scripts\python.exe scripts\python_scripts\test_local.py --step sql
.\venv\Scripts\python.exe scripts\python_scripts\test_local.py --step all --dry-run --limit 20
.\venv\Scripts\python.exe scripts\python_scripts\test_local.py --step all --limit 50
.\venv\Scripts\python.exe scripts\python_scripts\sync_nexudus_billing.py --dry-run --limit 50
.\venv\Scripts\python.exe scripts\python_scripts\sync_nexudus_billing.py
.\venv\Scripts\python.exe scripts\python_scripts\xero_nexudus_link_audit.py --limit 100 --show-unmatched
```

### Nexudus Silver

```powershell
.\venv\Scripts\python.exe scripts\python_scripts\test_locations_silver.py --write
.\venv\Scripts\python.exe scripts\python_scripts\test_products_silver.py --write
.\venv\Scripts\python.exe scripts\python_scripts\test_contracts_silver.py --write
.\venv\Scripts\python.exe scripts\python_scripts\test_extra_services_silver.py --write
```

### Xero

```powershell
.\venv\Scripts\python.exe scripts\python_scripts\xero_start_oauth.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_complete_oauth.py --redirect-url "<full redirect url>"
.\venv\Scripts\python.exe scripts\python_scripts\xero_list_tenants.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_get_connections.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_sync_invoices.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_list_invoices.py --owner-type workspace --owner-id default --top 20
```

### Unit Tests

```powershell
.\venv\Scripts\python.exe -m unittest tests.test_xero_integration tests.test_xero_tenant_directory tests.test_xero_nexudus_invoice_linking
```

## Xero Auth and Refresh

The supported production path is DB-backed, not `.env` refresh-token rotation:

- OAuth state is stored in `meta.xero_oauth_states`
- Encrypted tokens are stored in `meta.xero_connections`
- Tenant metadata and sync watermarks are stored in `meta.xero_tenants`
- Account metadata is stored in `silver.xero_accounts`
- Tenant-to-location directory rows are stored in `silver.xero_tenants`
- `xero.silver_tenants` is a view alias for the same directory
- Automatic refresh happens in `shared/xero/client.py`
- If Xero returns `invalid_grant`, the connection is marked disconnected

Recommended Xero scopes now include `accounting.settings.read` so the sync can read account names for finance-dashboard classification. Existing connections without that scope will continue syncing invoices, but account sync will be skipped until the app is re-authorized.

Recommended verification:

1. Force `meta.xero_connections.expires_at` into the past
2. Run `xero_get_connections.py`
3. Confirm `expires_at` moved forward and `is_connected = 1`

## Azure Deployment

The current deployment docs target:

- resource group: `infinitspace-prod-northeurope-data-rg`
- ETL app: `func-infinitspace-datawarehouse`
- optional admin app: `func-infinitspace-datawarehouse-admin`
- storage account: `staccinfinitspaceprod001`

Deploy the ETL app:

```powershell
func azure functionapp publish func-infinitspace-datawarehouse --python

az functionapp config appsettings set `
  --resource-group infinitspace-prod-northeurope-data-rg `
  --name func-infinitspace-datawarehouse `
  --settings `
    ENABLE_ETL_FUNCTIONS=1 `
    ENABLE_ADMIN_FUNCTIONS=0 `
    AZURE_STORAGE_ACCOUNT_NAME=staccinfinitspaceprod001 `
    AZURE_STORAGE_CONTAINER_RAW_NEXUDUS=nexudus-raw-snapshots
```

Optional admin app:

```powershell
func azure functionapp publish func-infinitspace-datawarehouse-admin --python

az functionapp config appsettings set `
  --resource-group infinitspace-prod-northeurope-data-rg `
  --name func-infinitspace-datawarehouse-admin `
  --settings `
    ENABLE_ETL_FUNCTIONS=0 `
    ENABLE_ADMIN_FUNCTIONS=1
```

More detail: [docs/deploy.md](docs/deploy.md)

## Daily Runtime Expectations

Default UTC order:

1. `02:00` `nexudus_to_bronze`
2. `02:30` `bronze_to_silver`
3. queue fanout via `silver_entity_worker`
4. `03:00` `refresh_ava_availability`
5. `04:00` `xero_invoice_sync`
6. `05:00` `bamboohr_sync`
7. `05:30` `refresh_finance_dashboard`

Suggested reminder flow after this extension:

1. Nexudus bronze/silver lands `coworker_invoices` and `coworkers`
2. Xero sync refreshes `silver.xero_invoices`
3. Query `silver.xero_overdue_invoice_contacts` for reminder-ready rows

Important:

- `bronze_to_silver` is schedule-based, not dependency-aware
- `refresh_ava_availability` is also schedule-based
- operationally, bronze should finish before silver starts, and silver workers should finish before AVA starts

## What To Monitor

### SQL

```sql
SELECT TOP 20
    source_name, entity, layer, status,
    started_at, finished_at,
    rows_read, rows_written, rows_skipped, error_message
FROM meta.sync_runs
ORDER BY started_at DESC;

SELECT
    tenant_name,
    last_invoice_sync_started_at,
    last_invoice_sync_completed_at,
    last_invoice_sync_error,
    last_invoice_modified_utc
FROM meta.xero_tenants
ORDER BY tenant_name;

SELECT
    tenant_name,
    location_name,
    location_city,
    location_country_name,
    community_manager_name,
    location_match_rule
FROM xero.silver_tenants
ORDER BY tenant_name;
```

### Expected Nexudus Logs

- `Nexudus -> Bronze sync started`
- `Locations: X fetched, Y written to bronze`
- `Products: X fetched, Y written to bronze`
- `Contracts: X fetched, Y written to bronze`
- `Resources: X attempted, Y written, Z skipped`
- `Extra services: X fetched, Y written to bronze`
- `Nexudus -> Bronze sync complete`

### Expected Silver Logs

- `Bronze -> Silver orchestrator started`
- `Bronze -> Silver: 5 tasks enqueued`
- `Silver worker received: entity=...`
- `Silver worker complete: entity=...`

### Expected AVA Logs

- `AVA refresh started`
- `AVA refresh complete: before -> after rows`

### Expected Xero Logs

- `Xero invoice sync started`
- `Fetching Xero invoices page`
- `Writing Xero invoices page`
- `Xero invoice sync complete`
- tenant directory refresh stats inside the final Xero sync payload

## Current Status

- Nexudus bronze pipeline: done
- Nexudus silver pipeline: done
- AVA refresh pipeline: done
- Xero OAuth, token refresh, tenant storage, and invoice sync: done
- Xero accounts sync: done
- Xero tenant-to-location directory: done
- Optional admin HTTP routes: done, but not part of the default ETL app
- Google Maps enrichment utilities: present, not part of the scheduled ETL app
- Gold finance dashboard tables: done

## Key Docs

- [CLAUDE.md](CLAUDE.md)
- [SQL_datawarehouse.md](SQL_datawarehouse.md)
- [docs/silver_table_relationships.md](docs/silver_table_relationships.md)
- [docs/deploy.md](docs/deploy.md)

Last updated: 2026-04-07
