# CLAUDE.md -- InfinitSpace Data Warehouse

> Self-updating protocol: any agent that changes this project must update this file before finishing.

---

## Project Overview

InfinitSpace Data Warehouse is a Python 3.11 Azure Functions ETL project that moves operational data into Azure SQL across these layers:

- `bronze`: raw source payloads
- `silver`: typed and normalized entities
- `ava`: denormalized product availability
- `core`: planned, not implemented

Primary sources today:

- Nexudus API
- Xero API
- Google Maps enrichment utilities exist but are not part of the scheduled Function App

Platform:

- Azure Functions
- Azure SQL
- Azure Blob Storage for raw Nexudus snapshots
- Azure Storage Queue for silver fanout

Current deployment docs target:

- resource group: `infinitspace-prod-northeurope-data-rg`
- ETL app: `func-infinitspace-datawarehouse`
- optional admin app: `func-infinitspace-datawarehouse-admin`
- storage account: `staccinfinitspaceprod001`

---

## Runtime Topology

Default ETL execution order in UTC:

1. `02:00` `nexudus_to_bronze`
2. `02:30` `bronze_to_silver`
3. queue fanout via `silver_entity_worker`
4. `03:00` `refresh_ava_availability`
5. `04:00` `xero_invoice_sync`

Important operational caveat:

- `bronze_to_silver` is schedule-based, not dependency-aware
- `refresh_ava_availability` is also schedule-based
- bronze should finish before silver starts
- silver workers should finish before AVA starts

Flow:

```text
Nexudus API
  -> nexudus_to_bronze
  -> bronze.nexudus_locations
  -> bronze.nexudus_products
  -> bronze.nexudus_contracts
  -> bronze.nexudus_resources
  -> bronze.nexudus_extra_services
  -> blob snapshots

bronze_to_silver
  -> Azure Storage Queue: silver-sync-tasks
  -> silver_entity_worker x 5
  -> silver.nexudus_locations
  -> silver.nexudus_location_hours
  -> silver.nexudus_products
  -> silver.nexudus_contracts
  -> silver.nexudus_resources
  -> silver.nexudus_extra_services

refresh_ava_availability
  -> EXEC ava.sp_refresh_product_availability
  -> ava.product_availability

Xero API
  -> xero_invoice_sync
  -> bronze.xero_invoices
  -> silver.xero_invoices
  -> silver.xero_invoice_line_items
  -> optional bronze.xero_invoice_pdfs
```

---

## Function App Registration Model

`function_app.py` registers functions based on app settings:

- `ENABLE_ETL_FUNCTIONS=1`
  - registers the production ETL surface
- `ENABLE_ADMIN_FUNCTIONS=1`
  - registers optional admin/debug HTTP routes

Default ETL deployment:

- `ENABLE_ETL_FUNCTIONS=1`
- `ENABLE_ADMIN_FUNCTIONS=0`

Optional admin deployment:

- `ENABLE_ETL_FUNCTIONS=0`
- `ENABLE_ADMIN_FUNCTIONS=1`

This means the default Azure Function App should show only:

- `nexudus_to_bronze`
- `bronze_to_silver`
- `silver_entity_worker`
- `refresh_ava_availability`
- `xero_invoice_sync`

---

## Repository Structure

```text
Infinitspace-datawarehouse/
  CLAUDE.md
  README.md
  SQL_datawarehouse.md
  function_app.py
  host.json
  requirements.txt
  .env.example
  .funcignore
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
      sql_client.py
      bronze_writer.py
      blob_writer.py
      queue_client.py
      run_tracker.py
      silver_write_locations.py
      silver_writer_products.py
      silver_writer_contracts.py
      silver_writer_resources.py
      silver_writer_extra_services.py
    nexudus/
      auth.py
      client.py
      colleague_sync.py
      schemas.py
      transformers/
        locations.py
        products.py
        contracts.py
        resources.py
        extra_services.py
    xero/
      oauth.py
      flow.py
      token_cipher.py
      store.py
      client.py
      invoice_sync.py
    gmaps/
  scripts/
    python_scripts/
      test_local.py
      test_locations_silver.py
      test_products_silver.py
      test_contracts_silver.py
      test_extra_services_silver.py
      inspect_bronze.py
      inspect_product_per_type.py
      enrich_location_gmaps.py
      nexudus_debug_coworker.py
      nexudus_sync_colleague_access.py
      xero_start_oauth.py
      xero_complete_oauth.py
      xero_get_connections.py
      xero_list_tenants.py
      xero_sync_invoices.py
      xero_list_invoices.py
      xero_download_invoice_pdf.py
      xero_test_contacts.py
      xero_test_invoices.py
      xero_open_auth.py
      xero_exchange_code.py
      xero_refresh_token.py
      xero_register_connection.py
    sql_scripts/
      bronze_layer.sql
      bronze_upsert_constraints.sql
      silver_nexudus_locations_schema.sql
      silver_nexudus_products_schema.sql
      silver_nexudus_contracts_schema.sql
      silver_nexudus_resources_schema.sql
      silver_nexudus_extra_services_schema.sql
      silver_gmaps_locations_schema.sql
      ava_product_availability_schema.sql
      ava_sp_refresh_product_availability.sql
      integrations_nexudus_xero_schema.sql
      xero_invoices_schema.sql
      test.sql
  tests/
    test_nexudus_colleague_sync.py
    test_xero_integration.py
  docs/
    deploy.md
    silver_table_relationships.md
  deploy/
    setup_azure_resources.ps1
    setup_azure_resources.sh
```

Legacy Xero helper scripts still exist, but the supported path is now:

- `xero_start_oauth.py`
- `xero_complete_oauth.py`
- DB-backed refresh inside `shared/xero/client.py`

---

## Azure Functions Registry

| Function | File | Trigger | Default schedule or binding | Notes |
|----------|------|---------|-----------------------------|-------|
| `nexudus_to_bronze` | `functions/bronze_nexudus.py` | timer | `0 0 2 * * *` | writes bronze + blob snapshots |
| `bronze_to_silver` | `functions/silver_nexudus.py` | timer | `0 30 2 * * *` | enqueues 5 queue messages |
| `silver_entity_worker` | `functions/silver_worker.py` | queue | `silver-sync-tasks` | one entity per invocation |
| `refresh_ava_availability` | `functions/ava_refresh.py` | timer | `0 0 3 * * *` | executes AVA stored procedure |
| `xero_invoice_sync` | `functions/xero_sync.py` | timer | `0 0 4 * * *` | syncs all linked Xero tenants |
| admin HTTP routes | `functions/integrations_admin.py` | HTTP | on-demand | only when `ENABLE_ADMIN_FUNCTIONS=1` |
| `test_connections` | `functions/admin_health.py` | HTTP | on-demand | only when `ENABLE_ADMIN_FUNCTIONS=1` |

---

## Data Model Summary

### Bronze

- `bronze.nexudus_locations`
- `bronze.nexudus_products`
- `bronze.nexudus_contracts`
- `bronze.nexudus_resources`
- `bronze.nexudus_extra_services`
- `bronze.nexudus_coworkers`
- `bronze.xero_invoices`
- `bronze.xero_invoice_pdfs`

Nexudus bronze rows are latest-payload upserts on `source_id`, not append-only history.

### Silver

- `silver.nexudus_locations`
- `silver.nexudus_location_hours`
- `silver.nexudus_products`
- `silver.nexudus_contracts`
- `silver.nexudus_resources`
- `silver.nexudus_extra_services`
- `silver.nexudus_colleagues`
- `silver.nexudus_colleague_location_access`
- `silver.xero_invoices`
- `silver.xero_invoice_line_items`
- `silver.location_nearby_pois`
- `silver.location_transit_stations`
- `silver.location_neighborhoods`

### AVA

- `ava.product_availability`
  - rebuilt daily
  - populated by stored procedure
  - no incremental logic

### Meta

- `meta.sync_runs`
- `meta.sync_errors`
- `meta.gmaps_enrichment_log`
- `meta.xero_oauth_states`
- `meta.xero_connections`
- `meta.xero_tenants`

---

## Key Technical Behaviors

### Nexudus Bronze

- `functions/bronze_nexudus.py`
- fetch order:
  - locations
  - products
  - contracts
  - resources
  - extra_services
- each entity writes a `RunTracker` row
- each entity also writes a blob snapshot

### Silver Fanout

- `functions/silver_nexudus.py` only enqueues work
- `functions/silver_worker.py` performs the actual transformation
- queue retries are safe because silver writes are idempotent upserts
- poison queue: `silver-sync-tasks-poison`

### AVA Refresh

- `functions/ava_refresh.py`
- runs `EXEC ava.sp_refresh_product_availability`
- logs before and after row count

### Xero OAuth and Sync

- `shared/xero/flow.py`
  - `start_auth()`
  - `handle_callback()`
- `shared/xero/client.py`
  - auto-refreshes tokens near expiry
  - marks connection disconnected on `invalid_grant`
- `shared/xero/invoice_sync.py`
  - incremental by tenant using `If-Modified-Since`
  - updates `meta.xero_tenants` watermarks
  - does not use `RunTracker`

---

## Logging and Operational Expectations

### RunTracker-backed functions

`RunTracker` writes to `meta.sync_runs` for:

- Nexudus bronze entity runs
- Nexudus silver worker entity runs
- AVA refresh

Expected SQL status fields:

- `status`
- `started_at`
- `finished_at`
- `rows_read`
- `rows_written`
- `rows_skipped`
- `error_message`

### Expected Nexudus logs

- `Nexudus -> Bronze sync started`
- `Locations: X fetched, Y written to bronze`
- `Products: X fetched, Y written to bronze`
- `Contracts: X fetched, Y written to bronze`
- `Resources: X attempted, Y written, Z skipped`
- `Extra services: X fetched, Y written to bronze`
- `Nexudus -> Bronze sync complete`

### Expected silver logs

- `Bronze -> Silver orchestrator started`
- `Bronze -> Silver: 5 tasks enqueued`
- `Silver worker received: entity=... dequeue_count=...`
- `Silver worker complete: entity=... result=...`

### Expected AVA logs

- `AVA refresh started`
- `AVA refresh complete: before rows -> after rows`

### Expected Xero logs

- `Xero invoice sync started`
- `Fetching Xero invoices page`
- `Writing Xero invoices page`
- `Xero invoice sync complete`
- possible warning: `Some tenants failed during Xero sync`

---

## Environment Variables

```bash
# Nexudus
NEXUDUS_USERNAME=...
NEXUDUS_PASSWORD=...
NEXUDUS_BEARER_TOKEN=...

# Azure SQL
AZURE_SQL_CONNECTION_STRING=...
AZURE_SQL_SERVER=...
AZURE_SQL_DATABASE=...
AZURE_SQL_USERNAME=...
AZURE_SQL_PASSWORD=...
AZURE_SQL_DRIVER="ODBC Driver 18 for SQL Server"
AZURE_SQL_CONNECTION_TIMEOUT=60
AZURE_SQL_TRUST_SERVER_CERTIFICATE=false

# Blob storage
AZURE_STORAGE_ACCOUNT_NAME=staccinfinitspaceprod001
AZURE_STORAGE_CONTAINER_RAW_NEXUDUS=nexudus-raw-snapshots

# Queue trigger storage
AzureWebJobsStorage=...

# Google Maps
GOOGLE_MAPS_API_KEY=...

# Xero
XERO_CLIENT_ID=...
XERO_CLIENT_SECRET=...
XERO_REDIRECT_URI=https://...
XERO_POST_AUTH_REDIRECT_URI=...
XERO_SCOPES="offline_access accounting.invoices accounting.payments ..."
INTEGRATIONS_ENCRYPTION_KEY=...

# Function registration
ENABLE_ETL_FUNCTIONS=1
ENABLE_ADMIN_FUNCTIONS=0

# Schedule overrides
NEXUDUS_SYNC_SCHEDULE="0 0 2 * * *"
SILVER_SYNC_SCHEDULE="0 30 2 * * *"
AVA_REFRESH_SCHEDULE="0 0 3 * * *"
XERO_INVOICE_SYNC_SCHEDULE="0 0 4 * * *"
XERO_INVOICE_SYNC_FORCE_FULL=0
```

---

## Local Validation

Recommended order:

```powershell
.\venv\Scripts\python.exe scripts\python_scripts\test_local.py --step auth
.\venv\Scripts\python.exe scripts\python_scripts\test_local.py --step sql
.\venv\Scripts\python.exe scripts\python_scripts\test_local.py --step all --dry-run --limit 20
.\venv\Scripts\python.exe scripts\python_scripts\test_local.py --step all --limit 50
.\venv\Scripts\python.exe scripts\python_scripts\test_locations_silver.py --write
.\venv\Scripts\python.exe scripts\python_scripts\test_products_silver.py --write
.\venv\Scripts\python.exe scripts\python_scripts\test_contracts_silver.py --write
.\venv\Scripts\python.exe scripts\python_scripts\test_extra_services_silver.py --write
```

Xero validation:

```powershell
.\venv\Scripts\python.exe scripts\python_scripts\xero_start_oauth.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_complete_oauth.py --redirect-url "<full redirect url>"
.\venv\Scripts\python.exe scripts\python_scripts\xero_get_connections.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_sync_invoices.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_list_invoices.py --owner-type workspace --owner-id default --top 20
```

Explicit refresh verification:

1. set `meta.xero_connections.expires_at` into the past
2. run `xero_get_connections.py`
3. verify `expires_at` moved forward and `is_connected = 1`

---

## SQL Validation Queries

```sql
SELECT TOP 20
    source_name, entity, layer, status,
    started_at, finished_at,
    rows_read, rows_written, rows_skipped, error_message
FROM meta.sync_runs
ORDER BY started_at DESC;

SELECT TOP 20 * FROM meta.sync_errors ORDER BY created_at DESC;

SELECT id, owner_type, owner_id, is_connected, last_error, expires_at, updated_at
FROM meta.xero_connections
ORDER BY updated_at DESC;

SELECT
    tenant_name,
    last_invoice_sync_started_at,
    last_invoice_sync_completed_at,
    last_invoice_sync_error,
    last_invoice_modified_utc
FROM meta.xero_tenants
ORDER BY tenant_name;
```

---

## Deployment Notes

ETL app:

```powershell
func azure functionapp publish func-infinitspace-datawarehouse --python

az functionapp config appsettings set `
  --resource-group infinitspace-prod-northeurope-data-rg `
  --name func-infinitspace-datawarehouse `
  --settings `
    ENABLE_ETL_FUNCTIONS=1 `
    ENABLE_ADMIN_FUNCTIONS=0
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

---

## Current Status

| Feature | Status | Notes |
|---------|--------|-------|
| Nexudus bronze sync | done | 5 entities |
| Nexudus silver fanout | done | queue-based |
| AVA refresh | done | stored procedure rebuild |
| Xero OAuth + tenant storage | done | DB-backed |
| Xero auto-refresh | done | disconnects on `invalid_grant` |
| Xero invoice sync | done | incremental by tenant |
| Optional admin HTTP routes | done | separate deployment mode |
| Google Maps scheduled pipeline | not wired | utilities exist, not registered in default app |
| Core layer population | planned | not implemented |

---

## Self-Update Rules

After any material project change:

1. update repository structure if files moved or were added
2. update function registry if triggers changed
3. update env vars if configuration changed
4. update runtime topology if schedules or dependencies changed
5. update validation steps if the recommended test flow changed
6. update current status if a feature moved from planned to done or vice versa
7. always update the date at the bottom

---

Last updated: 2026-04-07
Current branch: `main`
Maintainer: InfinitSpace Data Engineering Team
