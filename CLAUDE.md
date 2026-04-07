# CLAUDE.md -- InfinitSpace Data Warehouse

> **Self-Updating Protocol**: Every Claude instance that makes changes to this project MUST update this file before finishing the session. Update the relevant sections below and bump the `Last Updated` field at the bottom. This ensures the next instance starts with accurate context.

---

## Project Overview

**InfinitSpace Data Warehouse** is a production-grade ETL pipeline running on **Azure Functions** that ingests coworking space data from multiple sources into a three-tier lakehouse architecture (Bronze -> Silver -> Core) stored in **Azure SQL**, with raw data archived in **Azure Blob Storage**.

- **Language**: Python 3.11+
- **Platform**: Azure Functions (Consumption Plan)
- **Database**: Azure SQL Server (`infinitspace-prod-main-db`)
- **Data sources**: Nexudus API (primary), Xero API (invoicing), Google Maps Places API (enrichment)
- **Blob storage**: Azure Blob Storage (`staccinfinitspaceprod001`)
- **Resource group**: `infinitspace-datawarehouse-prod`
- **Function App name**: `func-infinitspace-datawarehouse`
- **Timeout**: 10 minutes (`host.json`)

---

## Architecture: Data Flow

```
Nexudus API  --[02:00 UTC]-->  Bronze (raw JSON, upserted on source_id)
                                     |
                             [02:30 UTC]
                                     v
                   bronze_to_silver() timer trigger
                   (enqueues 5 messages -- one per entity)
                                     |
                    Azure Storage Queue "silver-sync-tasks"
                     |         |          |         |         |
                [locations] [products] [contracts] [resources] [extra_services]
                 silver_entity_worker() x 5  (parallel, isolated)
                                     |
                                     v
                            Silver (typed, upserted on source_id)
                                     |
                             [03:00 UTC]
                                     v
                          AVA layer (denormalized, chatbot-ready)
                                     |
                             [Future]
                                     v
                             Core (canonical, multi-source)
                                     |
                                     v
                      Power BI / Ava Bot / Internal APIs


Xero API  --[04:00 UTC]-->  bronze.xero_invoices  -->  silver.xero_invoices
                                                         + silver.xero_invoice_line_items
                            bronze.xero_invoice_pdfs   (optional PDF caching)


Nexudus Coworkers  --[on-demand HTTP]-->  bronze.nexudus_coworkers
                                          silver.nexudus_colleagues
                                          silver.nexudus_colleague_location_access
```

**Bronze**: Raw JSON stored with metadata. Upserted on `source_id` (not append-only -- latest payload overwrites).
**Silver**: Cleaned, typed, normalized records. Upserted daily on `source_id`.
**AVA**: Denormalized, chatbot-ready flat table. TRUNCATE + rebuild daily via stored procedure.
**Core**: Source-agnostic canonical entities. **Not yet implemented**.

---

## Repository Structure

```
Infinitspace-datawarehouse/
|-- CLAUDE.md                          <-- This file (keep updated!)
|-- README.md                          <-- Human-facing project docs
|-- SQL_datawarehouse.md               <-- SQL architecture overview
|-- requirements.txt                   <-- Python dependencies
|-- function_app.py                    <-- Azure Functions entry point (registers blueprints)
|-- host.json                          <-- Azure Functions config (timeout: 10min)
|-- .env.example                       <-- Environment variable template
|-- .funcignore                        <-- Files excluded from Azure deployment
|-- .gitignore                         <-- WARNING: includes *.json (overly broad)
|
|-- functions/
|   |-- bronze_nexudus.py              <-- Timer: Nexudus -> Bronze (02:00 UTC)
|   |-- silver_nexudus.py              <-- Timer: enqueues 5 silver tasks (02:30 UTC)
|   |-- silver_worker.py               <-- Queue: Bronze -> Silver per entity (parallel)
|   |-- ava_refresh.py                 <-- Timer: Silver -> AVA layer (03:00 UTC)
|   |-- xero_sync.py                   <-- Timer: Xero invoice sync (04:00 UTC)
|   |-- integrations_admin.py          <-- HTTP: Admin endpoints (Nexudus colleague sync, Xero OAuth)
|
|-- shared/
|   |-- azure_clients/
|   |   |-- sql_client.py              <-- SQL connection manager (SQLClient + Database wrapper)
|   |   |-- bronze_writer.py           <-- Batch MERGE upsert to bronze.nexudus_* tables
|   |   |-- blob_writer.py             <-- Store raw snapshots in Azure Blob Storage
|   |   |-- queue_client.py            <-- Enqueue silver tasks to Azure Storage Queue
|   |   |-- run_tracker.py             <-- Context manager: logs to meta.sync_runs
|   |   |-- silver_write_locations.py  <-- Bronze -> silver.nexudus_locations + _hours
|   |   |-- silver_writer_products.py  <-- Bronze -> silver.nexudus_products
|   |   |-- silver_writer_contracts.py <-- Bronze -> silver.nexudus_contracts
|   |   |-- silver_writer_resources.py <-- Bronze -> silver.nexudus_resources
|   |   +-- silver_writer_extra_services.py <-- Bronze -> silver.nexudus_extra_services
|   |
|   |-- nexudus/
|   |   |-- auth.py                    <-- Bearer token auth (cached, 60s expiry buffer)
|   |   |-- client.py                  <-- Async API client (3 concurrent, retry, paginate)
|   |   |-- schemas.py                 <-- TypedDicts: ParsedCoworker, AccessibleBusiness
|   |   |-- colleague_sync.py          <-- Coworker access sync (bronze + silver + access mapping)
|   |   +-- transformers/
|   |       |-- locations.py           <-- transform_location() + transform_location_hours()
|   |       |-- products.py            <-- transform_product() (all 5 item types)
|   |       |-- contracts.py           <-- transform_contract()
|   |       |-- resources.py           <-- transform_resource()
|   |       +-- extra_services.py      <-- transform_extra_service()
|   |
|   |-- xero/
|   |   |-- __init__.py
|   |   |-- oauth.py                   <-- OAuth2 authorization code flow + token refresh
|   |   |-- flow.py                    <-- High-level auth orchestration (start_auth, handle_callback)
|   |   |-- token_cipher.py            <-- Fernet (AES) encryption for tokens at rest
|   |   |-- store.py                   <-- SQL persistence (connections, tenants, oauth states)
|   |   |-- client.py                  <-- Authenticated Xero API client (auto-refresh)
|   |   +-- invoice_sync.py            <-- Invoice sync service (bronze/silver/PDF)
|   |
neighborhoods
|
|-- scripts/
|   |-- python_scripts/
|   |   |-- test_local.py              <-- Full pipeline test (--step auth/sql/locations/all)
|   |   |-- test_locations_silver.py   <-- Silver location transformation test
|   |   |-- test_products_silver.py    <-- Silver product transformation test
|   |   |-- test_contracts_silver.py   <-- Silver contract transformation test
|   |   |-- test_extra_services_silver.py <-- Silver extra services transformation test
|   |   |-- inspect_bronze.py          <-- Query bronze tables (row counts, recent syncs)
|   |   |-- inspect_product_per_type.py <-- Breakdown of products by item_type
|   |   |-- enrich_location_gmaps.py   <-- Bulk Google Maps enrichment CLI
|   |   |-- nexudus_debug_coworker.py  <-- Debug single coworker payload
|   |   |-- nexudus_sync_colleague_access.py <-- CLI: sync specific coworker IDs
|   |   |-- xero_start_oauth.py        <-- Initiate Xero OAuth flow
|   |   |-- xero_complete_oauth.py     <-- Complete OAuth from copied redirect URL
|   |   |-- xero_get_connections.py    <-- List Xero connections
|   |   |-- xero_list_tenants.py       <-- List Xero tenants per connection
|   |   |-- xero_sync_invoices.py      <-- Trigger invoice sync manually
|   |   |-- xero_list_invoices.py      <-- Query synced invoices
|   |   |-- xero_download_invoice_pdf.py <-- Download & cache invoice PDF
|   |   |-- xero_test_contacts.py      <-- Fetch & count Xero contacts (smoke test)
|   |   |-- xero_test_invoices.py      <-- Fetch & validate invoices (smoke test)
|   |   |-- xero_open_auth.py          <-- DEAD: dev helper, superseded by xero_start_oauth.py
|   |   |-- xero_exchange_code.py      <-- DEAD: manual OAuth code exchange, superseded
|   |   |-- xero_refresh_token.py      <-- DEAD: manual token refresh, superseded by auto-refresh
|   |   +-- xero_register_connection.py <-- DEAD: manual connection registration, superseded
|   |
|   +-- sql_scripts/
|       |-- bronze_layer.sql                        <-- CREATE: all bronze.* + meta tables
|       |-- bronze_upsert_constraints.sql           <-- ADD: unique indexes on bronze tables
|       |-- silver_nexudus_locations_schema.sql      <-- CREATE: silver locations + hours
|       |-- silver_nexudus_products_schema.sql       <-- CREATE: silver products
|       |-- silver_nexudus_contracts_schema.sql      <-- CREATE: silver contracts
|       |-- silver_nexudus_resources_schema.sql      <-- CREATE: silver resources
|       |-- silver_nexudus_extra_services_schema.sql <-- CREATE: silver extra services
|       |-- silver_gmaps_locations_schema.sql        <-- CREATE: POI/transit/neighborhood tables
|       |-- ava_product_availability_schema.sql      <-- CREATE: ava schema + product_availability table
|       |-- ava_sp_refresh_product_availability.sql  <-- SP: TRUNCATE + rebuild from silver tables
|       |-- integrations_nexudus_xero_schema.sql     <-- CREATE: nexudus coworker + xero tables
|       |-- xero_invoices_schema.sql                 <-- CREATE: xero invoice + PDF cache tables
|       +-- test.sql                                 <-- Ad-hoc validation queries
|
|-- tests/
|   |-- test_nexudus_colleague_sync.py  <-- Unit tests: coworker parsing & sync
|   +-- test_xero_integration.py        <-- Unit tests: OAuth flow, token refresh, store
|
|-- docs/
|   |-- deploy.md
|   +-- silver_table_relationships.md
|
|-- deploy/
|   |-- setup_azure_resources.sh
|   +-- setup_azure_resources.ps1
|
+-- membership_agreement_test/         <-- DEAD: experimental PDF parsing, not part of pipeline
    |-- compute_notice_period.py
    |-- count_pages.py
    |-- extract.py
    +-- pdfs/                          <-- ~80 PDF files (should not be in repo)
```

---

## Azure Functions Registry

Functions are registered in `function_app.py` via blueprints and gated by app settings:
- `ENABLE_ETL_FUNCTIONS=1` registers the production ETL timers/queue worker
- `ENABLE_ADMIN_FUNCTIONS=1` registers the optional admin/debug HTTP routes

| Function | File | Trigger | Schedule / Binding | Registered |
|----------|------|---------|-------------------|------------|
| `nexudus_to_bronze` | `functions/bronze_nexudus.py` | Timer | `0 0 2 * * *` (02:00 UTC) | YES |
| `bronze_to_silver` | `functions/silver_nexudus.py` | Timer | `0 30 2 * * *` (02:30 UTC) | YES |
| `silver_entity_worker` | `functions/silver_worker.py` | Queue | `silver-sync-tasks` | YES |
| `refresh_ava_availability` | `functions/ava_refresh.py` | Timer | `0 0 3 * * *` (03:00 UTC) | YES |
| `xero_invoice_sync` | `functions/xero_sync.py` | Timer | `0 0 4 * * *` (04:00 UTC) | YES |
| 10+ HTTP endpoints | `functions/integrations_admin.py` | HTTP (ADMIN) | On-demand | OPTIONAL |
| `test_connections` | `functions/admin_health.py` | HTTP (ADMIN) | On-demand | OPTIONAL |

---

## Database Schema

### Schema Overview

| Schema | Purpose |
|--------|---------|
| `bronze` | Raw JSON from source APIs (Nexudus, Xero) |
| `silver` | Cleaned, typed, upserted records |
| `ava` | Denormalized chatbot-ready tables |
| `meta` | Pipeline tracking, OAuth state, integration metadata |
| `core` | **Future** -- source-agnostic canonical entities |

### Bronze Tables

| Table | Upsert Key | Source |
|-------|-----------|--------|
| `bronze.nexudus_locations` | `source_id` | Nexudus `GET /sys/businesses` |
| `bronze.nexudus_products` | `source_id` | Nexudus `GET /sys/floorplandesks` |
| `bronze.nexudus_contracts` | `source_id` | Nexudus `GET /billing/coworkercontracts` |
| `bronze.nexudus_resources` | `source_id` | Nexudus `GET /spaces/resources/{id}` |
| `bronze.nexudus_extra_services` | `source_id` | Nexudus `GET /billing/extraservices` |
| `bronze.nexudus_coworkers` | `source_id` | Nexudus `GET /api/spaces/coworkers/{id}` |
| `bronze.xero_invoices` | `(xero_tenant_id, source_id)` | Xero Invoices API |
| `bronze.xero_invoice_pdfs` | `(xero_tenant_id, invoice_source_id)` | Xero Invoice PDF endpoint |

Nexudus bronze tables share columns: `id (IDENTITY PK)`, `sync_run_id (UUID)`, `source_id`, `raw_json (NVARCHAR MAX)`, `synced_at (DATETIME2)`. Products/contracts/resources also carry denormalized `location_id`.

### Silver Tables -- Nexudus

| Table | Key Fields | Notes |
|-------|-----------|-------|
| `silver.nexudus_locations` | `source_id BIGINT UNIQUE` | One row per coworking location |
| `silver.nexudus_location_hours` | `(location_source_id, day_of_week)` | 7 rows per location (Mon-Sun), open/close as minutes-since-midnight |
| `silver.nexudus_products` | `source_id BIGINT UNIQUE` | All item types: 1=Private Office, 2=Dedicated Desk, 3=Hot Desk, 4=Other, 5=Meeting Room |
| `silver.nexudus_contracts` | `source_id BIGINT UNIQUE` | Membership agreements with pricing, dates, coworker info |
| `silver.nexudus_resources` | `source_id BIGINT UNIQUE` | Meeting rooms |
| `silver.nexudus_extra_services` | `source_id BIGINT UNIQUE` | Pricing tiers for bookable resources |
| `silver.nexudus_colleagues` | `source_id BIGINT UNIQUE` | Normalized coworker data |
| `silver.nexudus_colleague_location_access` | `(colleague_source_id, location_source_id)` | Coworker -> location access mapping |

### Silver Tables -- Xero

| Table | Key Fields | Notes |
|-------|-----------|-------|
| `silver.xero_invoices` | `(xero_tenant_id, source_id)` | Typed invoice rows with financial fields |
| `silver.xero_invoice_line_items` | `(xero_tenant_id, invoice_source_id, line_item_index)` | DELETE + INSERT per invoice (not MERGE) |

### Silver Tables -- Google Maps

| Table | Purpose |
|-------|---------|
| `silver.location_nearby_pois` | Restaurants, cafes, gyms within 500-1000m |
| `silver.location_transit_stations` | Metro, train, tram, bus stops |
| `silver.location_neighborhoods` | District/area context per location |

### AVA Layer

| Table | Purpose | Refresh |
|-------|---------|---------|
| `ava.product_availability` | Denormalized chatbot-ready product availability | TRUNCATE + INSERT daily via SP |

### Meta Tables

| Table | Purpose |
|-------|---------|
| `meta.sync_runs` | Pipeline execution log (id, source, entity, layer, status, timing, row counts) |
| `meta.sync_errors` | Record-level errors with raw payload |
| `meta.gmaps_enrichment_log` | Google Maps enrichment status per location |
| `meta.xero_oauth_states` | OAuth state tokens with expiry (CSRF protection) |
| `meta.xero_connections` | Stored Xero connections (encrypted tokens, owner, tenant selection) |
| `meta.xero_tenants` | Xero tenant metadata + invoice sync watermarks |

---

## Key Modules

### `shared/azure_clients/sql_client.py`

Two classes:
- **`SQLClient`** -- Low-level ODBC connection with retry on HYT00 (serverless auto-pause). Auth modes: direct ODBC string, Managed Identity, SQL auth. Methods: `execute_query()`, `execute_non_query()`, `execute_many()`, `execute_scalar()`, `insert_and_get_id()`. Singleton via `get_sql_client()`.
- **`Database`** -- High-level wrapper with named params (`:name` syntax). Methods: `fetch_all()`, `fetch_one()`, `execute()`. Singleton via `get_db()`. Used by Ava chatbot code.

### `shared/azure_clients/run_tracker.py`

Async context manager for pipeline observability:
```python
async with RunTracker("nexudus", "locations", "bronze") as run:
    records = await fetch()
    run.rows_read = len(records)
    writer.write(records)
    run.rows_written = count
# Auto-commits status to meta.sync_runs on exit (success or failure)
```

### `shared/nexudus/client.py`

Async Nexudus API client:
- Semaphore: max 3 concurrent requests
- Exponential backoff retry (4-60s, 5 attempts) via tenacity
- Handles 429 (rate limit), 5xx, timeouts
- `paginate()` yields pages; `get_all()` collects full results
- `get_one()` for single-record fetch; `get_coworker()` for coworker records

### `shared/nexudus/auth.py`

Bearer token management:
- Priority: static env token (`NEXUDUS_BEARER_TOKEN`) -> cached token -> fetch new via password grant
- 60s expiry buffer; caches for function instance lifetime
- Endpoint: `https://spaces.nexudus.com/api/token`

### `shared/nexudus/transformers/`

Pure functions -- no I/O, stateless. JSON dict in -> typed dict out. Used by silver writers.

- `transform_location()` -- strips HTML from descriptions, parses dates
- `transform_location_hours()` -- generates 7 rows with minutes-since-midnight times
- `transform_product()` -- handles all item types; extracts `custom_size_sqm` from `CustomFields` (type 1 only); maps amenities (types 4-5 only)
- `transform_contract()` -- full contract with pricing, dates, coworker info
- `transform_resource()` -- meeting room metadata
- `transform_extra_service()` -- pricing tier transformation

### `shared/nexudus/colleague_sync.py`

Coworker access synchronization:
- Fetches coworker data from Nexudus API
- Parses accessible businesses per coworker (handles multiple API payload structures)
- Upserts to `bronze.nexudus_coworkers` (raw JSON)
- Upserts to `silver.nexudus_colleagues` (normalized)
- Syncs location access mapping via delta computation (add/remove)
- Default team business ID: `1376491118`
- Returns `NexudusColleagueSyncStats` with detailed counts

### `shared/xero/` -- Xero Integration

Complete OAuth2 + invoice sync pipeline:

- **`oauth.py`** -- Low-level OAuth helpers: build authorization URL, exchange code for tokens, refresh tokens (with rotation), fetch tenant connections. Uses `requests` + `HTTPBasicAuth`.
- **`flow.py`** -- High-level auth orchestration: `start_auth()` creates CSRF state + redirect URL; `handle_callback()` exchanges code, saves connection + tenants. Default owner: `workspace/default`.
- **`token_cipher.py`** -- Fernet (AES) encryption for tokens at rest. Key from `INTEGRATIONS_ENCRYPTION_KEY` env var. `TokenCipher.generate_key()` for new keys.
- **`store.py`** -- SQL persistence: OAuth state management (create/consume with TTL), connection upsert/update/disconnect, tenant CRUD, invoice sync watermarks. `StoredXeroConnection` dataclass for hydrated connections.
- **`client.py`** -- Authenticated Xero API client: auto-refreshes tokens when expired (5min leeway), resolves tenant ID, supports contacts/invoices/PDF endpoints. Marks connection as disconnected on `invalid_grant`.
- **`invoice_sync.py`** -- Full invoice sync service: iterates all tenants on a connection, fetches invoices (paginated, incremental via `If-Modified-Since` header), upserts to bronze + silver, replaces line items, optionally caches PDFs.

### `shared/azure_clients/bronze_writer.py`

Batch upserts using SQL MERGE (100 rows/batch via `execute_many`). `source_id` is the upsert key. Denormalizes `location_id`, `product_id`, `item_type` for indexing.

### Silver Writers

All follow the same pattern:
1. Load latest bronze rows per `source_id`
2. Call the pure transformer
3. Batch MERGE upsert into silver table
4. Return dict with row counts

**Exclusion rule**: Demo/test locations (hardcoded Nexudus source IDs) are skipped in silver writers.

---

## HTTP Endpoints (integrations_admin.py)

All endpoints require ADMIN auth level (function key or master key).
These routes are optional and are only registered when `ENABLE_ADMIN_FUNCTIONS=1`.

| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/api/integrations/nexudus/coworker-debug?coworker_id=N` | Debug single coworker payload |
| GET/POST | `/api/integrations/nexudus/sync-colleagues` | Sync coworker access (body or query: `coworker_ids`) |
| GET | `/api/integrations/xero/connect` | Initiate OAuth flow (redirects to Xero) |
| GET | `/api/integrations/xero/callback` | OAuth callback handler (exchanges code for tokens) |
| GET | `/api/integrations/xero/tenants` | List stored Xero tenants |
| GET | `/api/integrations/xero/connections` | List live Xero connections |
| GET | `/api/integrations/xero/test-contacts` | Fetch sample contacts (smoke test) |
| GET | `/api/integrations/xero/invoice-debug?invoice_id=X` | Debug single invoice |
| GET/POST | `/api/integrations/xero/sync-invoices` | Trigger invoice sync manually |
| GET | `/api/integrations/xero/invoices` | List cached invoices |
| GET | `/api/integrations/xero/invoice-pdf?invoice_id=X` | Download/cache invoice PDF |
| GET | `/api/test-connections` | Smoke test: Nexudus auth + SQL connection |

Default Xero auth path is now CLI-based:
- `python scripts/python_scripts/xero_start_oauth.py`
- `python scripts/python_scripts/xero_complete_oauth.py --redirect-url "<full redirect url>"`

---

## Environment Variables

```bash
# --- Nexudus authentication (one of the two) ---
NEXUDUS_USERNAME=...
NEXUDUS_PASSWORD=...
# OR
NEXUDUS_BEARER_TOKEN=...              # Static token (dev/test only)

# --- Azure SQL (one of three modes) ---
AZURE_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=...;Database=...;..."
# OR (Managed Identity -- no user/password needed)
AZURE_SQL_SERVER=server.database.windows.net
AZURE_SQL_DATABASE=db_name
# OR (SQL auth -- add username/password)
AZURE_SQL_USERNAME=...
AZURE_SQL_PASSWORD=...
# Optional SQL settings
AZURE_SQL_DRIVER="ODBC Driver 18 for SQL Server"  # default
AZURE_SQL_CONNECTION_TIMEOUT=60                     # default
AZURE_SQL_TRUST_SERVER_CERTIFICATE=false             # default

# --- Azure Blob Storage ---
AZURE_STORAGE_ACCOUNT_NAME=staccinfinitspaceprod001
AZURE_STORAGE_CONTAINER_RAW_NEXUDUS=nexudus-raw-snapshots

# --- Azure Storage Queue (for silver fanout) ---
AzureWebJobsStorage=...               # Auto-configured in Azure; set in local.settings.json locally

# --- Google Maps ---
GOOGLE_MAPS_API_KEY=...

# --- Xero OAuth ---
XERO_CLIENT_ID=...
XERO_CLIENT_SECRET=...
XERO_REDIRECT_URI=https://...         # Redirect copied into xero_complete_oauth.py
XERO_POST_AUTH_REDIRECT_URI=...       # Optional: user-facing redirect after auth
XERO_SCOPES="offline_access accounting.invoices accounting.payments ..."  # default: see .env.example
ENABLE_ETL_FUNCTIONS=1                # default: ETL triggers enabled
ENABLE_ADMIN_FUNCTIONS=0              # default: admin HTTP routes disabled

# --- Token encryption ---
INTEGRATIONS_ENCRYPTION_KEY=...       # Fernet key for Xero token encryption at rest

# --- Schedule overrides (cron syntax) ---
NEXUDUS_SYNC_SCHEDULE="0 0 2 * * *"               # default: 02:00 UTC
SILVER_SYNC_SCHEDULE="0 30 2 * * *"                # default: 02:30 UTC
AVA_REFRESH_SCHEDULE="0 0 3 * * *"                 # default: 03:00 UTC
XERO_INVOICE_SYNC_SCHEDULE="0 0 4 * * *"           # default: 04:00 UTC
XERO_INVOICE_SYNC_FORCE_FULL=0                     # 1 = skip incremental, full resync
```

---

## Running Locally

```bash
# Setup
python -m venv venv
source venv/Scripts/activate  # Windows bash
pip install -r requirements.txt
cp .env.example .env  # then fill in credentials

# Test auth + SQL
python scripts/python_scripts/test_local.py --step auth
python scripts/python_scripts/test_local.py --step sql

# Test individual entity (bronze fetch + write)
python scripts/python_scripts/test_local.py --step locations

# Full pipeline dry run
python scripts/python_scripts/test_local.py --step all --dry-run

# Test silver transformations
python scripts/python_scripts/test_locations_silver.py
python scripts/python_scripts/test_products_silver.py
python scripts/python_scripts/test_contracts_silver.py
python scripts/python_scripts/test_extra_services_silver.py

# Inspect database
python scripts/python_scripts/inspect_bronze.py
python scripts/python_scripts/inspect_product_per_type.py

# Nexudus colleague sync
python scripts/python_scripts/nexudus_sync_colleague_access.py --coworker-ids 123,456

# Xero operations
python scripts/python_scripts/xero_start_oauth.py
python scripts/python_scripts/xero_get_connections.py
python scripts/python_scripts/xero_list_tenants.py
python scripts/python_scripts/xero_sync_invoices.py
python scripts/python_scripts/xero_list_invoices.py

# Run unit tests
python -m pytest tests/ -v
```

---

## Azure Deployment

```bash
# Deploy function app
func azure functionapp publish func-infinitspace-datawarehouse --build remote --python

# Manual trigger via CLI
az functionapp function invoke \
  --name func-infinitspace-datawarehouse \
  --resource-group infinitspace-datawarehouse-prod \
  --function-name nexudus-to-bronze

# Tail logs
az functionapp log tail \
  --name func-infinitspace-datawarehouse \
  --resource-group infinitspace-datawarehouse-prod
```

---

## Python Dependencies

| Package | Purpose |
|---------|---------|
| `azure-functions` >=1.18.0 | Azure Functions SDK |
| `azure-identity` | Managed Identity / DefaultAzureCredential |
| `azure-storage-blob` | Blob Storage integration |
| `azure-storage-queue` | Azure Storage Queue (silver task dispatch) |
| `aiohttp` | Async HTTP (Nexudus API calls) |
| `requests` | Sync HTTP (Xero API, Google Maps API) |
| `pyodbc` | SQL Server via ODBC |
| `tenacity` | Retry with exponential backoff |
| `cryptography` | Fernet encryption for Xero tokens at rest |
| `python-dotenv` | Load `.env` for local dev |
| `openpyxl` | Excel file handling |
| `python-dateutil` | Date parsing utilities |

---

## Coding Conventions

1. **Async first**: Azure Function handlers are `async def`. SQL operations are sync (pyodbc is not async-native).
2. **Pure transformers**: All `shared/nexudus/transformers/*.py` are stateless pure functions. No DB calls inside transformers.
3. **MERGE for upserts**: All insert/update operations use SQL `MERGE ... ON target.source_id = source.source_id`. Exception: Xero line items use DELETE + INSERT.
4. **Named SQL params**: `Database` wrapper uses `:name` syntax. `SQLClient` uses `?` positional params.
5. **Batch size**: Bronze writer batches 100 rows per MERGE via `execute_many`.
6. **Error isolation**: Individual record failures are caught, logged to `meta.sync_errors`, and skipped. Pipeline continues.
7. **Run tracking**: Every Nexudus pipeline step must use `RunTracker` context manager. Xero sync tracks via `meta.xero_tenants` watermarks.
8. **Blob snapshots**: Every bronze run stores a full JSON snapshot: `nexudus/{entity}/{yyyy}/{mm}/{dd}/{run_id}.json`.
9. **Exclusions**: Demo/test locations (hardcoded Nexudus source IDs) are skipped in silver writers.
10. **Item type mapping**: products `item_type` -> 1=Private Office, 2=Dedicated Desk, 3=Hot Desk, 4=Other, 5=Meeting Room.
11. **Token encryption**: All Xero tokens are Fernet-encrypted at rest in `meta.xero_connections`.
12. **Blueprint pattern**: Each function file exports a `bp = func.Blueprint()` that must be registered in `function_app.py`.

---

## AVA Layer

The `ava` schema contains a single denormalized table consumed directly by the Ava chatbot. It is rebuilt from scratch daily (TRUNCATE + INSERT) by a stored procedure -- **no upsert, no delta, full rebuild every run**.

### `ava.product_availability` -- key columns

| Column | Type | Notes |
|--------|------|-------|
| `location_source_id` | BIGINT | FK to `silver.nexudus_locations.source_id` |
| `location_name`, `city`, `country_name` | NVARCHAR | Denormalized for zero-join queries |
| `item_category` | NVARCHAR(32) | `hot_desk` / `dedicated_desk` / `private_office` / `meeting_room` / `day_pass` |
| `product_source_id` | BIGINT NULL | Populated for desk types + private offices |
| `resource_source_id` | BIGINT NULL | Populated for meeting rooms (when resource join hits) |
| `extra_service_source_id` | BIGINT NULL | Populated for meeting rooms + day passes |
| `item_name` | NVARCHAR(512) | Display name for the item |
| `capacity` | INT | 1 for desks/passes; from `products.capacity` for offices; from `resources.allocation` for rooms |
| `price` | DECIMAL(12,2) | Min price for meeting rooms/day passes |
| `charge_period` | NVARCHAR(32) | `per_month` / `per_booking` / `per_day` |
| `is_available` | BIT | 0 = occupied (private offices only); 1 = available |
| `available_from` | DATE | Date the occupied office becomes free (NULL = indefinite) |
| `occupied_until` | DATE | Known end of active contract (NULL = rolling monthly) |
| `next_occupied_from` | DATE | When a currently-free office will next be taken |
| `chain_occupied_until` | DATE NULL | End of consecutive future contract chain. NULL = indefinite |
| `availability_notes` | NVARCHAR(512) | Human-readable string for Ava to surface |

### Source logic per category

| Category | Source tables | Availability |
|----------|--------------|-------------|
| `hot_desk` | `silver.nexudus_products` WHERE `item_type=3` | Always available |
| `dedicated_desk` | `silver.nexudus_products` WHERE `item_type=2` | Always available |
| `private_office` | `silver.nexudus_products` WHERE `item_type=1` + contracts via `contract_ids_raw` | Contract-derived (see scenarios below) |
| `meeting_room` | `silver.nexudus_extra_services` (NOT LIKE `'hot desk%'`) + `silver.nexudus_resources` | Always available |
| `day_pass` | `silver.nexudus_extra_services` WHERE `resource_type_names LIKE 'hot desk%'` | Always available; MIN price per location |

### Private office availability scenarios

**Consecutive chain**: If contract A ends Jun 30 and contract B starts Jun 30 (same day or earlier), they form a chain. `chain_occupied_until` = end of the last contract in the chain. NULL = last contract in chain has no known end.

| Scenario | `is_available` | `available_from` | `chain_occupied_until` | `availability_notes` |
|----------|----------------|-----------------|------------------------|---------------------|
| No contracts ever | 1 | NULL | NULL | `'Available'` |
| Only past/inactive contracts | 1 | NULL | NULL | `'Available'` |
| Free now + future chain (known end) | 1 | NULL | chain end date | `'Available now -- reserved from X through Y'` |
| Free now + future chain (indefinite) | 1 | NULL | NULL | `'Available now -- reserved from X (long-term occupancy follows)'` |
| Active rolling, no future | 0 | NULL | NULL | `'Occupied -- active monthly contract, no fixed end date'` |
| Active rolling + future chain | 0 | NULL | chain end | `'Occupied (monthly renewal); re-occupied from Y'` |
| Active known end + NO gap + chain (known end) | 0 | chain end | chain end | `'Occupied until X; re-occupied through Y'` |
| Active known end + NO gap + chain (indefinite) | 0 | NULL | NULL | `'Occupied until X; immediately re-occupied with no known end date'` |
| Active known end + GAP + chain (known end) | 0 | occupied_until | chain end | `'Occupied until X; briefly available, then re-occupied from Y through Z'` |
| Active known end + GAP + chain (indefinite) | 0 | occupied_until | NULL | `'Occupied until X; briefly available, then re-occupied from Y'` |
| Active known end + no future | 0 | occupied_until | NULL | `'Occupied until X'` |

### Key files

- Schema DDL: `scripts/sql_scripts/ava_product_availability_schema.sql`
- Stored procedure: `scripts/sql_scripts/ava_sp_refresh_product_availability.sql`
- Azure Function: `functions/ava_refresh.py`
- Schedule env var: `AVA_REFRESH_SCHEDULE` (default: `0 0 3 * * *` = 03:00 UTC daily)

---

## Silver Queue-Based Architecture

`bronze_to_silver` (timer trigger) is the **orchestrator** -- it only enqueues 5 messages and exits in under 1 second. `silver_entity_worker` (queue trigger) is the **worker** -- one isolated invocation per entity, running in parallel.

- **Queue name**: `silver-sync-tasks` on storage account `staccinfinitspaceprod001`
- **Connection binding**: `Storage` (resolves to `AzureWebJobsStorage`)
- **Poison queue**: `silver-sync-tasks-poison` -- messages failing 5 consecutive times land here
- **Idempotency**: All MERGE upserts on `source_id` -- queue retries are fully safe
- **Failure isolation**: one entity failing raises an exception -> message is retried; other entities are unaffected

---

## SQL Validation Queries

```sql
-- Latest runs
SELECT TOP 10 * FROM meta.sync_runs ORDER BY started_at DESC;

-- Data counts
SELECT 'bronze.locations' AS t, COUNT(*) FROM bronze.nexudus_locations
UNION ALL SELECT 'silver.locations', COUNT(*) FROM silver.nexudus_locations
UNION ALL SELECT 'silver.products', COUNT(*) FROM silver.nexudus_products
UNION ALL SELECT 'silver.contracts', COUNT(*) FROM silver.nexudus_contracts
UNION ALL SELECT 'silver.resources', COUNT(*) FROM silver.nexudus_resources
UNION ALL SELECT 'silver.extra_services', COUNT(*) FROM silver.nexudus_extra_services
UNION ALL SELECT 'silver.colleagues', COUNT(*) FROM silver.nexudus_colleagues
UNION ALL SELECT 'silver.xero_invoices', COUNT(*) FROM silver.xero_invoices
UNION ALL SELECT 'ava.product_availability', COUNT(*) FROM ava.product_availability;

-- Failed runs
SELECT * FROM meta.sync_runs WHERE status = 'failed' ORDER BY started_at DESC;

-- Record errors
SELECT TOP 20 * FROM meta.sync_errors ORDER BY created_at DESC;

-- Xero connection status
SELECT id, owner_type, owner_id, is_connected, last_error, expires_at
FROM meta.xero_connections ORDER BY updated_at DESC;

-- Xero tenant sync status
SELECT xero_tenant_id, tenant_name, last_invoice_sync_completed_at, last_invoice_sync_error
FROM meta.xero_tenants ORDER BY tenant_name;
```

---

## Current Implementation Status

| Feature | Status | Details |
|---------|--------|---------|
| Nexudus -> Bronze (5 entities) | DONE | Daily at 02:00 UTC |
| Bronze -> Silver (5 entities) | DONE | Queue-based parallel at 02:30 UTC |
| Azure Blob snapshots | DONE | Raw JSON per run |
| SQL retry logic (HYT00) | DONE | Serverless auto-pause handling |
| Run tracking (meta.sync_runs) | DONE | All Nexudus pipeline steps |
| Silver queue-based fanout | DONE | 5 parallel workers via Storage Queue |
| AVA layer (product_availability) | DONE | SP rebuild daily at 03:00 UTC |
| Nexudus colleague sync | DONE | HTTP trigger, bronze + silver + access mapping |
| Xero OAuth + invoice sync | DONE | Timer at 04:00 UTC + HTTP endpoints |
| Xero PDF caching | DONE | Optional per-invoice PDF cache in bronze |
| Google Maps enrichment | DONE (code) | BUT: blueprint not registered -- needs fix |
| Unit tests | PARTIAL | Colleague sync + Xero integration covered |
| Silver -> Core population | PLANNED | Roadmap item |
| Power BI dashboards | PLANNED | Roadmap item |
| HubSpot integration | PLANNED | Roadmap item |
| Incremental loads | PLANNED | Currently full-reload daily for Nexudus |
| dbt transformation layer | PLANNED | Roadmap item |
| Data quality checks | PLANNED | Roadmap item |

---

### Code quality observations

| Issue | Location | Severity |
|-------|----------|----------|
| `.gitignore` excludes `*.json` | `.gitignore` | Medium -- blocks `host.json`, `local.settings.json` from tracking |
| `__pycache__/` directories in repo | Multiple | Low -- should be in `.gitignore` (they are, but some were committed) |
| No `__init__.py` in several packages | `shared/azure_clients/`, `shared/nexudus/transformers/` | Low -- works due to implicit namespace packages but explicit is cleaner |

---

## Current Branch Strategy

- `main` -- production branch (current)
- Feature branches: `feature/your-feature-name`

---

## Self-Update Instructions for Claude

After ANY change to this project, update this file:

1. **New files added** -> Add to the Repository Structure section
2. **Schema changes** -> Update the relevant table in the Database Schema section
3. **New features completed** -> Move from "PLANNED" to "DONE" in Implementation Status
4. **New dependencies** -> Add to Python Dependencies table
5. **New env vars** -> Add to Environment Variables section
6. **Bugs/gotchas discovered** -> Add to Dead Code & Cleanup or Known Issues
7. **Branch changes** -> Update Current Branch Strategy
8. **Functions added/removed** -> Update Azure Functions Registry table
9. **HTTP endpoints added** -> Update HTTP Endpoints table
10. **Dead code removed** -> Remove from Dead Code section
11. **Always** -> Update `Last Updated` at the bottom

---

**Last Updated**: 2026-04-07 (comprehensive rewrite: added Xero integration, colleague sync, dead code audit, improvement suggestions)
**Current Branch**: `main`
**Maintainer**: InfinitSpace Data Engineering Team
