# CLAUDE.md — InfinitSpace Data Warehouse

> **Self-Updating Protocol**: Every Claude instance that makes changes to this project MUST update this file before finishing the session. Update the relevant sections below and bump the `Last Updated` field at the bottom. This ensures the next instance starts with accurate context.

---

## Project Overview

**InfinitSpace Data Warehouse** is a production-grade ETL pipeline running on **Azure Functions** that ingests coworking space data from **Nexudus API** into a three-tier lakehouse architecture (Bronze → Silver → Core) stored in **Azure SQL**.

- **Language**: Python 3.11+
- **Platform**: Azure Functions (Consumption Plan)
- **Database**: Azure SQL Server (`infinitspace-prod-main-db`)
- **Primary data source**: Nexudus API (coworking space management platform)
- **Secondary enrichment**: Google Maps Places API
- **Blob storage**: Azure Blob Storage (raw snapshots / audit trail)
- **Resource group**: `infinitspace-datawarehouse-prod`
- **Function App name**: `infinitspace-dw-functions`

---

## Architecture: Three-Tier Lakehouse

```
Nexudus API  ──[02:00 UTC]──▶  Bronze (raw, append-only)
                                      │
                              [02:30 UTC]
                                      ▼
                    bronze_to_silver() timer trigger
                    (enqueues 5 messages — one per entity)
                                      │
                     Azure Storage Queue "silver-sync-tasks"
                      │          │          │         │         │
                 [locations] [products] [contracts] [resources] [extra_services]
                  silver_entity_worker() × 5  (parallel, isolated)
                                      │
                                      ▼
                             Silver (typed, upserted)
                                      │
                              [03:00 UTC]
                                      ▼
                           AVA layer (denormalized, chatbot-ready)
                                      │
                              [Future]
                                      ▼
                              Core (canonical, multi-source)
                                      │
                                      ▼
                       Power BI / Ava Bot / Internal APIs
```

**Bronze**: Immutable raw JSON stored with metadata. Never modified, only appended.
**Silver**: Cleaned, typed, normalized records. Upserted daily on `source_id` (Nexudus entity ID).
**AVA**: Denormalized, chatbot-ready flat table. TRUNCATE + rebuild daily via stored procedure. See [AVA Layer](#ava-layer) section below.
**Core**: Source-agnostic canonical entities. **Not yet implemented** (Q1 2026 roadmap).

---

## Repository Structure

```
Infinitspace-datawarehouse/
├── CLAUDE.md                          ← This file (keep updated!)
├── README.md                          ← Human-facing project docs
├── SQL_datawarehouse.md               ← SQL architecture overview
├── requirements.txt                   ← Python dependencies
├── function_app.py                    ← Azure Functions entry point (registers blueprints)
├── host.json                          ← Azure Functions config (timeout: 10min)
├── .env.example                       ← Environment variable template
├── .funcignore                        ← Files excluded from Azure deployment
│
├── functions/
│   ├── bronze_nexudus.py              ← Timer trigger: Nexudus → Bronze (02:00 UTC)
│   ├── silver_nexudus.py              ← Timer trigger: enqueues 5 silver tasks (02:30 UTC)
│   ├── silver_worker.py               ← Queue trigger: Bronze → Silver per entity (parallel)
│   ├── ava_refresh.py                 ← Timer trigger: Silver → AVA layer (03:00 UTC)
│   └── enrich_gmaps.py                ← HTTP trigger: Google Maps enrichment (on-demand)
│
├── shared/
│   ├── azure_clients/
│   │   ├── sql_client.py              ← SQL connection manager + Database wrapper
│   │   ├── bronze_writer.py           ← Batch upsert to bronze.nexudus_* tables
│   │   ├── blob_writer.py             ← Store raw snapshots in Azure Blob Storage
│   │   ├── queue_client.py            ← Enqueue silver tasks to Azure Storage Queue
│   │   ├── run_tracker.py             ← Context manager: logs to meta.sync_runs
│   │   ├── silver_write_locations.py  ← Bronze → silver.nexudus_locations + _hours
│   │   ├── silver_writer_products.py  ← Bronze → silver.nexudus_products
│   │   ├── silver_writer_contracts.py ← Bronze → silver.nexudus_contracts
│   │   ├── silver_writer_resources.py ← Bronze → silver.nexudus_resources
│   │   └── silver_writer_extra_services.py ← Bronze → silver.nexudus_extra_services
│   │
│   ├── nexudus/
│   │   ├── auth.py                    ← Bearer token auth (cached, refreshes on expiry)
│   │   ├── client.py                  ← Async API client (rate limit: 3 concurrent, retry)
│   │   └── transformers/
│   │       ├── locations.py           ← transform_location() + transform_location_hours()
│   │       ├── products.py            ← transform_product() (all item types)
│   │       ├── contracts.py           ← transform_contract()
│   │       ├── resources.py           ← transform_resource()
│   │       └── extra_services.py      ← transform_extra_service()
│   │
│   └── gmaps/
│       ├── __init__.py
│       └── enrichment.py              ← Google Places API: POIs, transit, neighborhoods
│
├── scripts/
│   ├── python_scripts/
│   │   ├── test_local.py              ← Full pipeline test (--step auth/sql/locations/all)
│   │   ├── test_locations_silver.py
│   │   ├── test_products_silver.py
│   │   ├── test_contracts_silver.py
│   │   ├── test_extra_services_silver.py
│   │   ├── inspect_bronze.py
│   │   ├── inspect_product_per_type.py
│   │   └── enrich_location_gmaps.py
│   │
│   └── sql_scripts/
│       ├── bronze_layer.sql                        ← CREATE: all bronze.* + meta tables
│       ├── bronze_upsert_constraints.sql
│       ├── silver_nexudus_locations_schema.sql
│       ├── silver_nexudus_products_schema.sql
│       ├── silver_nexudus_contracts_schema.sql
│       ├── silver_nexudus_resources_schema.sql
│       ├── silver_nexudus_extra_services_schema.sql
│       ├── silver_gmaps_locations_schema.sql
│       ├── ava_product_availability_schema.sql     ← CREATE: ava schema + product_availability table
│       ├── ava_sp_refresh_product_availability.sql ← SP: TRUNCATE + rebuild from silver tables
│       └── test.sql                                ← Ad-hoc validation queries
│
├── docs/
│   ├── deploy.md
│   └── silver_table_relationships.md
│
├── deploy/
│   ├── setup_azure_resources.sh
│   └── setup_azure_resources.ps1
│
└── membership_agreement_test/         ← Experimental: PDF contract parsing
    ├── compute_notice_period.py
    ├── count_pages.py
    ├── extract.py
    └── extracted/
```

---

## Database Schema

### Schemas

| Schema | Purpose |
|--------|---------|
| `bronze` | Raw, append-only Nexudus records with `raw_json` column |
| `silver` | Cleaned, typed, upserted records (upsert key: `source_id`) |
| `core` | **Future** – source-agnostic canonical entities |
| `meta` | Pipeline run tracking (`sync_runs`, `sync_errors`, `gmaps_enrichment_log`) |

### Bronze Tables

| Table | Upsert Key | Source API |
|-------|-----------|-----------|
| `bronze.nexudus_locations` | `source_id` | `GET /sys/businesses` |
| `bronze.nexudus_products` | `source_id` | `GET /sys/floorplandesks` |
| `bronze.nexudus_contracts` | `source_id` | `GET /billing/coworkercontracts` |
| `bronze.nexudus_resources` | `source_id` | `GET /spaces/resources/{id}` |
| `bronze.nexudus_extra_services` | `source_id` | `GET /billing/extraservices` |

All bronze tables share columns: `id (IDENTITY PK)`, `sync_run_id (UUID)`, `source_id`, `raw_json (NVARCHAR MAX)`, `synced_at (DATETIME2)`. Products/contracts/resources also carry denormalized `location_id`.

### Silver Tables

| Table | Key Fields | Notes |
|-------|-----------|-------|
| `silver.nexudus_locations` | `source_id BIGINT UNIQUE` | One row per coworking location |
| `silver.nexudus_location_hours` | `(location_source_id, day_of_week)` | 7 rows per location, open/close in minutes-since-midnight |
| `silver.nexudus_products` | `source_id BIGINT UNIQUE` | All item types (1=Office, 2=Dedicated, 3=Hot, 4=Other, 5=Room) |
| `silver.nexudus_contracts` | `source_id BIGINT UNIQUE` | Membership agreements |
| `silver.nexudus_resources` | `source_id BIGINT UNIQUE` | Meeting rooms (types 4-5) |
| `silver.nexudus_extra_services` | `source_id BIGINT UNIQUE` | Pricing tiers for bookable resources |

### Google Maps Tables

| Table | Purpose |
|-------|---------|
| `silver.location_nearby_pois` | Restaurants, cafes, gyms, etc. within 500-1000m |
| `silver.location_transit_stations` | Metro, train, tram, bus stops |
| `silver.location_neighborhoods` | District/area context per location |

### Meta Tables

| Table | Purpose |
|-------|---------|
| `meta.sync_runs` | Pipeline execution log (status, row counts, timing) |
| `meta.sync_errors` | Record-level errors with raw payload |
| `meta.gmaps_enrichment_log` | Google Maps enrichment status per location |

---

## Key Modules

### `shared/azure_clients/sql_client.py`

Two classes:
- **`SQLClient`** – Low-level ODBC connection with retry on HYT00 (serverless auto-pause). Auth modes: direct ODBC string, Managed Identity, SQL auth. Methods: `execute_query()`, `execute_non_query()`, `execute_scalar()`, `insert_and_get_id()`.
- **`Database`** – High-level wrapper with named params (`:name` syntax). Methods: `fetch_all()`, `fetch_one()`, `execute()`. Singleton via `get_db()`.

### `shared/azure_clients/run_tracker.py`

Context manager pattern:
```python
async with RunTracker("nexudus", "locations", "bronze") as run:
    records = await fetch()
    run.rows_read = len(records)
    writer.write(records)
    run.rows_written = count
# Auto-commits to meta.sync_runs on exit
```

### `shared/nexudus/client.py`

Async Nexudus API client:
- Semaphore: max 3 concurrent requests
- Exponential backoff retry (2–60s wait)
- Handles 429, 5xx, timeouts
- `paginate()` yields pages; `get_all()` collects full results

### `shared/nexudus/auth.py`

Bearer token management:
- Priority: static env token → cached token → fetch new via password grant
- 60s expiry buffer; caches for function instance lifetime
- Endpoint: `https://spaces.nexudus.com/api/token`

### `shared/nexudus/transformers/`

Pure functions — no I/O, stateless. JSON dict in → typed dict out. Used by silver writers and test scripts.

- `transform_location()` → strips HTML from descriptions, parses dates
- `transform_location_hours()` → generates 7 rows with minutes-since-midnight times
- `transform_product()` → handles all item types; extracts `custom_size_sqm` from `CustomFields` (type 1 only); maps amenities (types 4-5 only)
- `transform_contract()` → full contract with pricing, dates, coworker info
- `transform_resource()` → meeting room metadata
- `transform_extra_service()` → pricing tier transformation

### `shared/azure_clients/bronze_writer.py`

Batch upserts using SQL MERGE (100 rows/batch). `source_id` is the upsert key. Denormalizes `location_id`, `product_id` for indexing.

### Silver Writers

All follow the same pattern:
1. Load latest bronze rows per `source_id`
2. Call the pure transformer
3. Batch MERGE upsert into silver table
4. Return (rows_written, rows_skipped) counts

**Exclusion rule**: Locations with ID = "beyond Global" or demo are skipped.

---

## Environment Variables

```bash
# Nexudus authentication (one of the two)
NEXUDUS_USERNAME=...
NEXUDUS_PASSWORD=...
# OR
NEXUDUS_BEARER_TOKEN=...

# Azure SQL (one of two modes)
AZURE_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=...;Database=...;..."
# OR (Managed Identity)
AZURE_SQL_SERVER=server.database.windows.net
AZURE_SQL_DATABASE=db_name

# Azure Blob Storage
AZURE_STORAGE_ACCOUNT_NAME=staccinfinitspaceprod001
AZURE_STORAGE_CONTAINER_RAW_NEXUDUS=nexudus-raw-snapshots

# Google Maps
GOOGLE_MAPS_API_KEY=...

# Optional schedule overrides (cron syntax)
NEXUDUS_SYNC_SCHEDULE="0 0 2 * * *"   # default: 02:00 UTC daily
SILVER_SYNC_SCHEDULE="0 30 2 * * *"   # default: 02:30 UTC daily
AVA_REFRESH_SCHEDULE="0 0 3 * * *"    # default: 03:00 UTC daily
```

---

## Running Locally

```bash
# Setup
python -m venv venv
source venv/Scripts/activate  # Windows bash
pip install -r requirements.txt
cp .env.example .env  # then fill in credentials

# Test auth
python scripts/python_scripts/test_local.py --step auth

# Test SQL connection
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
```

---

## Azure Deployment

```bash
# Deploy function app
func azure functionapp publish infinitspace-dw-functions --build remote --python

# Manual trigger via CLI
az functionapp function invoke \
  --name infinitspace-dw-functions \
  --resource-group infinitspace-datawarehouse-prod \
  --function-name nexudus-to-bronze

# Tail logs
az functionapp log tail \
  --name infinitspace-dw-functions \
  --resource-group infinitspace-datawarehouse-prod
```

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
UNION ALL SELECT 'silver.resources', COUNT(*) FROM silver.nexudus_resources;

-- Failed runs
SELECT * FROM meta.sync_runs WHERE status = 'failed' ORDER BY started_at DESC;

-- Record errors
SELECT TOP 20 * FROM meta.sync_errors ORDER BY created_at DESC;
```

---

## Python Dependencies

| Package | Purpose |
|---------|---------|
| `azure-functions` >=1.18.0 | Azure Functions SDK |
| `azure-identity` | Managed Identity authentication |
| `azure-storage-blob` | Blob Storage integration |
| `azure-storage-queue` | Azure Storage Queue (silver task dispatch) |
| `aiohttp` | Async HTTP (Nexudus API calls) |
| `requests` | Sync HTTP (Google Maps API) |
| `pyodbc` | SQL Server via ODBC |
| `tenacity` | Retry with exponential backoff |
| `python-dotenv` | Load `.env` for local dev |
| `openpyxl` | Excel file handling |
| `python-dateutil` | Date parsing utilities |

---

## Coding Conventions

1. **Async first**: Azure Function handlers are `async def`. SQL operations may be sync (pyodbc is not async-native).
2. **Pure transformers**: All `shared/nexudus/transformers/*.py` are stateless pure functions. No DB calls inside transformers.
3. **MERGE for upserts**: All insert/update operations use SQL `MERGE ... ON target.source_id = source.source_id`.
4. **Named SQL params**: `Database` wrapper uses `:name` syntax. `SQLClient` uses `?` positional params.
5. **Batch size**: Bronze writer batches 100 rows per MERGE. Silver writers do one-by-one or small batches.
6. **Error isolation**: Individual record failures are caught, logged to `meta.sync_errors`, and skipped. Pipeline continues.
7. **Run tracking**: Every pipeline step must use `RunTracker` context manager.
8. **Blob snapshots**: Every bronze run stores a full JSON snapshot in blob storage path `nexudus/{entity}/{yyyy}/{mm}/{dd}/{run_id}.json`.
9. **Exclusions**: Demo/test locations (hardcoded Nexudus source IDs) are skipped in silver writers.
10. **Item type mapping**: products `item_type` → 1=Private Office, 2=Dedicated Desk, 3=Hot Desk, 4=Other, 5=Meeting Room.

---

## AVA Layer

The `ava` schema contains a single denormalized table consumed directly by the Ava chatbot. It is rebuilt from scratch daily (TRUNCATE + INSERT) by a stored procedure — **no upsert, no delta, full rebuild every run**.

### `ava.product_availability` — key columns

| Column | Type | Notes |
|--------|------|-------|
| `location_source_id` | BIGINT | FK to `silver.nexudus_locations.source_id` |
| `location_name`, `city`, `country_name` | NVARCHAR | Denormalized for zero-join queries |
| `item_category` | NVARCHAR(32) | `hot_desk` \| `dedicated_desk` \| `private_office` \| `meeting_room` \| `day_pass` |
| `product_source_id` | BIGINT NULL | Populated for desk types + private offices |
| `resource_source_id` | BIGINT NULL | Populated for meeting rooms (when resource join hits) |
| `extra_service_source_id` | BIGINT NULL | Populated for meeting rooms + day passes |
| `item_name` | NVARCHAR(512) | Display name for the item |
| `capacity` | INT | 1 for desks/passes; from `products.capacity` for offices; from `resources.allocation` for rooms |
| `price` | DECIMAL(12,2) | Min price for meeting rooms/day passes |
| `charge_period` | NVARCHAR(32) | `per_month` \| `per_booking` \| `per_day` |
| `is_available` | BIT | 0 = occupied (private offices only); 1 = available |
| `available_from` | DATE | Date the occupied office becomes free (NULL = indefinite) |
| `occupied_until` | DATE | Known end of active contract (NULL = rolling monthly) |
| `next_occupied_from` | DATE | When a currently-free office will next be taken |
| `chain_occupied_until` | DATE NULL | End of the consecutive future contract chain starting at `next_occupied_from`. NULL = chain is indefinite. E.g. office free now → contract A Mar 31–Jun 30 → contract B Jun 30–Mar 2027 → `chain_occupied_until = 2027-03-30` |
| `availability_notes` | NVARCHAR(512) | Human-readable string for Ava to surface |

### Source logic per category

| Category | Source tables | Availability |
|----------|--------------|-------------|
| `hot_desk` | `silver.nexudus_products` WHERE `item_type=3` | Always available |
| `dedicated_desk` | `silver.nexudus_products` WHERE `item_type=2` | Always available |
| `private_office` | `silver.nexudus_products` WHERE `item_type=1` + `silver.nexudus_contracts` via `contract_ids_raw` | Contract-derived (5 scenarios) |
| `meeting_room` | `silver.nexudus_extra_services` (NOT LIKE `'hot desk%'`) + `silver.nexudus_resources` | Always available |
| `day_pass` | `silver.nexudus_extra_services` WHERE `resource_type_names LIKE 'hot desk%'` | Always available; MIN price per location |

### Private office availability logic (5 scenarios)

**Consecutive chain**: If contract A ends Jun 30 and contract B starts Jun 30 (same day or earlier), they form a chain. `chain_occupied_until` = end of the last contract in the chain. NULL = last contract in chain has no known end.

| Scenario | `is_available` | `available_from` | `chain_occupied_until` | `availability_notes` |
|----------|----------------|-----------------|------------------------|---------------------|
| No contracts ever | 1 | NULL | NULL | `'Available'` |
| Only past/inactive contracts | 1 | NULL | NULL | `'Available'` |
| Free now + future chain (known end) | 1 | NULL | chain end date | `'Available now – reserved from X through Y'` |
| Free now + future chain (indefinite) | 1 | NULL | NULL | `'Available now – reserved from X (long-term occupancy follows)'` |
| Active rolling, no future | 0 | NULL | NULL | `'Occupied – active monthly contract, no fixed end date'` |
| Active rolling + future chain | 0 | NULL | chain end | `'Occupied (monthly renewal); re-occupied from Y'` |
| Active known end + NO gap + chain (known end) | 0 | chain end | chain end | `'Occupied until X; re-occupied through Y'` |
| Active known end + NO gap + chain (indefinite) | 0 | NULL | NULL | `'Occupied until X; immediately re-occupied with no known end date'` |
| Active known end + GAP + chain (known end) | 0 | occupied_until | chain end | `'Occupied until X; briefly available, then re-occupied from Y through Z'` |
| Active known end + GAP + chain (indefinite) | 0 | occupied_until | NULL | `'Occupied until X; briefly available, then re-occupied from Y'` |
| Active known end + no future | 0 | occupied_until | NULL | `'Occupied until X'` |

### Key files

- Schema DDL: [scripts/sql_scripts/ava_product_availability_schema.sql](scripts/sql_scripts/ava_product_availability_schema.sql)
- Stored procedure: [scripts/sql_scripts/ava_sp_refresh_product_availability.sql](scripts/sql_scripts/ava_sp_refresh_product_availability.sql)
- Azure Function: [functions/ava_refresh.py](functions/ava_refresh.py)
- Schedule env var: `AVA_REFRESH_SCHEDULE` (default: `0 0 3 * * *` = 03:00 UTC daily)

---

## Current Branch Strategy

- `main` — production branch
- `etl/silver-layer` — active development branch (current as of last update)
- Feature branches: `feature/your-feature-name`

---

## Current Implementation Status

| Feature | Status | Details |
|---------|--------|---------|
| Nexudus → Bronze | ✅ Complete | 5 entities synced daily at 02:00 UTC |
| Bronze → Silver | ✅ Complete | All 5 entities transformed at 02:30 UTC |
| Azure Blob snapshots | ✅ Complete | Raw JSON stored per run |
| SQL retry logic | ✅ Complete | HYT00 serverless auto-pause handling |
| Run tracking | ✅ Complete | `meta.sync_runs` + `meta.sync_errors` |
| Google Maps enrichment | ✅ Complete | POIs, transit, neighborhoods (HTTP trigger) |
| Silver queue-based fanout | ✅ Complete | Timer enqueues 5 tasks; workers run in parallel via `silver-sync-tasks` queue |
| AVA layer table + SP | ✅ Complete | `ava.product_availability` rebuilt daily at 03:00 UTC |
| Local test scripts | ✅ Complete | All entities covered |
| Silver → Core population | 🚧 Planned | Q1 2026 |
| Power BI dashboards | 🚧 Planned | Q1 2026 |
| Hubspot integration | 🚧 Planned | Q2 2026 |
| Incremental loads | 🚧 Planned | Q2 2026 |
| dbt transformation layer | 🚧 Planned | Q3 2026 |
| Data quality checks | 🚧 Planned | Q2 2026 |

---

## Silver Queue-Based Architecture

`bronze_to_silver` (timer trigger) is the **orchestrator** — it only enqueues 5 messages and exits in under 1 second. `silver_entity_worker` (queue trigger) is the **worker** — one isolated invocation per entity, running in parallel.

- **Queue name**: `silver-sync-tasks` on storage account `staccinfinitspaceprod001`
- **Connection binding**: `AzureWebJobsStorage` (auto-configured in Azure; set in `local.settings.json` for local dev)
- **Poison queue**: `silver-sync-tasks-poison` — messages failing 5 consecutive times land here for manual inspection
- **Idempotency**: All `_upsert()` calls use SQL MERGE on `source_id` — queue retries are fully safe
- **Failure isolation**: one entity failing raises an exception → message is retried; other entities are unaffected

---

## Known Issues / Gotchas

- The `.gitignore` contains `*.json` which is overly broad — it may accidentally exclude JSON config files. Be aware when adding new JSON files.
- `silver_write_locations.py` generates exactly 7 rows per location (Mon-Sun) in `silver.nexudus_location_hours`. Open/close times are stored as integers (minutes since midnight), e.g. 540 = 09:00, 1020 = 17:00.
- Products of type 4-5 (Other/Meeting Room) carry populated `resource_*` and `amenity_*` columns; types 1-3 have `NULL` for those fields.
- `custom_size_sqm` is only populated for type 1 (Private Office) via `CustomFields` in the raw JSON. All other types use the standard `size_sqm`.
- Nexudus API uses bearer token (OAuth2 password grant). Token is cached per function instance; a 60-second buffer avoids edge-case expiry.
- `Database` wrapper (`get_db()`) uses named params (`:name`); `SQLClient` (`get_sql_client()`) uses positional `?` params.
- Silver workers share `AzureWebJobsStorage` (queue trigger binding) with the same storage account used by `BlobWriter` (`AZURE_STORAGE_ACCOUNT_NAME`). Ensure the Function App's managed identity has `Storage Queue Data Contributor` + `Storage Queue Data Message Sender` roles on the storage account.

---

## Self-Update Instructions for Claude

After ANY change to this project, update this file:

1. **New files added** → Add to the repository structure section
2. **Schema changes** → Update the relevant table in the Database Schema section
3. **New features completed** → Move from "Planned" to "Complete" in Implementation Status
4. **New dependencies** → Add to Python Dependencies table
5. **New env vars** → Add to Environment Variables section
6. **Bugs/gotchas discovered** → Add to Known Issues section
7. **Branch changes** → Update Current Branch Strategy
8. **Always** → Update `Last Updated` at the bottom

---

**Last Updated**: 2026-03-09 (silver layer refactored to queue-based fanout; added silver_worker.py + queue_client.py; bronze_to_silver is now an orchestrator)
**Current Branch**: `etl/silver-layer`
**Maintainer**: InfinitSpace Data Engineering Team
