# AGENTS.md -- InfinitSpace Data Warehouse

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
- CoStar PDF extractor (Real Estate HTTP function)
- Google Maps enrichment utilities exist but are not part of the scheduled Function App
- Location Scraper (HTTP-triggered Durable Functions pipeline — Idealista + Otodom)

Platform:

- Azure Functions
- Azure SQL
- Azure Blob Storage for raw Nexudus snapshots, Nexudus invoice PDFs, and Xero invoice PDFs
- Azure Storage Queue for silver fanout

Current deployment docs target:

- resource group: `infinitspace-prod-northeurope-data-rg`
- ETL app: `func-infinitspace-etl`
- storage account: `staccinfinitspaceprod001`

---

## Runtime Topology

Default ETL execution order in UTC:

1. `Sun 01:00` `nexudus_silver_reconcile` (weekly soft-delete sweep for non-invoice silver tables)
2. `02:00` `nexudus_to_bronze`
3. `02:30` `bronze_to_silver`
4. queue fanout via `silver_entity_worker`
5. `03:00` `refresh_ava_availability`
6. `03:30` `nexudus_invoice_pdf_cache` (caches PDFs for invoices missing `pdf_blob_path`)
7. `04:00` `xero_invoice_sync` (includes PDF caching for invoices missing `pdf_blob_path`)
8. `05:00` `bamboohr_sync` (includes daily employee roster reconcile)
9. `05:15` `nexudus_invoice_reconcile` (daily soft-delete of removed invoices + cascaded lines)
10. `05:30` `replyio_stats_sync`
11. `05:30` `refresh_finance_dashboard`
12. `06:00` `sync_health_report` (emails green/red daily report via Microsoft Graph)

Important operational caveat:

- `bronze_to_silver` is schedule-based, not dependency-aware
- `refresh_ava_availability` is also schedule-based
- bronze should finish before silver starts
- silver workers should finish before AVA starts
- silver workers now load only bronze rows changed since their last successful silver run, so `rows_written` is incremental rather than full-snapshot

Flow:

```text
Nexudus API
  -> nexudus_to_bronze
  -> bronze.nexudus_locations
  -> bronze.nexudus_products
  -> bronze.nexudus_contracts
  -> bronze.nexudus_coworker_invoices   (incremental via UpdatedSince watermark)
  -> bronze.nexudus_coworkers           (distinct CoworkerIds from invoices)
  -> bronze.nexudus_resources
  -> bronze.nexudus_extra_services
  -> bronze.nexudus_coworker_invoice_lines (per-invoice line items)
  -> blob snapshots (nexudus-raw-snapshots container)

bronze_to_silver
  -> Azure Storage Queue: silver-sync-tasks
  -> silver_entity_worker x 8
  -> silver.nexudus_locations
  -> silver.nexudus_location_hours
  -> silver.nexudus_products
  -> silver.nexudus_contracts
  -> silver.nexudus_coworker_invoices
  -> silver.nexudus_coworkers
  -> silver.nexudus_resources
  -> silver.nexudus_extra_services
  -> silver.nexudus_coworker_invoice_lines

nexudus_to_bronze
  -> bronze.nexudus_coworker_invoice_histories (only unpaid direct-debit invoices due from last month onward)

nexudus_invoice_pdf_cache (timer, 03:30 UTC)
  -> downloads PDFs from Nexudus API for recent invoices missing pdf_blob_path (last 2 days)
  -> uploads to Azure Blob: nexudus-invoice-pdfs container
  -> updates silver.nexudus_coworker_invoices.pdf_blob_path
  -> marks invoices returning server errors with '__unavailable__' sentinel to avoid retries

refresh_ava_availability
  -> EXEC ava.sp_refresh_product_availability
  -> ava.product_availability

Xero API
  -> xero_invoice_sync
  -> bronze.xero_invoices
  -> silver.xero_invoices (pdf_blob_path populated when PDF is cached)
  -> silver.xero_invoice_line_items
  -> silver.xero_tenants
  -> xero.silver_tenants (view alias)
  -> optional bronze.xero_invoice_pdfs

Real Estate (HTTP trigger, optional)
  -> run_costar_extractor (HTTP POST /api/real-estate/costar/run)
  -> costar-extraction-tasks queue
  -> costar_extraction_worker
  -> downloads PDF from Blob (pdf-uploads/<blob_name>)
  -> BuildingContactExtractor (Anthropic API)
  -> uploads XLSX to Blob (excel-outputs/<job_id>_contacts.xlsx)
  -> updates bronze.costar_pdf_extractor_logs (Real Estate DB)

Reply.io API
  -> replyio_stats_sync (timer, 05:30 UTC)
  -> bronze.replyio_sequence_steps
  -> bronze.replyio_sequence_step_performance (daily stats, yesterday)

refresh_finance_dashboard (timer, 05:30 UTC)
  -> EXEC gold.sp_refresh_finance_dashboard
  -> gold.finance_dashboard_user_access (BambooHR -> Nexudus locations)
  -> gold.finance_dashboard_invoice_worklist (Nexudus invoices, Nexudus-only)
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
- `ENABLE_REAL_ESTATE_FUNCTIONS=0`
- `ENABLE_LOCATION_SCRAPER_FUNCTIONS=0`

Optional admin deployment:

- `ENABLE_ETL_FUNCTIONS=0`
- `ENABLE_ADMIN_FUNCTIONS=1`

Optional Real Estate deployment (can be combined with ETL):

- `ENABLE_ETL_FUNCTIONS=1`
- `ENABLE_REAL_ESTATE_FUNCTIONS=1`
- `ENABLE_ADMIN_FUNCTIONS=0`

This means the default Azure Function App should show only:

- `nexudus_to_bronze`
- `bronze_to_silver`
- `silver_entity_worker`
- `refresh_ava_availability`
- `xero_invoice_sync`
- `replyio_stats_sync`

---

## Repository Structure

```text
Infinitspace-datawarehouse/
  AGENTS.md
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
    nexudus_invoice_pdf_cache.py
    nexudus_invoice_reconcile.py
    nexudus_silver_reconcile.py
    xero_sync.py
    finance_dashboard_refresh.py
    integrations_admin.py
    admin_health.py
    real_estate_costar.py
    real_estate_costar_worker.py
    replyio_sync.py
    sync_health_report.py
  shared/
    azure_clients/
      sql_client.py
      bronze_writer.py
      blob_writer.py
      queue_client.py
      run_tracker.py
      silver_sync.py
      silver_write_locations.py
      silver_writer_products.py
      silver_writer_contracts.py
      silver_writer_resources.py
      silver_writer_extra_services.py
      silver_writer_coworker_invoices.py
      silver_writer_coworker_invoice_lines.py
      silver_writer_coworkers.py
    nexudus/
      auth.py
      client.py
      invoice_pdf_cache.py
      transformers/
        locations.py
        products.py
        contracts.py
        resources.py
        extra_services.py
        coworker_invoices.py
        coworker_invoice_lines.py
        coworkers.py
    bamboohr/
      __init__.py
      client.py
      transformers/
        employees.py
    xero/
      oauth.py
      flow.py
      token_cipher.py
      store.py
      client.py
      invoice_sync.py
      tenant_directory.py
    integrations/
      xero_nexudus_overdue.py
    gmaps/
    replyio/
      __init__.py
      client.py
    real_estate/
      __init__.py
      building_contact_extractor.py   (adapted from AI-REAL-ESTATE repo, uses PyMuPDF)
    notifications/
      __init__.py
      graph_mailer.py   (Microsoft Graph app-only sendMail)
    azure_clients/
      ...
      costar_queue_client.py
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
      sync_nexudus_billing.py
      xero_nexudus_link_audit.py
      test_xero_pdf.py
      test_xero_pdf_cache.py
      backfill_xero_pdfs.py
    sql_scripts/
      bronze_layer.sql
      bronze_upsert_constraints.sql
      silver_nexudus_locations_schema.sql
      silver_nexudus_products_schema.sql
      silver_nexudus_contracts_schema.sql
      silver_nexudus_resources_schema.sql
      silver_nexudus_resources_alignment.sql
      silver_nexudus_extra_services_schema.sql
      silver_gmaps_locations_schema.sql
      silver_soft_delete_migration.sql
      ava_product_availability_schema.sql
      ava_sp_refresh_product_availability.sql
      integrations_nexudus_xero_schema.sql
      nexudus_billing_sync_schema.sql
      nexudus_coworker_invoice_lines_schema.sql
      xero_invoices_schema.sql
      xero_pdf_blob_migration.sql
      test.sql
  tests/
    test_ava_refresh.py
    test_nexudus_resource_transformer.py
    test_silver_sync.py
    test_xero_integration.py
    test_xero_tenant_directory.py
    test_xero_nexudus_invoice_linking.py
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
| `nexudus_to_bronze` | `functions/bronze_nexudus.py` | timer | `0 0 2 * * *` | writes bronze + blob snapshots; 8 entities including coworker_invoice_lines |
| `bronze_to_silver` | `functions/silver_nexudus.py` | timer | `0 30 2 * * *` | enqueues 8 queue messages (includes coworker_invoice_lines) |
| `silver_entity_worker` | `functions/silver_worker.py` | queue | `silver-sync-tasks` | one entity per invocation |
| `refresh_ava_availability` | `functions/ava_refresh.py` | timer | `0 0 3 * * *` | executes AVA stored procedure |
| `xero_invoice_sync` | `functions/xero_sync.py` | timer | `0 0 4 * * *` | syncs all linked Xero tenants + caches PDFs for invoices missing `pdf_blob_path`; reuses the backfill retry/throttle flow |
| admin HTTP routes | `functions/integrations_admin.py` | HTTP | on-demand | only when `ENABLE_ADMIN_FUNCTIONS=1` |
| `test_connections` | `functions/admin_health.py` | HTTP | on-demand | only when `ENABLE_ADMIN_FUNCTIONS=1` |
| `run_costar_extractor` | `functions/real_estate_costar.py` | HTTP POST | `real-estate/costar/run` | only when `ENABLE_REAL_ESTATE_FUNCTIONS=1` — enqueues only, returns 202 |
| `costar_extraction_worker` | `functions/real_estate_costar_worker.py` | queue | `costar-extraction-tasks` | only when `ENABLE_REAL_ESTATE_FUNCTIONS=1` — does the actual extraction |
| `bamboohr_sync` | `functions/bamboohr_sync.py` | timer | `0 0 5 * * *` | syncs all BambooHR employees to bronze + silver; join key: `work_email` |
| `nexudus_invoice_pdf_cache` | `functions/nexudus_invoice_pdf_cache.py` | timer | `0 30 3 * * *` | caches Nexudus invoice PDFs to blob for invoices missing `pdf_blob_path` |
| `nexudus_invoice_reconcile` | `functions/nexudus_invoice_reconcile.py` | timer | `0 15 5 * * *` | daily soft-delete pass for `silver.nexudus_coworker_invoices` + cascaded lines; 365-day due_date window |
| `nexudus_silver_reconcile` | `functions/nexudus_silver_reconcile.py` | timer | `0 0 1 * * 0` | weekly soft-delete sweep for locations, products, contracts, extra_services, resources, coworkers |
| `replyio_stats_sync` | `functions/replyio_sync.py` | timer | `0 30 5 * * *` | syncs Reply.io sequence steps + daily step performance stats to bronze |
| `sync_health_report` | `functions/sync_health_report.py` | timer | `0 0 6 * * *` | daily health report email via Microsoft Graph; green/red flags per entity from `meta.sync_runs` (last 24h) + record-level error summary from `meta.sync_errors` |
| `location_scraper_http` | `functions/location_scraper.py` | HTTP POST | `/api/scrape` | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — Durable Functions starter; returns 202 |
| `location_scraper_monthly` | `functions/location_scraper.py` | timer | `0 0 1 1 * *` | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — starts unlimited monthly Durable scrape runs for Barcelona, Madrid, Milan, Berlin, Munich, Hamburg, Cologne, Frankfurt, Dusseldorf, Stuttgart, and Warsaw |
| `location_scraper_orch` | `functions/location_scraper.py` | orchestration | — | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — Durable orchestrator |
| `ls_*` activities | `functions/location_scraper.py` | activity | — | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — resolve / scrape / enrich / persist / log / materialize |

---

## Data Model Summary

### Real Estate (CoStar extractor)

- uses `bronze.costar_pdf_extractor_logs` (in Real Estate DB, not datawarehouse DB)
- connection string: `AZURE_SQL_PDF_JOBS_CONNECTION_STRING`
- extractor module: `shared/real_estate/building_contact_extractor.py`
  (adapted from AI-REAL-ESTATE repo, uses PyMuPDF — no system dependencies)

### Bronze

- `bronze.nexudus_locations`
- `bronze.nexudus_products`
- `bronze.nexudus_contracts`
- `bronze.nexudus_resources`
- `bronze.nexudus_extra_services`
- `bronze.nexudus_coworker_invoices`
- `bronze.nexudus_coworkers`
- `bronze.xero_invoices`
- `bronze.xero_invoice_pdfs` — stores `blob_path` reference, not raw bytes
- `bronze.bamboohr_employees`
- `bronze.nexudus_coworker_invoice_lines`
- `bronze.nexudus_coworker_invoice_histories`
- `bronze.replyio_sequence_steps`
- `bronze.replyio_sequence_step_performance`

Nexudus bronze rows are latest-payload upserts on `source_id`, not append-only history.

### Silver

All Nexudus silver tables below carry `is_deleted BIT NOT NULL DEFAULT 0`
and `deleted_at DATETIME2 NULL`, maintained by the reconcile jobs. Gold
tables and downstream reads must filter `WHERE is_deleted = 0`.

- `silver.nexudus_locations`
- `silver.nexudus_location_hours`
- `silver.nexudus_products`
- `silver.nexudus_contracts`
- `silver.nexudus_resources`
- `silver.nexudus_extra_services`
- `silver.nexudus_coworker_invoices` — includes `pdf_blob_path`, `pdf_cached_at`, `invoice_status`, `processing`
- `silver.nexudus_coworker_invoice_lines` — per-invoice line items with `financial_account_code`/`financial_account_name`
- `silver.nexudus_coworkers`
- `silver.xero_invoices` — includes `pdf_blob_path`, `pdf_cached_at`
- `silver.xero_invoice_line_items`
- `silver.xero_tenants`
- `silver.location_nearby_pois`
- `silver.location_transit_stations`
- `silver.location_neighborhoods`
- `silver.xero_overdue_invoice_contacts` — view joining overdue Xero invoices to Nexudus customer email data
- `silver.bamboohr_employees` — join key: `work_email` → `silver.nexudus_coworkers.email`; carries `is_deleted`/`deleted_at` reconciled daily by `bamboohr_sync`
- `silver.eventbrite_events` — fully flattened Eventbrite event rows; venue and ticket availability objects are scalar columns, with raw JSON retained only in `bronze.eventbrite_events.raw_json`
- `silver.hubspot_marketing_emails` — fully flattened HubSpot marketing email rows; stats and content/widget objects are scalar columns, with raw JSON retained only in `bronze.hubspot_marketing_emails.raw_json`

### AVA

- `ava.product_availability`
  - rebuilt daily
  - populated by stored procedure
  - no incremental logic

### Gold

- `gold.finance_dashboard_user_access`
  - materialized BambooHR employee → Nexudus location access table
  - CM/ACM access rules with Amsterdam exception
- `gold.finance_dashboard_invoice_worklist`
  - materialized Nexudus invoice worklist (Nexudus-only, no Xero dependency)
  - workflow_type: `recurrent` if any line item has `financial_account_name LIKE '%MEMBERSHIP FEES%'`
  - includes `pdf_blob_path` for cached Nexudus invoice PDFs
- `gold.finance_dashboard_revenue_occupancy`
  - materialized daily location snapshot for contracted MRR and workstation occupancy
  - occupancy = active physical workstation capacity assigned by active contracts / total physical workstation capacity from Nexudus products
  - revenue = contracted monthly recurring value from active Nexudus contracts, using `price_with_products` fallback to `price`/`tariff_price`
- `gold.vw_finance_dashboard_membership_schedule`
  - contract-level membership schedule view for member/company, dates, capacity, latest monthly fee, notice period, contract value, and remaining value
  - rebuilt by `gold.sp_refresh_finance_dashboard`

### Meta

- `meta.sync_runs`
- `meta.sync_errors`
- `meta.gmaps_enrichment_log`
- `meta.xero_oauth_states`
- `meta.xero_connections`
- `meta.xero_tenants`
- `meta.finance_dashboard_location_settings` — per-location finance email, seeded from known locations

### Xero Directory

- canonical table: `silver.xero_tenants`
- SQL view alias: `xero.silver_tenants`
- one row per Xero tenant for a connection
- location columns are copied from the best matched `silver.nexudus_locations` row
- `community_manager_name` is intentionally a placeholder for now and is preserved on refresh

---

## Key Technical Behaviors

### Nexudus Bronze

- `functions/bronze_nexudus.py`
- fetch order:
  - locations (incremental via `UpdatedSince` watermark)
  - products (incremental via `UpdatedSince` watermark)
  - contracts (incremental via `UpdatedSince` watermark)
  - coworker_invoices (incremental via `UpdatedSince` watermark)
  - coworkers (per-ID from invoices — no `UpdatedSince`)
  - resources (per-ID from products — no `UpdatedSince`)
  - extra_services (incremental via `UpdatedSince` watermark)
  - coworker_invoices (2-day lookback via `from_CoworkerInvoice_UpdatedOn` — no watermark)
  - coworker_invoice_lines (per-invoice via `CoworkerInvoiceLine_CoworkerInvoice` for invoices from above)
- all paginated entities use `UpdatedSince` watermark from `meta.sync_runs.finished_at` on subsequent runs; first run does full fetch
- each entity writes a `RunTracker` row
- each entity also writes a blob snapshot

### Silver Fanout

- `functions/silver_nexudus.py` only enqueues work
- `functions/silver_worker.py` performs the actual transformation
- each writer loads only the latest bronze rows changed since the last successful silver run for that entity
- queue retries are safe because silver writes are idempotent upserts
- poison queue: `silver-sync-tasks-poison`
- entities: locations, products, contracts, coworker_invoices, coworkers, resources, extra_services, coworker_invoice_lines

### AVA Refresh

- `functions/ava_refresh.py`
- runs `EXEC ava.sp_refresh_product_availability`
- verifies both `ava.product_availability` and `ava.sp_refresh_product_availability` exist before running
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
  - refreshes `silver.xero_tenants` after invoice sync
  - `cache_missing_pdfs()` runs after sync — fetches PDFs for any invoice with no `pdf_blob_path`
  - reuses the same retry/throttle flow as `scripts/python_scripts/backfill_xero_pdfs.py`
  - does not use `RunTracker`
- `shared/xero/tenant_directory.py`
  - matches legal Xero tenant names to Nexudus locations
  - preserves any manually maintained `community_manager_name`

### Location Scraper Geocoding

- `shared/location_scraper/activities/scrape.py`
  - normalizes Apify rows and applies a Google Maps geocoding fallback when a listing has no latitude/longitude but does have address, postal code, or district data
  - avoids city-only geocoding so vague rows do not all stack on one city-center marker
  - uses `GOOGLE_MAPS_API_KEY`; if unset, rows without coordinates remain ungeocoded and may not appear on the map
- `shared/location_scraper/activities/materialize_globe.py`
  - applies the same fallback while building `silver.location_scraper_globe_v2`, so existing raw runs can be rematerialized into mappable rows
  - refreshes `gold.location_scraper_map_markers` through `gold.sp_refresh_location_scraper_map_markers` after each successful location scraper materialization
  - preserves listing `currency` through the gold marker table so Warsaw can display mixed EUR/PLN prices correctly
- this is especially important for Immobilienscout24, which often hides exact German addresses

### Nexudus Invoice PDF Storage

- PDFs are stored in Azure Blob Storage, not in SQL
- container: `nexudus-invoice-pdfs` on `staccinfinitspaceprod001`
- blob path format: `{location_source_id}/{yyyy}/{mm}/{invoice_source_id}.pdf`
- `silver.nexudus_coworker_invoices.pdf_blob_path` holds the reference
- `pdf_blob_path IS NULL` AND `updated_on >= 2 days ago` is the watermark — only recently-updated invoices missing a cached PDF are fetched each run
- invoices returning Nexudus server errors (500/502/503) are marked `__unavailable__` to avoid infinite retries
- timer function: `nexudus_invoice_pdf_cache` at 03:30 UTC

### Nexudus invoice status and due-date handling

- `shared/nexudus/transformers/coworker_invoices.py` normalizes timezone-aware Nexudus `DueDate` values into the local business timezone before writing silver.
- Default invoice timezone is `Europe/Amsterdam`; override with `NEXUDUS_INVOICE_TIMEZONE` if needed.
- Silver invoices carry `invoice_status` and `processing`; finance dashboard gold refreshes exclude invoices where `processing = 1` or `invoice_status` contains `Processing`.
- The Nexudus invoice API does not expose the UI `Processing` badge as `Status`; direct-debit payment state is derived from `billing/coworkerinvoicehistories` `Payment Result` rows.
- Histories are fetched only for unpaid direct-debit invoices with due dates from the last month onward (`NEXUDUS_INVOICE_HISTORY_LOOKBACK_MONTHS`, default `1`).
- `AWAITING`/pending history rows keep an invoice out of the finance dashboard. Failed payment result rows are counted in `payment_failure_count`; invoices enter the dashboard only when the failed-payment count is greater than `3`.
- One-off finance dashboard rows are held back until `due_date <= today + 2 days`, matching the -2 / due-day reminder start.

### Xero PDF Storage

- PDFs are stored in Azure Blob Storage, not in SQL
- container: `xero-invoice-pdfs` on `staccinfinitspaceprod001`
- blob path format: `{xero_tenant_id}/{yyyy}/{mm}/{invoice_source_id}.pdf`
- `bronze.xero_invoice_pdfs.blob_path` and `silver.xero_invoices.pdf_blob_path` hold the reference
- `BlobWriter.write_pdf()` uploads; `BlobWriter.read_pdf()` downloads by path
- `pdf_blob_path IS NULL` is the natural watermark — only invoices still missing a cached PDF are fetched each night

### Xero ↔ Nexudus Invoice Linking

- `shared/integrations/xero_nexudus_overdue.py` — pure linking logic, no I/O
- `silver.xero_overdue_invoice_contacts` — SQL view, ready to query
- match priority: `invoice_number` > `payment_reference` > `xero_reference` > `xero_reference_payment_reference`
- same-location matches ranked above cross-location matches
- `recipient_email` coalesces: `billing_email` → `email` → `coworker_billing_email`
- rows with `match_reason = 'unmatched'` have no Nexudus record; `recipient_email` will be NULL
- current coverage: all 12 Xero tenants connected (Starter tier limit)

### Soft-delete / source reconciliation

The regular bronze/silver sync is upsert-only and cannot observe source-side
deletions (an incremental `UpdatedSince` response simply stops returning a
deleted record; per-ID fetches never revisit records whose parent didn't
change). Silver tables therefore carry `is_deleted BIT NOT NULL DEFAULT 0`
and `deleted_at DATETIME2 NULL` columns, populated by dedicated reconcile
jobs that fetch the full current ID set from the source and flag missing
rows.

Pattern for all reconcile jobs:

1. Fetch every currently-active `source_id` from the source API (scoped to a
   sensible window where applicable).
2. Safety floor: abort if the fetched count is below a per-entity threshold
   (protects against an empty API response wiping silver).
3. `UPDATE ... SET is_deleted = 1, deleted_at = GETUTCDATE()` for silver rows
   whose `source_id` is missing from the fetched set.
4. `UPDATE ... SET is_deleted = 0, deleted_at = NULL` for silver rows whose
   `source_id` reappears (restore on source-side undelete).

Schedule + cadence per entity:

| Silver table | Reconcile job | Cadence | Deletion lag |
|---|---|---|---|
| `nexudus_coworker_invoices` | `nexudus_invoice_reconcile` | daily 05:15 | ≤24h |
| `nexudus_coworker_invoice_lines` | `nexudus_invoice_reconcile` (cascade) | daily 05:15 | ≤24h |
| `bamboohr_employees` | embedded in `bamboohr_sync` | daily 05:00 | ≤24h |
| `nexudus_locations` | `nexudus_silver_reconcile` | weekly Sun 01:00 | ≤7d |
| `nexudus_products` | `nexudus_silver_reconcile` | weekly Sun 01:00 | ≤7d |
| `nexudus_contracts` | `nexudus_silver_reconcile` | weekly Sun 01:00 | ≤7d |
| `nexudus_extra_services` | `nexudus_silver_reconcile` | weekly Sun 01:00 | ≤7d |
| `nexudus_resources` | `nexudus_silver_reconcile` | weekly Sun 01:00 | ≤7d |
| `nexudus_coworkers` | `nexudus_silver_reconcile` | weekly Sun 01:00 | ≤7d |
| `xero_invoices` | none — Xero sets `invoice_status = 'DELETED'`, existing sync picks it up | daily | ≤24h (status-based) |

Invoice reconcile window: default 365 days of `due_date` (configurable via
`NEXUDUS_INVOICE_RECONCILE_LOOKBACK_DAYS`). Invoices older than the window
are ignored — the finance dashboard only cares about recent due dates.

Downstream consumers (gold tables, views, reports) MUST filter
`WHERE is_deleted = 0` on any silver read. `gold.sp_refresh_finance_dashboard`
already enforces this on every silver join.

Manual business exclusions:

- `shared/nexudus/exclusions.py` lists Nexudus locations intentionally hidden
  from silver even when Nexudus still returns them. Current excluded real
  location: Kingsbourne House / London - Holborn - 229-231 High Holborn
  (`location_source_id=1414964752`).
- Use `scripts/python_scripts/deactivate_nexudus_location.py` for an
  idempotent dry-run/apply soft-delete and immediate AVA/finance refresh for a
  location exclusion.

---

## Logging and Operational Expectations

### RunTracker-backed functions

`RunTracker` writes to `meta.sync_runs` for:

- Nexudus bronze entity runs (including coworker_invoices, coworkers)
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

Operational note:

- for Nexudus silver runs, `rows_read` and `rows_written` now reflect only bronze rows changed since the last successful silver run for that entity, not the full current snapshot size

### Expected Nexudus logs

- `Nexudus -> Bronze sync started`
- `Locations: X fetched, Y written to bronze`
- `Products: X fetched, Y written to bronze`
- `Contracts: X fetched, Y written to bronze`
- `Coworker invoices: X fetched, Y written to bronze. Distinct coworker ids: Z`
- `Coworkers: X attempted, Y written, Z skipped`
- `Resources: X attempted, Y written, Z skipped`
- `Extra services: X fetched, Y written to bronze`
- `Nexudus -> Bronze sync complete`

### Expected silver logs

- `Bronze -> Silver orchestrator started`
- `Bronze -> Silver: 7 tasks enqueued`
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
- `Xero PDF cache complete: {pdfs_cached: N, pdfs_failed: N, pdfs_total: N}`
- possible warning: `Some tenants failed during Xero sync`
- final Xero sync stats include nested `tenant_directory` refresh results

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
AZURE_STORAGE_CONTAINER_XERO_PDFS=xero-invoice-pdfs
AZURE_STORAGE_CONTAINER_NEXUDUS_PDFS=nexudus-invoice-pdfs

# Queue trigger storage
AzureWebJobsStorage=...

# BambooHR
BAMBOOHR_SUBDOMAIN=infinitspace
BAMBOOHR_API_KEY=...
BAMBOOHR_SYNC_SCHEDULE="0 0 5 * * *"  # optional override

# Google Maps
GOOGLE_MAPS_API_KEY=...

# Xero
XERO_CLIENT_ID=...
XERO_CLIENT_SECRET=...
XERO_REDIRECT_URI=https://...
XERO_POST_AUTH_REDIRECT_URI=...
XERO_SCOPES="offline_access accounting.invoices accounting.payments ..."
INTEGRATIONS_ENCRYPTION_KEY=...

# Reply.io
REPLY_IO_API_KEY=...

# Sync health report (Microsoft Graph sendMail)
# App registration needs Mail.Send (Application) permission with admin consent
# on the sender mailbox (GRAPH_SENDER_UPN, e.g. info@infinitspace.com).
GRAPH_TENANT_ID=...
GRAPH_CLIENT_ID=...
GRAPH_CLIENT_SECRET=...
GRAPH_SENDER_UPN=info@infinitspace.com
SYNC_REPORT_RECIPIENTS=bryan.swannie@infinitspace.com,baptiste.valentin@infinitspace.com
SYNC_HEALTH_REPORT_SCHEDULE="0 0 6 * * *"
SYNC_REPORT_LOOKBACK_HOURS=24

# Location Scraper
APIFY_TOKEN=...
LUSHA_API_KEY=...
ENABLE_LOCATION_SCRAPER_FUNCTIONS=0
LOCATION_SCRAPER_MONTHLY_SCHEDULE="0 0 1 1 * *"

# Function registration
ENABLE_ETL_FUNCTIONS=1
ENABLE_ADMIN_FUNCTIONS=0

# Schedule overrides
NEXUDUS_SYNC_SCHEDULE="0 0 2 * * *"
SILVER_SYNC_SCHEDULE="0 30 2 * * *"
AVA_REFRESH_SCHEDULE="0 0 3 * * *"
XERO_INVOICE_SYNC_SCHEDULE="0 0 4 * * *"
XERO_INVOICE_SYNC_FORCE_FULL=0
NEXUDUS_PDF_CACHE_SCHEDULE="0 30 3 * * *"
NEXUDUS_INVOICE_RECONCILE_SCHEDULE="0 15 5 * * *"
NEXUDUS_INVOICE_RECONCILE_LOOKBACK_DAYS=365
NEXUDUS_INVOICE_RECONCILE_MIN_IDS=100
NEXUDUS_SILVER_RECONCILE_SCHEDULE="0 0 1 * * 0"
BAMBOOHR_RECONCILE_MIN_IDS=10
FINANCE_DASHBOARD_REFRESH_SCHEDULE="0 30 5 * * *"
REPLYIO_SYNC_SCHEDULE="0 30 5 * * *"
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

Nexudus billing validation:

```powershell
# Dry-run first
.\venv\Scripts\python.exe scripts\python_scripts\sync_nexudus_billing.py --dry-run --limit 5
# Full backfill (first time only)
.\venv\Scripts\python.exe scripts\python_scripts\sync_nexudus_billing.py
# Incremental (subsequent runs)
.\venv\Scripts\python.exe scripts\python_scripts\sync_nexudus_billing.py --since-last-run
# Check Xero <-> Nexudus match rate
.\venv\Scripts\python.exe scripts\python_scripts\xero_nexudus_link_audit.py --show-unmatched
```

Xero PDF validation:

```powershell
# Test single PDF fetch (saves to disk)
.\venv\Scripts\python.exe scripts\python_scripts\test_xero_pdf.py
# Test full round-trip: fetch -> blob upload -> SQL -> read back
.\venv\Scripts\python.exe scripts\python_scripts\test_xero_pdf_cache.py
# Backfill PDFs for all existing invoices missing `pdf_blob_path`
.\venv\Scripts\python.exe scripts\python_scripts\backfill_xero_pdfs.py --dry-run
.\venv\Scripts\python.exe scripts\python_scripts\backfill_xero_pdfs.py
```

Xero validation:

```powershell
.\venv\Scripts\python.exe scripts\python_scripts\xero_start_oauth.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_complete_oauth.py --redirect-url "<full redirect url>"
.\venv\Scripts\python.exe scripts\python_scripts\xero_get_connections.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_sync_invoices.py --owner-type workspace --owner-id default
.\venv\Scripts\python.exe scripts\python_scripts\xero_list_invoices.py --owner-type workspace --owner-id default --top 20
.\venv\Scripts\python.exe -m unittest tests.test_xero_integration tests.test_xero_tenant_directory tests.test_xero_nexudus_invoice_linking
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

SELECT
    tenant_name,
    location_name,
    location_city,
    location_country_name,
    community_manager_name,
    location_match_rule
FROM xero.silver_tenants
ORDER BY tenant_name;

-- Xero <-> Nexudus link quality
SELECT match_reason, COUNT(*) AS cnt
FROM silver.xero_overdue_invoice_contacts
GROUP BY match_reason ORDER BY cnt DESC;

-- Overdue invoices ready for email automation
SELECT invoice_number, contact_name, due_date, amount_due,
       recipient_email, coworker_full_name, match_reason
FROM silver.xero_overdue_invoice_contacts
WHERE recipient_email IS NOT NULL
  AND match_reason != 'unmatched'
ORDER BY due_date ASC;

-- PDF cache status
SELECT
    COUNT(*) AS total_overdue,
    SUM(CASE WHEN pdf_blob_path IS NOT NULL THEN 1 ELSE 0 END) AS pdfs_cached,
    SUM(CASE WHEN pdf_blob_path IS NULL THEN 1 ELSE 0 END) AS pdfs_missing
FROM silver.xero_invoices
WHERE invoice_status = 'AUTHORISED'
  AND amount_due > 0
  AND due_date < CAST(GETUTCDATE() AS DATE);
```

---

## Deployment Notes

Before deploying, ensure the new env var is set:

```powershell
az functionapp config appsettings set `
  --resource-group infinitspace-prod-northeurope-data-rg `
  --name func-infinitspace-etl `
  --settings AZURE_STORAGE_CONTAINER_XERO_PDFS=xero-invoice-pdfs
```

ETL app:

```powershell
func azure functionapp publish func-infinitspace-etl --python

az functionapp config appsettings set `
  --resource-group infinitspace-prod-northeurope-data-rg `
  --name func-infinitspace-etl `
  --settings `
    ENABLE_ETL_FUNCTIONS=1 `
    ENABLE_ADMIN_FUNCTIONS=0
```

Optional admin app:

```powershell
func azure functionapp publish func-infinitspace-etl --python

az functionapp config appsettings set `
  --resource-group infinitspace-prod-northeurope-data-rg `
  --name func-infinitspace-etl `
  --settings `
    ENABLE_ETL_FUNCTIONS=0 `
    ENABLE_ADMIN_FUNCTIONS=1
```

---

## Current Status
 
| Feature | Status | Notes |
|---------|--------|-------|
| Nexudus bronze sync | done | 8 entities (added coworker_invoice_lines) |
| Nexudus silver fanout | done | queue-based, 8 entities |
| AVA refresh | done | stored procedure rebuild |
| Xero OAuth + tenant storage | done | DB-backed |
| Xero auto-refresh | done | disconnects on `invalid_grant` |
| Xero invoice sync | done | incremental by tenant |
| Xero tenant directory | done | refreshed after Xero sync and exposed as `xero.silver_tenants` |
| Xero invoice PDF caching | done | blob storage (`xero-invoice-pdfs`); path in `silver.xero_invoices.pdf_blob_path`; auto-cached nightly for invoices missing `pdf_blob_path` |
| Nexudus coworker invoices + coworkers | done | incremental via UpdatedSince watermark |
| Xero ↔ Nexudus invoice linking | done | `silver.xero_overdue_invoice_contacts` view; 5/12 tenants connected |
| Optional admin HTTP routes | done | separate deployment mode |
| Google Maps scheduled pipeline | not wired | utilities exist, not registered in default app |
| Core layer population | planned | not implemented |
| Real Estate CoStar extractor HTTP function | done | `ENABLE_REAL_ESTATE_FUNCTIONS=1` to activate |
| BambooHR employee sync | done | bronze + silver; `work_email` is join key to Nexudus coworkers |
| Reply.io stats sync | done | bronze only; sequence steps + daily step performance; 4 AB test sequences |
| Nexudus coworker invoice lines | done | bronze + silver; `financial_account_code`/`financial_account_name` per line item |
| Nexudus invoice PDF caching | done | blob storage (`nexudus-invoice-pdfs`); path in `silver.nexudus_coworker_invoices.pdf_blob_path` |
| Finance dashboard gold layer | done | Nexudus-only; `gold.finance_dashboard_invoice_worklist` + `gold.finance_dashboard_user_access`; rebuilt by `gold.sp_refresh_finance_dashboard`; filters `is_deleted = 0` on all silver reads |
| Soft-delete / source reconciliation | done | `is_deleted`/`deleted_at` on all Nexudus + BambooHR silver tables; daily `nexudus_invoice_reconcile` (invoices + cascaded lines), daily roster reconcile inside `bamboohr_sync`, weekly `nexudus_silver_reconcile` for other entities |
| Sync health report email | done | daily 06:00 UTC via Microsoft Graph; subject `[OK]`/`[FAIL]`; green/red table per entity + record-level error summary; sends to `SYNC_REPORT_RECIPIENTS` from `GRAPH_SENDER_UPN` |
| Location Scraper (Idealista + Otodom + Immobilienscout24) | done | HTTP-triggered and monthly timer-triggered Durable Functions pipeline; `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1`; monthly unlimited runs for Barcelona, Madrid, Milan, Berlin, Munich, Hamburg, Cologne, Frankfurt, Dusseldorf, Stuttgart, and Warsaw; Lusha enrichment via fan-out; bronze schema; see `docs/location_scraper.md` |

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

Last updated: 2026-05-06 (Location Scraper: added JSON schema discovery script `scripts/sql_scripts/location_scraper_raw_schema_discovery.sql` to profile `payload_json` paths/types/coverage for future globe view mapping; docs updated accordingly. Orchestrator now marks failed runs via new `ls_mark_run_failed` activity calling `mark_run_failed`, preventing stale `running` status on errors. Fixed `location_scraper_run_quality` insert placeholder mismatch in `shared/location_scraper/activities/log_run.py` so quality rows persist correctly. Scraper volume cap is now configurable via common env `LOCATION_SCRAPER_MAX_ITEMS` across all actors (`idealista`, `otodom`, `immobilienscout`) with fallback defaults; tests updated. SQL client retry logic now treats AAD/local transient network states (`HYT00`, `08001`, `08S01`) as retryable to reduce intermittent local hangs while keeping passwordless auth mode. Added `docs/location_scraper_source_mapping.md` with source-specific JSON mapping across all active sources: immobilienscout, idealista, and otodom. Added `scripts/sql_scripts/location_scraper_globe_materialized_v2.sql` for materialized app read table DDL (`silver.location_scraper_globe_v2`). raw -> silver materialization is now handled in Function App code via new activity `ls_materialize_globe` (`shared/location_scraper/activities/materialize_globe.py`) using per-run delete+insert into silver table. Globe materialization now reuses existing bronze Lusha contacts to populate top-3 email/contact/title/confidence slots in `silver.location_scraper_globe_v2`, and Idealista country fallback now resolves from configured city so Milan maps to `IT` while Spanish cities map to `ES`. Added `scripts/sql_scripts/location_scraper_globe_quality.sql` to create `silver.location_scraper_globe_quality` plus refresh proc `silver.sp_refresh_location_scraper_globe_quality`, enabling run/source/city data-quality analysis for globe coverage, duplicates, raw-to-silver deltas, and Lusha email coverage. The Durable orchestrator now calls `ls_refresh_globe_quality` after `ls_materialize_globe`, so the quality table updates automatically on each successful run. Otodom globe materialization now extracts individual `contact_name` from non-agency keys in `sellerPhones` and uses the matching phone number, fixing zero contact-name coverage when raw payloads contain person names. Otodom Lusha enrichment now keeps multiple distinct individual broker candidates per agency instead of one candidate per agency, caps individual candidates per agency, allows only one company fallback per agency to avoid repeated Google/Lusha calls, and Lusha company search titles were expanded with broker/agent/advisor variants including Polish real-estate terms. Location Scraper log handling now upserts running rows, sets `updated_at`, marks stale `running` rows failed on new run startup (`LOCATION_SCRAPER_STALE_RUNNING_HOURS`, default 2), safely attempts to mark failed from critical activities, and includes `scripts/sql_scripts/location_scraper_logs_hardening.sql` for DB cleanup/default hardening. Added `bronze.location_scraper_lusha_diagnostics` via `scripts/sql_scripts/location_scraper_raw_and_quality.sql`; the orchestrator writes one row per Lusha enrichment candidate through `ls_write_lusha_diagnostics`, including `run_id`, source/city, agency/person, path/reason, raw/final contact counts, and Google domains JSON for post-run Lusha debugging. Lusha V2 now retries individual searches with cleaned company names (legal suffix/noise stripped) and retries company-domain searches without `jobTitles` when title-filtered search returns no contacts; diagnostics now include `company_name_cleaned`, `lusha_search_mode`, and `domain_used`.)
2026-05-07 update: Otodom/Warsaw Lusha enrichment now skips Otodom `private` sellers before Lusha candidate generation, and company-domain search tries a four-step fallback cascade: job-title + country, job-title global, domain-only + country, domain-only global. `LushaClient.search_contacts_by_domain` accepts `country=None`, and diagnostics now keep the exact successful or final failed `lusha_search_mode`/`domain_used` for better post-run analysis. Lusha company search now uses the current Prospecting flow (`/prospecting/contact/search` followed by `/prospecting/contact/enrich`) with the cleaned company name, while preserving the older `/v2/contacts/search` fallback for compatibility. Prospecting reveal volume is capped by `LUSHA_MAX_REVEALS_PER_AGENCY` (default 5) before final top-3 consolidation, but Prospecting search page size remains 20 because Lusha rejects too-small search pages; reveal now walks prospects in capped batches until it collects the target number of usable emails or exhausts the first search page. Otodom low-value person-only agency names are skipped, Otodom enrichment is now company-first (no individual person Lusha calls, because Warsaw diagnostics showed they consistently return no contacts), and social/marketplace domains (`facebook.com`, `instagram.com`, `linkedin.com`, `youtube.com`, `otodom.pl`, `realtor.com`) are filtered out before Lusha calls. `ls_filter_new_agencies` now receives normalized listings and prevents re-sourcing when an agency already has successful Lusha diagnostics, when an exact Lusha contact name already exists, or when every matching listing is on a building that already has a linked Lusha contact. Idealista Spain/Italy Lusha enrichment now tries source-specific agency-name variants after the raw Idealista commercial name, removing listing descriptors such as `Oficinas y Locales` and `Especialistas en oficinas` (for example `Knight Frank Oficinas y Locales` -> `Knight Frank`), while preserving the existing cleaned-first Otodom behavior. Google domain candidates are de-duplicated before Lusha calls to avoid repeated attempts against the same domain.
2026-05-12 update: `silver.location_scraper_globe_v2` now carries `hubspot_exported` and `hubspot_re_location_id` so the dashboard app can replace the HubSpot export button with a direct HubSpot RE Location link after export. The globe materializer preserves those HubSpot export fields across per-run rematerialization.
2026-05-12 update: `location_scraper_monthly` now starts one unlimited Durable scrape orchestration per month for Barcelona, Madrid, Milan, Berlin, Munich, Hamburg, Cologne, Frankfurt, Dusseldorf, Stuttgart, and Warsaw. Monthly runs set `unlimited_items=true`, omit Apify `maxItems`, and fetch the full Apify dataset; manual HTTP runs keep the existing default/env max item caps.
2026-05-21 update: Location Scraper map marker currency now flows from `silver.location_scraper_globe_v2.currency` into `gold.location_scraper_map_markers.currency` after the gold refresh. Added `scripts/sql_scripts/location_scraper_gold_map_markers_currency.sql` to add/backfill the gold column, and Otodom globe rematerialization now defaults missing currency to `PLN` instead of the generic `EUR` fallback while preserving explicit source currencies.
2026-06-24 update: **Competitor flexible-workspace classification** added — cleans `silver.competence_competitors` (15,319 rows / **8,433 unique domains**; the APIFY scrape only tags 3 flex-ish Google categories — Coworking space / Office space rental agency / Business center — so category can't separate real operators from brokers/virtual-office/noise; the **website** is the signal and `city` is garbage → use lat/lng). Two-tier, deduped by domain (one LLM call per operator, not per site): free category rules → Anthropic Haiku on the ambiguous middle (metadata → homepage-escalate when unsure), `shared/competence/classification.py` + `classifier_service.py`. New `silver.competence_competitor_classification` table (verdict per place_id) + clean `silver.competence_flex_competitors` view (DDL `scripts/sql_scripts/competence_classification.sql`, applied 2026-06-24). Nightly `functions/competence_classification.py` behind new `ENABLE_COMPETENCE_CLASSIFY` (default off, needs `ANTHROPIC_API_KEY`); one-off backfill `scripts/python_scripts/backfill_competitor_classification.py`; tests `tests/test_competence_classification.py` (16). Sample-validated 2026-06-24 (coworking ~20% junk, the other two ~40% → not auto-trusting coworking); backfill + deploy pending; code uncommitted.
Current branch: `main`
Maintainer: InfinitSpace Data Engineering Team
