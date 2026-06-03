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
    nexudus_invoice_pdf_cache.py
    nexudus_invoice_reconcile.py
    nexudus_silver_reconcile.py
    xero_sync.py
    finance_dashboard_refresh.py
    finance_invoice_worklist_refresh.py
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
| `refresh_invoice_worklist` | `functions/finance_invoice_worklist_refresh.py` | HTTP POST | `finance/refresh-invoice-worklist` | on-demand from finance dashboard; silver refresh (coworker_invoices → coworker_invoice_lines → coworkers) then `gold.sp_refresh_invoice_worklist`; skips user_access; auth_level=FUNCTION |
| `location_scraper_http` | `functions/location_scraper.py` | HTTP POST | `/api/scrape` | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — Durable Functions starter; returns 202 |
| `location_scraper_orch` | `functions/location_scraper.py` | orchestration | — | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — Durable orchestrator |
| `ls_*` (11 activities) | `functions/location_scraper.py` | activity | — | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — resolve / scrape / enrich / persist / log |

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
- `silver.nexudus_coworker_invoices` — includes `pdf_blob_path`, `pdf_cached_at`
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
  - rebuilt by `gold.sp_refresh_finance_dashboard`
- `gold.vw_landlord_current_contracts` (view — `scripts/sql_scripts/landlord_dashboard_schema.sql`)
  - one row per current-month active contract with full pricing
  - list price = SUM of `silver.nexudus_products.price` joined via `floor_plan_desk_ids`
  - sold price = `COALESCE(price_with_products, price, tariff_price)`
  - stop date = `cancellation_date` only; `contract_term` is informational column only
  - Includes: positive-fee contracts with a physical product (capacity > 0); also
    negative-fee adjustment contracts (discount/credit) with no product link,
    flagged via `is_negative_adjustment = 1`. Their negative `sold_monthly_fee`
    nets out of `vw_landlord_pricing_summary.sold_monthly_revenue` so revenue
    isn't overstated. `list_price_missing` is suppressed for adjustments.
- `gold.vw_landlord_contract_book_monthly` (view — `scripts/sql_scripts/landlord_dashboard_schema.sql`)
  - one row per location per month, ±12 months from current UTC month (25 rows per location)
  - active-in-month: `start_date <= EOMONTH(month)` AND `(cancellation_date IS NULL OR cancellation_date >= EOMONTH(month))`
    - the `>=` (not strict `>`) handles Nexudus's month-end cancellation convention:
      a contract with `cancellation_date = last_day_of_month` is `active=1` in Nexudus
      through its last day, so it must count as active for that month
  - `contract_term` NOT used as stop criterion — open-ended contracts stay active indefinitely
  - Includes future-signed contracts (`active=0 AND cancelled=0 AND start_date > today`)
    so they appear in the forecast from their start month forward; also includes
    negative-fee adjustment contracts (zero capacity, negative revenue impact).
    Surfaces `adjustment_contract_count` and `adjustment_monthly_value` per row.
  - columns: occupancy, workstation flow (new/cancelling), sold/list revenue, avg prices, discount
- `gold.vw_landlord_pricing_summary` (view — `scripts/sql_scripts/landlord_dashboard_schema.sql`)
  - one row per location for current month; aggregates `vw_landlord_current_contracts`
  - includes `product_match_coverage_pct` for list-price data quality QA
- `gold.vw_landlord_current_companies` (view — `scripts/sql_scripts/landlord_dashboard_schema.sql`)
  - one row per location + member_company_name; aggregates `vw_landlord_current_contracts`
  - filters: `location_source_id IS NOT NULL` AND `(capacity > 0 OR sold_monthly_fee > 0)`
  - pricing re-derived from summed totals (not row averages)
  - status priority: `notice_period > active > paused > inactive` across all contracts for that company
  - **Follow-up detection** (added 2026-05-27): exposes `has_open_ended_current_contract`,
    `has_followup_contract`, `followup_contract_count`, `followup_total_monthly_fee`,
    `earliest_followup_start`, `latest_followup_end_date`, `has_re_engagement`,
    `next_engagement_date`, `next_engagement_gap_days`, `re_engagement_contract_count`,
    `re_engagement_total_monthly_fee`, `latest_re_engagement_end_date`, `lifecycle_state`,
    and the **`effective_end_date`** column dashboards should display instead of
    `cancellation_date`. The raw aggregate `MAX(cancellation_date)` picks up the
    latest cancellation across all the company's current contracts (including
    discount lines), which misrepresents companies that have a follow-up signed
    or an open-ended office contract.
    - **Renewal (gap = 0)** → continuous occupancy under Nexudus's same-day cutover
      convention. Counted via `has_followup_contract`; extends `effective_end_date`
      to the latest follow-up end.
    - **Re-engagement (gap >= 1 day, any length up to many months)** → counted via
      `has_re_engagement`; the company IS leaving on `cancellation_date`
      (which becomes `effective_end_date`), and the dashboard should surface
      `next_engagement_date` + `next_engagement_gap_days` as "returning on X
      after N days". The forecast chart correctly shows zero contribution during
      the gap months because each contract is counted in its own active months only.
    - The 0-vs-1+ day boundary is intentional (not 7 days or any other arbitrary
      threshold): gap=0 reflects Nexudus's same-day handover convention, gap>=1
      means the contract truly lapses for at least one day so the company is
      not continuously present.
    - `lifecycle_state` is a convenience label: `'ongoing'` (open-ended current
      contract), `'renewing'` (continuous renewal, gap=0), `'returning'`
      (re-engagement only, has a gap), `'terminating'` (cancellation, no
      follow-up), `'active'` (no cancellation, no follow-up info).
    - `effective_end_date` is NULL when (a) any positive-fee current contract is
      open-ended, or (b) any continuous-renewal follow-up is open-ended; otherwise
      it returns the latest continuous-renewal end date if available, else falls
      back to the raw `cancellation_date`.
    - **Ancillary contracts deliberately excluded**: positive-fee non-desk lines
      (parking, business address, hot desk, bandwidth, network ports, etc.) are
      filtered out of `vw_landlord_current_contracts` via the
      `(capacity > 0 OR sold_monthly_fee < 0)` rule. The dashboard is
      desk-focused; ancillary revenue (~€12k/month dataset-wide) is intentionally
      not aggregated here. Use silver/finance views for full revenue.

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

### Nexudus Invoice PDF Storage

- PDFs are stored in Azure Blob Storage, not in SQL
- container: `nexudus-invoice-pdfs` on `staccinfinitspaceprod001`
- blob path format: `{location_source_id}/{yyyy}/{mm}/{invoice_source_id}.pdf`
- `silver.nexudus_coworker_invoices.pdf_blob_path` holds the reference
- `pdf_blob_path IS NULL` AND `updated_on >= 2 days ago` is the watermark — only recently-updated invoices missing a cached PDF are fetched each run
- invoices returning Nexudus server errors (500/502/503) are marked `__unavailable__` to avoid infinite retries
- timer function: `nexudus_invoice_pdf_cache` at 03:30 UTC

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
LOCATION_SCRAPER_WAVE_SIZE=3  # monthly cities scraped per sequential wave (OOM guard)

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
| Landlord dashboard gold views | done | 3 views in `scripts/sql_scripts/landlord_dashboard_schema.sql`: `gold.vw_landlord_current_contracts`, `gold.vw_landlord_contract_book_monthly` (±12 months), `gold.vw_landlord_pricing_summary`; cancellation_date-only semantics for forecasting; list price from product join |
| Soft-delete / source reconciliation | done | `is_deleted`/`deleted_at` on all Nexudus + BambooHR silver tables; daily `nexudus_invoice_reconcile` (invoices + cascaded lines), daily roster reconcile inside `bamboohr_sync`, weekly `nexudus_silver_reconcile` for other entities |
| Sync health report email | done | daily 06:00 UTC via Microsoft Graph; subject `[OK]`/`[FAIL]`; green/red table per entity + record-level error summary; sends to `SYNC_REPORT_RECIPIENTS` from `GRAPH_SENDER_UPN` |
| Location Scraper (Idealista + Otodom + IS24 + LoopNet) | done | HTTP-triggered Durable Functions pipeline; `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1`; Idealista (ES/IT) + Otodom (PL) + Immobilienscout24 (DE) + LoopNet (UK/London + US: New York, San Francisco, Palo Alto, Los Angeles, Austin, Seattle); Lusha enrichment via fan-out; free Nominatim geocode fallback when no `GOOGLE_MAPS_API_KEY`; bronze schema; see `docs/location_scraper.md` |

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

Last updated: 2026-06-03 (Location Scraper — **`host.json` activity concurrency lowered 10 → 2 to stop worker OOM (`python exited with code 137`)**. Even with sequential waves of 3, `maxConcurrentActivityFunctions=10` let up to ~6 memory-heavy activities (`ls_fetch_dataset`/`ls_persist_raw` load the full Apify payload set) run at once → the Python worker was SIGKILL'd for OOM (German IS24 cities + austin failed with a *NULL* `error_message`: a hard OOM kill dies before it can log). `maxConcurrentActivityFunctions` is now **2**, so at most 2 big datasets are in memory at any time, regardless of wave composition. `maxConcurrentOrchestratorFunctions` stays 5 (orchestrators are lightweight; memory lives in activities). Trade-off: the monthly batch runs more serially (fine — it is a once-a-month job). If a *single* huge city still OOMs alone, the next lever is capping its items (`LOCATION_SCRAPER_MAX_ITEMS`) or bumping the plan instance size. NB deploy = push to `main` (GitHub Actions).)
Previous: 2026-06-03 (Location Scraper — **monthly run now batched into sequential waves of 3 cities (fixes worker OOM / exit 137) + SQL-based retry of only failed cities** [+ follow-up fix: a re-trigger now terminates a hung parent instead of skipping it]. Problem: the monthly timer fanned out all 18 cities at once; with `host.json` `maxConcurrentActivityFunctions=10`, several memory-heavy Apify datasets (`ls_fetch_dataset`/`ls_persist_raw` load the full payload set — e.g. madrid 633 buildings) loaded simultaneously and the Python worker was SIGKILL'd for OOM (`python exited with code 137`). Solution in [functions/location_scraper.py](functions/location_scraper.py): the timer no longer starts 18 orchestrations directly — it starts **one parent orchestrator** `location_scraper_monthly_orch` (instance_id `location-scraper-monthly-{YYYY-MM}`, one per month). The parent processes cities in **sequential waves** of `_wave_size()` (default 3, env `LOCATION_SCRAPER_WAVE_SIZE`): each wave fans out its cities as **sub-orchestrations** (`call_sub_orchestrator("location_scraper_orch", …)`, no explicit child instance_id → Durable auto-assigns; the month-scoped `run_id` drives SQL/gold de-dup) and `task_all` waits before the next wave — so at most `wave_size` datasets are in memory at once. A wave failure is caught (try/except around `task_all`) and logged (`is_replaying`-guarded), so one failed city doesn't block later waves. **Retry semantics**: new activity `ls_cities_needing_run` queries `bronze.n8n_location_scraper_logs` (new helper `log_run.completed_run_ids`) and the parent **skips cities already `completed` this month**, so a re-trigger only retries failed/missing cities (no wasted Apify credits). New activity `ls_init_run_log` writes the per-city RUNNING row from inside the parent (orchestrators can't do I/O directly). **Parent-level lock** (replaces the per-city lock from the previous entry): a (re-)trigger always supersedes the current parent — a `running`/`pending`/`suspended` parent is **terminated** (`client.terminate`) then purged, a `completed`/`failed` parent is purged, and a fresh parent is started; SQL idempotency handles the rest, so a manual portal **Test/Run** always re-runs (retrying only failed cities). NB: the earlier "skip if running" variant deadlocked — a hung parent silently blocked every Test/Run (`Monthly location scraper parent already in progress; skipping`); terminating fixes that. Also: `log_run._UPDATE_LOG` and `_UPSERT_RUNNING_LOG` now clear `error_message` on success/re-run, so `completed` rows no longer carry a stale error (previously a city that failed then succeeded on retry showed `completed` with the old error text). No schema change (uses existing `error_message` column). Tests: 40 pass (resolve/loopnet/IS24/run_quality); module imports clean. NB still needs `func azure functionapp publish func-infinitspace-etl --python` — i.e. merge to `main` (GitHub Actions `main_func-infinitspace-etl.yml` deploys on push to `main`). Per-city `POST /api/scrape` path unchanged.)
Previous: 2026-06-03 (Location Scraper — **monthly re-run no longer blocked by a failed run (idempotency lock relaxed) + auto-purge of failed Durable instances**. Problem: the `location_scraper_monthly` timer skipped any city whose Durable instance already existed *in any state*, including `Failed`. So once a monthly run crashed (e.g. the surface_unit deploy skew below), re-triggering the timer skipped those cities forever — the only fix was a manual Durable purge. Solution in [functions/location_scraper.py](functions/location_scraper.py): the per-city skip now only triggers when the existing instance is in a **blocking** state (`running`/`pending`/`completed`/`continuedasnew`/`suspended` — i.e. in-progress or already-succeeded). A `Failed`/`Terminated`/`Canceled` instance is **non-blocking**: the timer logs it, calls `client.purge_instance_history(instance_id)` to clear the stale history, then re-runs the city under the same `instance_id`. `runtime_status` is compared on its trailing name, case-insensitively (`OrchestrationRuntimeStatus.Failed` → `failed`). Net effect: a failed city auto-retries on the next monthly fire **or** on a manual portal **Test/Run**, with no manual purge ever needed. Monthly idempotency for *successful*/*in-progress* runs is unchanged (still one run per city per month). No SQL/schema change. NB still needs `func azure functionapp publish func-infinitspace-etl --python` to deploy.)
Previous: 2026-06-03 (Location Scraper — **surface display unit (sqft for UK/US) + gold email-only filter**. Problem: every layer stored surface in m² only, but LoopNet (UK/US) listings are quoted in **square feet** — both outreach emails and the dashboard must show the local unit. Solution: keep a canonical m² *and* carry the display value + unit. **New columns across 3 layers** (`available_surface_m2` → renamed `surface_m2` in bronze; globe v2 / gold already used `surface_m2`): `surface_m2` (canonical m², used for sort/compare + the LoopNet ≥1500 m² guardrail), `surface_display` (value in display unit: native sqft for loopnet, = m² for idealista/otodom/IS24), `surface_unit` (`'sqft'`|`'m2'`). Code: [models.py](shared/location_scraper/models.py) (`Listing` fields + `from_dict` **backward-compat shim** mapping legacy `available_surface_m2` queue messages); [adapters/loopnet.py](shared/location_scraper/adapters/loopnet.py) new `available_surface_sqft_from_payload` (native sqft; `available_surface_m2_from_payload` now derives m² from it) → `surface_unit='sqft'`; the 3 EU adapters set `surface_display=surface_m2`, `surface_unit='m2'`; [activities/persist.py](shared/location_scraper/activities/persist.py) (`_INSERT_LISTING`); [activities/materialize_globe.py](shared/location_scraper/activities/materialize_globe.py) (`_map_row` computes display/unit, `_INSERT_ROW`). **Gold now keeps only buildings with a contact email** — `gold.sp_refresh_location_scraper_map_markers` gained `WHERE marker_rank = 1 AND lusha_email_1 IS NOT NULL` (the representative row is ranked to prefer rows with an email, so this drops markers where no listing has any email; `lusha_email_*` slots carry both Lusha emails and LoopNet broker emails). Gold also gains `total/min/max_surface_display` + `surface_unit`. **SQL**: schema source-of-truth updated in `location_scraper_schema.sql`, `location_scraper_globe_materialized_v2.sql`, `location_scraper_gold_map_markers_price_breakdown.sql`; new migration `scripts/sql_scripts/location_scraper_surface_unit_migration.sql` does the bronze rename + adds the silver columns + **backfills history** (loopnet rows: `unit='sqft'`, `display = surface_m2 / 0.092903`; others: `unit='m2'`, `display = surface_m2`). **Apply order**: (1) surface_unit_migration.sql, (2) globe_materialized_v2.sql, (3) gold_map_markers_price_breakdown.sql, (4) `EXEC gold.sp_refresh_location_scraper_map_markers`. Tests updated (loopnet/IS24/idealista/berlin) — 74 pass; the 7 berlin_simulation failures remain pre-existing (city/region fixture mismatch, unrelated). NB still needs `func azure functionapp publish func-infinitspace-etl --python`. Idealista/Otodom/IS24 normalization logic otherwise unchanged.)
Previous: 2026-06-02 (Location Scraper — **LoopNet US cities added (New York, San Francisco, Palo Alto, Los Angeles, Austin, Seattle)**. The existing LoopNet adapter + `memo23/loopnet-scraper-ppe` actor (`0ZCQONxB3BdyOzrbD`) is reused — no adapter/orchestrator changes. New `COUNTRY_CONFIG["us"]` block in [shared/location_scraper/config.py](shared/location_scraper/config.py): `country_code="us"`, `actor="loopnet"`, **`property_path="office-space"` + `filter_suffix="for-lease"`** (US LoopNet uses a different URL shape than UK's `office-properties`/`for-rent`; verified against the actor docs + live loopnet.com URLs). City slugs are `{city}-{state}`: `new-york-ny`, `san-francisco-ca`, `palo-alto-ca`, `los-angeles-ca`, `austin-tx`, `seattle-wa`. The shared `loopnet` branch in [shared/location_scraper/activities/resolve.py](shared/location_scraper/activities/resolve.py) already builds `…/search/{property_path}/{slug}/{filter_suffix}/`, so US resolves to e.g. `https://www.loopnet.com/search/office-space/new-york-ny/for-lease/`. Currency derives from country (non-GB → USD) in `currency_for_country`. **Multi-word city names contain a space** (`new york`) — kept as-is for resolve matching (config keys + `COUNTRY_NAME_BY_SOURCE["loopnet"]` use the spaced form → "United States" for geocode fallback), but the monthly timer in [functions/location_scraper.py](functions/location_scraper.py) now **slugifies** the city (`new york`→`new-york`) when building `run_id`/Durable `instance_id` (spaces are unsafe there). All 6 US cities added to `MONTHLY_CITIES`. Lusha still skipped for loopnet (broker email comes from payload). Tests: added US cases to `tests/test_location_scraper_resolve_source.py` (29 pass). NB still needs `func azure functionapp publish func-infinitspace-etl --python` to deploy; per-city runs can be triggered now via `POST /api/scrape {"City":"New York"}`. Idealista/Otodom/IS24/LoopNet-UK logic unchanged.)
Previous: 2026-06-02 (Location Scraper — **monthly scrape failure fix (apify-client version mismatch)**. The first-ever firing of the `location_scraper_monthly` timer (01:00 UTC on 2026-06-01, cron `0 0 1 1 * *`) started all 11 city Apify runs successfully but every Durable orchestration was marked `failed` ~2s later. Root cause recovered from the Durable task-hub table `funcinfinitspaceetlInstances`: `Activity 'ls_start_apify_run' failed: TypeError: 'Run' object is not subscriptable`. `requirements.txt` had `apify-client>=1.7.0` (unpinned); the prod build pulled a newer apify-client where `Actor.start()` / `run().get()` return a typed `Run` **object** instead of a subscriptable dict, so `run["id"]` in [shared/location_scraper/clients/apify.py](shared/location_scraper/clients/apify.py) threw. The Apify actors themselves ran fine — only the orchestrator's dict access broke, which is why the runs were lost despite Apify succeeding. **Fixes:** (1) pinned `apify-client==2.5.0` in `requirements.txt`; (2) added `_run_field()` helper in `apify.py` that reads run fields from either a dict or a `Run` object (used by `start_run`, `get_run_status`, `run_sync`) so a future version bump can't re-break it; (3) errors are now persisted to SQL — new `error_message NVARCHAR(MAX)` column on `bronze.n8n_location_scraper_logs` (migration `scripts/sql_scripts/location_scraper_logs_error_message.sql`, already applied to prod DB) written by `mark_run_failed` in [shared/location_scraper/activities/log_run.py](shared/location_scraper/activities/log_run.py) with a graceful fallback if the column is absent. Previously failures only went to App Insights, and there was no June-1 telemetry, which made diagnosis require the Durable history table. NB still needs `func azure functionapp publish func-infinitspace-etl --python` to deploy; the monthly run can be re-triggered per city via `POST /api/scrape`.)
Previous: 2026-06-02 (Location Scraper — **LoopNet (UK / London) added as a new source**. New adapter [shared/location_scraper/adapters/loopnet.py](shared/location_scraper/adapters/loopnet.py) (registered in `adapters/registry.py`), new `COUNTRY_CONFIG["uk"]` block in [shared/location_scraper/config.py](shared/location_scraper/config.py) with `LOOPNET_ACTOR_ID = "0ZCQONxB3BdyOzrbD"` (memo23 **pay-per-event** actor `memo23/loopnet-scraper-ppe`; the $31/mo flat-rate twin `RuOxoBM1bnc5pQ3TJ` is deliberately NOT used), and a `loopnet` URL branch in [shared/location_scraper/activities/resolve.py](shared/location_scraper/activities/resolve.py) producing `…/search/office-properties/london-england--united-kingdom/for-rent/`. **City slug must include region + country** (`london-england--united-kingdom`) — the actor geocodes the search area from the URL; a bare `london` slug fails. Key behaviours: (1) **areas are square feet → m²** (×0.092903) via `available_surface_m2_from_payload` (parses `header.subtext` "X SF … Available", falls back to summed `spaces[].size`); (2) **≥1500 m² hard floor** enforced both in the adapter (`normalize` returns None) AND in globe materialization (`_map_row` returns None, filtered in `materialize_globe_run`) — LoopNet's URL size filter is unreliable (filters total building size, not available area); (3) LoopNet payloads carry **no coordinates** → filled by the geocode fallback; (4) **broker name/company/phone/email come straight from the payload**, so `Listing.email` is populated and `matching_name()` uses `company_name` for loopnet. **Free geocoding**: added [shared/location_scraper/free_geocoding.py](shared/location_scraper/free_geocoding.py) (`NominatimGeocodingCache`, OpenStreetMap, no API key, 1 req/s, same `get_or_geocode` interface as `GeocodingCache`). `activities/scrape.py` + `activities/materialize_globe.py` now use Google Maps when `GOOGLE_MAPS_API_KEY` is set, else the free Nominatim geocoder (selector helper `get_geocoding_cache()` in `geocoding.py`; `COUNTRY_NAME_BY_SOURCE["loopnet"]["london"]="United Kingdom"`). `london` added to `MONTHLY_CITIES`. Tests: `tests/test_location_scraper_loopnet_adapter.py` (new) + LoopNet cases in `tests/test_location_scraper_resolve_source.py`; fixed `test_normalize_idealista_missing_coords` to stub the geocoder (stay offline). **Lusha skipped for LoopNet**: `LUSHA_SKIP_SOURCES = {"loopnet"}` in `functions/location_scraper.py` makes the orchestrator bypass dedupe/enrich/consolidate for LoopNet (broker email observed at 100% coverage on ≥1500 m² London listings). The broker contact is still persisted to `bronze.n8n_location_scraper_contacts` (`source='scraper'`) by `ls_upsert_sql`, and broker email(s) are surfaced in the globe email slots via new `_loopnet_broker_contacts` in `materialize_globe.py`. Optional env: `NOMINATIM_URL`, `NOMINATIM_USER_AGENT`. NB: 7 pre-existing failures in `tests/test_location_scraper_berlin_simulation.py` are unrelated (test fixture sets `region` as a neighbourhood while the IS24 adapter treats `region` as the city). Idealista/Otodom/IS24 logic unchanged.)
Previous: 2026-05-26 (Location Scraper IS24 price extraction — silver + gold: office "buero-mieten" listings on Immobilienscout24 are tarifés "from X €/m²" when divisible, so the Apify actor leaves `normalized.price.amount = null` and our globe materializer was dropping the price entirely. **Silver layer** — added 5 columns to `silver.location_scraper_globe_v2` via `scripts/sql_scripts/location_scraper_globe_v2_price_breakdown.sql`: `additional_costs_per_m2` (Nebenkosten/m²), `total_price_per_m2`, `divisible_from_m2` (Teilbar ab), `price_kind` (RENT_NET / RENT_GROSS), `price_monthly_is_estimated` (BIT). [shared/location_scraper/activities/materialize_globe.py](shared/location_scraper/activities/materialize_globe.py) now extracts from IS24 `sections[type=TOP_ATTRIBUTES]` + `sections[type=ATTRIBUTE_LIST title=Kosten]` via new helpers `_iter_is24_attributes`, `_is24_attr_decimal`, `_parse_eu_number`. Cascade: when `price_monthly` is null, it's derived as `price_per_m2 × surface_m2` (base net rent only) and flagged `price_monthly_is_estimated=1`. Re-materialize existing IS24 runs without re-scraping via `scripts/python_scripts/rematerialize_globe_is24.py` — raw payloads are conserved in `bronze.location_scraper_raw.payload_json`. **Gold layer** — added 8 columns to `gold.location_scraper_map_markers` (`price_per_m2`, `min_price_per_m2`, `max_price_per_m2`, `additional_costs_per_m2`, `total_price_per_m2`, `divisible_from_m2`, `price_kind`, `price_monthly_is_estimated`) via `scripts/sql_scripts/location_scraper_gold_map_markers_price_breakdown.sql`; `CREATE OR ALTER PROCEDURE gold.sp_refresh_location_scraper_map_markers` includes them (representative row values for breakdown, MIN/MAX OVER PARTITION for `*_price_per_m2`, MIN for `divisible_from_m2`). Dashboard reads from gold, so the new pricing fields surface once the dashboard adds a "Pricing" section to its detail panel. Idealista/Otodom logic unchanged.)
Previous: 2026-05-27 (Landlord dashboard fix — follow-up contract surfacing + cutover double-count fix: (A) `gold.vw_landlord_current_companies` gains `has_open_ended_current_contract`, `has_followup_contract`, `followup_contract_count`, `followup_total_monthly_fee`, `earliest_followup_start`, `latest_followup_end_date`, and `effective_end_date`. The `effective_end_date` is the value dashboards should display in place of the raw aggregate `cancellation_date` — it is NULL when an open-ended current contract exists (e.g. ADP Nederland's offices through 2028 with a discount cancelling 2026-07-31) or when a follow-up is genuinely open-ended (RxSight), and otherwise extends to the latest follow-up end date (Allianz: shown 30/06/2026 raw → corrected to 31/12/2026 via follow-up). Detected 32 companies across all locations that had hidden follow-ups; the colleague's manual "Not terminating - check replacement product" annotation is now derivable from the data. (B) `gold.vw_landlord_contract_book_monthly` and `gold.vw_landlord_current_contracts` gain timezone-aware effective_start_date logic: `CAST(DATEADD(HOUR, 4, c.start_date) AS DATE)` converts Nexudus's UTC 22:00 end-of-day timestamps to local-time next-day start. This fixes a chart-side regression where the prior month-end-cancellation fix caused cutover contracts (new contract starts same day old contract ends) to be double-counted in the cutover month. Found 55 such same-day cutovers; the most material was Allianz at Taurusavenue where June 2026 was overstated by €6,600 + ~20 desks. Deployed via `scripts/python_scripts/apply_landlord_fix.py` (now also includes `vw_landlord_current_companies`). See investigation reports: `termination_investigation.txt`.)
Previous: 2026-05-27 (Landlord dashboard fix — month-end cancellation convention: changed the active-in-month rule in `gold.vw_landlord_current_contracts` and `gold.vw_landlord_contract_book_monthly` from strict `cancellation_date > EOMONTH(month_start)` to `cancellation_date >= EOMONTH(month_start)`. Reason: Nexudus's convention is to set `cancellation_date` to the LAST DAY of a contract's final billable month (e.g. a discount that applies Apr + May has `cancellation_date = 2026-05-31`), and Nexudus keeps `active = 1` through that day. The previous strict-`>` rule silently dropped these contracts from their own final month, overstating revenue (positives dropped) and understating discounts (negatives dropped). The new rule aligns with `gold.vw_finance_dashboard_membership_schedule`'s behaviour. Concrete impact for May 2026: Cainiao (Netherlands) B.V. (Alibaba)'s -€13,650 discount now correctly nets to €0 net revenue (was overstated as +€13,650); Amsterdam-Taurusavenue 3 monthly revenue corrected from €63,041 to €49,391. Same `>` → `>=` change applied to the notice-period branch of the status filter for consistency. `is_cancelling_this_month` flag unchanged — its semantic still works correctly under the new rule. Deployed via `scripts/python_scripts/apply_landlord_fix.py` (idempotent CREATE OR ALTER VIEW). See investigation reports: `partnership_data_findings.txt`, `cainiao_investigation.txt`, `cainiao_finance_investigation.txt`.)
Previous: 2026-05-26 (Landlord dashboard fix: `scripts/sql_scripts/landlord_dashboard_schema.sql` now considers two contract categories that were silently dropped: (1) **future-signed contracts** (`active=0 AND cancelled=0 AND start_date > today`) now appear in `gold.vw_landlord_contract_book_monthly` from their start month forward — previously the `active=1 OR cancelled=1` filter excluded them from the ±12-month forecast; (2) **negative-fee adjustment contracts** (Nexudus allows `price < 0` for discounts/credits/refunds — they typically have no `floor_plan_desk_ids` so the `pl.capacity > 0` filter dropped them) are now included with zero capacity and netted into `sold_monthly_revenue` so revenue isn't overstated. New column `is_negative_adjustment` on `vw_landlord_current_contracts`; new columns `adjustment_contract_count` + `adjustment_monthly_value` on both `vw_landlord_contract_book_monthly` and `vw_landlord_pricing_summary`. `list_price_missing` is suppressed for negative adjustments, and `product_match_coverage_pct` / `active_contract_count` now exclude adjustments from their counts. Added QA 9 (future-contract forecast presence) and QA 10 (negative-adjustment inventory). Current-month view `vw_landlord_current_contracts` still excludes future starts — they belong only to the forward forecast.)
Previous: 2026-05-20 (Landlord dashboard: added 3 gold views in `scripts/sql_scripts/landlord_dashboard_schema.sql` — `gold.vw_landlord_current_contracts` (pre-filtered current-month contracts; status filter built into WHERE so Flask reads without further filtering; LEFT JOIN to product_link so contracts without product resolution are included with `list_price_missing=1` rather than silently dropped; abandoned contracts `active=0 AND cancelled=0` excluded), `gold.vw_landlord_contract_book_monthly` (±12 months = 25 rows per location; same status filter applied to contract_facts so abandoned contracts don't inflate history; `contracts_missing_list_price` column for data quality), `gold.vw_landlord_pricing_summary` (current-month KPI aggregation; `product_match_coverage_pct` and `contracts_missing_list_price` for QA). Key semantic change: only `cancellation_date` is a hard stop; `contract_term` is informational only. 8 QA queries included, commented out. No HubSpot pipeline view — no deal pipeline data in ETL.)
Previous: 2026-05-12 (Observability: `nexudus_to_bronze` now wraps its entire body in `RunTracker("nexudus", "bronze_sync", "bronze")` so auth failures are recorded to `meta.sync_runs` and visible in the health report. `sync_health_report` gained an `_EXPECTED_DAILY` list of 5 critical orchestrator-level entries and a `_find_missing_runs()` check — any expected (source, entity, layer) absent from the window renders as a yellow ⚠ "never started" row, counts toward `[FAIL]` subject, and appears in the summary line as "N never started".)
Previous: 2026-05-11 (Finance dashboard: added on-demand invoice worklist refresh endpoint `POST /api/finance/refresh-invoice-worklist` (`functions/finance_invoice_worklist_refresh.py`); refreshes silver coworker_invoices → coworker_invoice_lines → coworkers then calls new `gold.sp_refresh_invoice_worklist` (invoice worklist only, skips user_access); SQL procedure in `scripts/sql_scripts/finance_invoice_worklist_sp.sql`; registered under `ENABLE_ETL_FUNCTIONS=1`; all steps tracked in `meta.sync_runs` with `triggered_by=http`.)
Previous: 2026-05-06 (Location Scraper: added JSON schema discovery script `scripts/sql_scripts/location_scraper_raw_schema_discovery.sql` to profile `payload_json` paths/types/coverage for future globe view mapping; docs updated accordingly. Orchestrator now marks failed runs via new `ls_mark_run_failed` activity calling `mark_run_failed`, preventing stale `running` status on errors. Fixed `location_scraper_run_quality` insert placeholder mismatch in `shared/location_scraper/activities/log_run.py` so quality rows persist correctly. Scraper volume cap is now configurable via common env `LOCATION_SCRAPER_MAX_ITEMS` across all actors (`idealista`, `otodom`, `immobilienscout`) with fallback defaults; tests updated. SQL client retry logic now treats AAD/local transient network states (`HYT00`, `08001`, `08S01`) as retryable to reduce intermittent local hangs while keeping passwordless auth mode. Added `docs/location_scraper_source_mapping.md` with source-specific JSON mapping across all active sources: immobilienscout, idealista, and otodom. Added `scripts/sql_scripts/location_scraper_globe_materialized_v2.sql` for materialized app read table DDL (`silver.location_scraper_globe_v2`). raw -> silver materialization is now handled in Function App code via new activity `ls_materialize_globe` (`shared/location_scraper/activities/materialize_globe.py`) using per-run delete+insert into silver table. Globe materialization now reuses existing bronze Lusha contacts to populate top-3 email/contact/title/confidence slots in `silver.location_scraper_globe_v2`, and Idealista country fallback now resolves from configured city so Milan maps to `IT` while Spanish cities map to `ES`. Added `scripts/sql_scripts/location_scraper_globe_quality.sql` to create `silver.location_scraper_globe_quality` plus refresh proc `silver.sp_refresh_location_scraper_globe_quality`, enabling run/source/city data-quality analysis for globe coverage, duplicates, raw-to-silver deltas, and Lusha email coverage. The Durable orchestrator now calls `ls_refresh_globe_quality` after `ls_materialize_globe`, so the quality table updates automatically on each successful run. Otodom globe materialization now extracts individual `contact_name` from non-agency keys in `sellerPhones` and uses the matching phone number, fixing zero contact-name coverage when raw payloads contain person names. Otodom Lusha enrichment now keeps multiple distinct individual broker candidates per agency instead of one candidate per agency, caps individual candidates per agency, allows only one company fallback per agency to avoid repeated Google/Lusha calls, and Lusha company search titles were expanded with broker/agent/advisor variants including Polish real-estate terms. Location Scraper log handling now upserts running rows, sets `updated_at`, marks stale `running` rows failed on new run startup (`LOCATION_SCRAPER_STALE_RUNNING_HOURS`, default 2), safely attempts to mark failed from critical activities, and includes `scripts/sql_scripts/location_scraper_logs_hardening.sql` for DB cleanup/default hardening. Added `bronze.location_scraper_lusha_diagnostics` via `scripts/sql_scripts/location_scraper_raw_and_quality.sql`; the orchestrator writes one row per Lusha enrichment candidate through `ls_write_lusha_diagnostics`, including `run_id`, source/city, agency/person, path/reason, raw/final contact counts, and Google domains JSON for post-run Lusha debugging. Lusha V2 now retries individual searches with cleaned company names (legal suffix/noise stripped) and retries company-domain searches without `jobTitles` when title-filtered search returns no contacts; diagnostics now include `company_name_cleaned`, `lusha_search_mode`, and `domain_used`.)
Current branch: `main`
Maintainer: InfinitSpace Data Engineering Team
