# CLAUDE.md -- InfinitSpace Data Warehouse

> Self-updating protocol: any agent that changes this project must update this file before finishing.

---

## Project Overview

InfinitSpace Data Warehouse is a Python 3.11 Azure Functions ETL project that moves operational data into Azure SQL across these layers:

- `bronze`: raw source payloads
- `silver`: typed and normalized entities
- `ava`: denormalized product availability + per-location plans
- `core`: planned, not implemented

Primary sources today:

- Nexudus API (incl. events: calendar events, attendees, ticket products)
- Xero API
- CoStar PDF extractor (Real Estate HTTP function)
- Google Maps enrichment utilities exist but are not part of the scheduled Function App
- Location Scraper (HTTP-triggered Durable Functions pipeline — Idealista + Otodom)
- HubSpot Marketing Email API (gated, `ENABLE_HUBSPOT_FUNCTIONS=1`)
- Eventbrite API (gated, `ENABLE_EVENTBRITE_FUNCTIONS=1`)

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
5. `03:00` `refresh_ava_availability` (rebuilds `ava.product_availability`, then `ava.location_plans`, then the desk-price audit)
6. `03:30` `nexudus_invoice_pdf_cache` (caches PDFs for invoices missing `pdf_blob_path`)
7. `04:00` `xero_invoice_sync` (includes PDF caching for invoices missing `pdf_blob_path`)
8. `05:00` `bamboohr_sync` (includes daily employee roster reconcile)
9. `05:15` `nexudus_invoice_reconcile` (daily soft-delete of removed invoices + cascaded lines)
10. `05:30` `replyio_stats_sync`
11. `05:30` `refresh_finance_dashboard` (guarded: rolls back + fails the run if the invoice worklist collapses below 50% of the live count)
12. `06:00` `sync_health_report` (emails green/red daily report via Microsoft Graph)
13. `10:00` `refresh_finance_dashboard_recheck` (re-runs the same guarded rebuild after the early-morning data settles, so an under-built 05:30 worklist is republished in full)

Optional gated surface (not in the default deployment):

- `Mon-Sat 04:30` `competence_sync` (Firebase `competence_new` -> bronze -> silver; **incremental** — only competitors changed since the last run, via an `updated_at` collection-group watermark)
- `Sun 04:00` `competence_full_reconcile` (full read of `competence_new` + soft-delete reconcile; deletes can only be observed by a full pass)
- both only when `ENABLE_COMPETENCE_FUNCTIONS=1` and a Firebase credential is set; the incremental read needs a Firestore index on `competitors.updated_at`
- `05:45` `hubspot_sync` (marketing emails + KPI stats -> bronze -> silver + embedded soft-delete reconcile; only when `ENABLE_HUBSPOT_FUNCTIONS=1` and `HUBSPOT_ACCESS_TOKEN` is set)
- `05:50` `eventbrite_sync` (all events, all orgs/statuses, with venue/ticket expansions -> bronze -> silver + embedded soft-delete reconcile; only when `ENABLE_EVENTBRITE_FUNCTIONS=1` and `EVENTBRITE_PRIVATE_TOKEN` is set)

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
     + _resync_open_invoices            (by-ID re-fetch of open unpaid invoices, ignores UpdatedOn — keeps DueAmount fresh)
  -> bronze.nexudus_coworkers           (full list via UpdatedSince watermark)
  -> bronze.nexudus_resources
  -> bronze.nexudus_extra_services
  -> bronze.nexudus_coworker_invoice_lines (per-invoice line items)
  -> bronze.nexudus_calendar_events     (incremental via UpdatedSince watermark)
  -> bronze.nexudus_event_attendees     (incremental via UpdatedSince watermark)
  -> bronze.nexudus_event_products      (incremental via UpdatedSince watermark)
  -> blob snapshots (nexudus-raw-snapshots container)

bronze_to_silver
  -> Azure Storage Queue: silver-sync-tasks
  -> silver_entity_worker x 13
  -> silver.nexudus_locations
  -> silver.nexudus_location_hours
  -> silver.nexudus_products
  -> silver.nexudus_contracts
  -> silver.nexudus_coworker_invoices
  -> silver.nexudus_coworkers
  -> silver.nexudus_resources
  -> silver.nexudus_extra_services
  -> silver.nexudus_coworker_invoice_lines
  -> silver.nexudus_calendar_events     (location via BusinessId)
  -> silver.nexudus_event_attendees     (links: event, coworker, ticket, invoice)
  -> silver.nexudus_event_products      (location inherited from parent event)

nexudus_invoice_pdf_cache (timer, 03:30 UTC)
  -> downloads PDFs from Nexudus API for recent invoices missing pdf_blob_path (last 2 days)
  -> uploads to Azure Blob: nexudus-invoice-pdfs container
  -> updates silver.nexudus_coworker_invoices.pdf_blob_path
  -> marks invoices returning server errors with '__unavailable__' sentinel to avoid retries

refresh_ava_availability
  -> EXEC ava.sp_refresh_product_availability
  -> ava.product_availability
  -> EXEC ava.sp_refresh_location_plans          (filtered serving view of silver.nexudus_tariffs)
  -> ava.location_plans                          (per-location plans; desks/offices + €0 plans excluded)

Xero API
  -> xero_invoice_sync
  -> bronze.xero_invoices
  -> silver.xero_invoices (pdf_blob_path populated when PDF is cached)
  -> silver.xero_invoice_line_items
  -> silver.xero_tenants
  -> xero.silver_tenants (view alias)
  -> optional bronze.xero_invoice_pdfs
  -> (same timer, after PDF caching) bank transactions:
     bronze.xero_bank_transactions
     silver.xero_bank_transactions
     silver.xero_bank_transaction_line_items
     silver.vw_xero_bank_transaction_pnl_lines   (P&L serving view: net-of-tax, SPEND/RECEIVE only)
     -- requires accounting.banktransactions[.read]; tenants on a token missing
        the scope are skipped with a warning until the OAuth re-consent lands
  -> (same timer, last step) monthly Profit & Loss reports:
     GET /Reports/ProfitAndLoss?standardLayout=true, one call per tenant x month
     bronze.xero_profit_loss_reports             (raw report payloads)
     silver.xero_profit_loss_accounts            (one row per tenant x month x account + summary rows)
     silver.vw_xero_profit_loss_monthly_accounts (budget-tool serving view)
     -- backfill from 2024-01, re-pulls the last 3 months each run (late postings);
        requires accounting.reports.profitandloss.read (granular; the broad
        accounting.reports.read is REJECTED for post-2026-03 apps)

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

refresh_finance_dashboard (timer, 05:30 UTC) + refresh_finance_dashboard_recheck (timer, 10:00 UTC)
  -> EXEC gold.sp_refresh_finance_dashboard          (inside a guarded transaction)
  -> gold.finance_dashboard_user_access (BambooHR -> Nexudus locations)
  -> gold.finance_dashboard_invoice_worklist (Nexudus invoices, Nexudus-only)
  -> guardrail: roll back + fail the run if the worklist collapses < 50% of the live count

Firebase competence_new (optional, ENABLE_COMPETENCE_FUNCTIONS=1)
  -> competence_sync (timer, Mon-Sat 04:30 UTC)        incremental (updated_at watermark)
  -> competence_full_reconcile (timer, Sun 04:00 UTC)  full read + soft-delete
  -> bronze.competence_lists
  -> bronze.competence_competitors
  -> silver.competence_lists
  -> silver.competence_competitors        (is_deleted reconciled weekly)

HubSpot Marketing Email API (optional, ENABLE_HUBSPOT_FUNCTIONS=1)
  -> hubspot_sync (timer, 05:45 UTC)      full fetch + includeStats (stats keep changing)
  -> bronze.hubspot_marketing_emails      (latest-payload MERGE, SHA-256 hash-dedup)
  -> silver.hubspot_marketing_emails      (subject/body/content + KPI counters & ratios;
                                           is_deleted reconciled daily, embedded)

Eventbrite API (optional, ENABLE_EVENTBRITE_FUNCTIONS=1)
  -> eventbrite_sync (timer, 05:50 UTC)   /users/me/organizations -> per-org events
                                          (status=all, expand=venue,ticket_availability,...)
  -> bronze.eventbrite_events             (latest-payload MERGE, SHA-256 hash-dedup)
  -> silver.eventbrite_events             (schedule/venue/tickets flattened;
                                           is_deleted reconciled daily, embedded)
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
- `ENABLE_COMPETENCE_FUNCTIONS=0`

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
    competence_sync.py
    hubspot_sync.py
    eventbrite_sync.py
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
      silver_writer_calendar_events.py
      silver_writer_event_attendees.py
      silver_writer_event_products.py
      hubspot_bronze_writer.py
      silver_writer_hubspot.py
      eventbrite_bronze_writer.py
      silver_writer_eventbrite.py
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
        calendar_events.py
        event_attendees.py
        event_products.py
    bamboohr/
      __init__.py
      client.py
      transformers/
        employees.py
    hubspot/
      __init__.py
      client.py              (Marketing Email API v3, HUBSPOT_ACCESS_TOKEN)
      transformers/
        marketing_emails.py
    eventbrite/
      __init__.py
      client.py              (API v3, EVENTBRITE_PRIVATE_TOKEN, continuation pagination)
      transformers/
        events.py
    firebase/
      __init__.py
      client.py              (get_firestore_client — firebase-admin, FIREBASE_CREDENTIALS)
      competence.py          (read_competence — competence_new lists + competitors)
      transformers/
        competence.py        (pure transform_competence_list / transform_competitor)
    xero/
      oauth.py
      flow.py
      token_cipher.py
      store.py
      client.py
      invoice_sync.py
      bank_transaction_sync.py
      profit_loss_sync.py
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
      test_competence_sync.py
      backfill_competence_competitor_country.py   (one-off: fill silver competitor country/country_code)
      backfill_broker_directory.py                (seed silver.location_scraper_broker_directory from the raw LoopNet archive; --write / --rematerialize)
      backfill_nexudus_coworkers.py               (one-off: full coworker list backfill after the list-endpoint switch)
      resync_open_invoices.py                     (backfill: re-fetch open invoices by ID -> bronze + silver + gold; fixes stale DueAmount from the UpdatedOn-freeze bug; dry-run / --apply)
      test_events_sync.py                         (Nexudus events: dry-run / --write end-to-end)
      inspect_nexudus_events.py                   (field-shape discovery for the event endpoints)
      test_hubspot_sync.py                        (HubSpot: dry-run incl. stats key inventory / --write)
      test_eventbrite_sync.py                     (Eventbrite: dry-run / --write)
      test_bank_transactions_sync.py              (Xero bank transactions: scope probe + volumetry / --write)
      xero_sync_profit_loss.py                    (Xero monthly P&L: --dry-run / backfill runner)
      apply_schema_script.py                      (apply a GO-batched .sql script to the warehouse DB)
      teamandy_migration/                         (one-shot TeamAndy Firestore -> Azure migration ETL, ported from AI-teamandy)
        common.py                                 (NDJSON codec, value coercers, SQL + Firestore client bridge, teamandy schema const)
        manifest.py                               (Firestore collection -> teamandy table map, children/hooks, EXCLUDE_* sets, load order)
        extract.py                                (Firestore -> _work/ndjson per collection + freeze-time counts; READ-ONLY)
        transform.py                              (ndjson -> per-table row files with named hooks; coercion failures -> rejects, never abort)
        load_sql.py                               (batched load into teamandy.* tables; per-batch commit + row-by-row retry on reject)
        load_tables.py                            (cache collections -> Azure Table Storage; bodies >32 KB spill to Blob cache-bodies)
        validate.py                               (post-load row-count gate vs freeze-time counts; dual-shape + soft-FK orphan report)
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
      ava_location_plans_schema.sql                 (ava.location_plans table + sp_refresh_location_plans)
      integrations_nexudus_xero_schema.sql
      nexudus_billing_sync_schema.sql
      nexudus_coworkers_tariff_name_widen_migration.sql   (widens silver coworker tariff_name/next_tariff_name to NVARCHAR(MAX))
      nexudus_coworker_invoice_lines_schema.sql
      xero_invoices_schema.sql
      xero_bank_transactions_schema.sql             (bronze + silver + meta watermarks + P&L view)
      xero_profit_loss_schema.sql                   (bronze + silver + meta watermarks + monthly-accounts view)
      xero_pdf_blob_migration.sql
      competence_schema.sql
      competence_competitor_country_migration.sql   (adds silver competitor country column)
      location_scraper_broker_directory.sql         (silver.location_scraper_broker_directory — LoopNet broker name→email memory)
      nexudus_events_schema.sql                     (bronze + silver: calendar_events, event_attendees, event_products)
      hubspot_marketing_emails_schema.sql           (bronze + silver)
      eventbrite_events_schema.sql                  (bronze + silver)
      teamandy_00_schema.sql                        (TeamAndy migration: CREATE SCHEMA teamandy — no ALTER DATABASE, shared DB, RCSI already on)
      teamandy_10_core_crm.sql                      (TeamAndy migration: 21 core CRM tables — leads, contacts, lead_lists, locations, campaigns, …)
      teamandy_20_scraping_ref.sql                  (TeamAndy migration: 9 scraping/reference tables)
      teamandy_30_ops_config.sql                    (TeamAndy migration: 8 ops/config tables)
      teamandy_40_runtime.sql                       (TeamAndy migration: graph_subscriptions runtime table)
      teamandy_50_company_index.sql                 (TeamAndy migration: 3 company-index tables)
      test.sql
  tests/
    test_ava_refresh.py
    test_nexudus_resource_transformer.py
    test_silver_sync.py
    test_xero_integration.py
    test_xero_bank_transactions.py
    test_xero_profit_loss.py
    test_xero_tenant_directory.py
    test_xero_nexudus_invoice_linking.py
    test_location_scraper_broker_directory.py
    test_nexudus_event_transformers.py
    test_hubspot_transformer.py
    test_eventbrite_transformer.py
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
| `refresh_ava_availability` | `functions/ava_refresh.py` | timer | `0 0 3 * * *` | executes `ava.sp_refresh_product_availability`, then `ava.sp_refresh_location_plans` (per-location plans = filtered tariffs), then a best-effort duplicate desk-price audit (emails an alert if any location has >1 `hot_desk`/`dedicated_desk` price) |
| `xero_invoice_sync` | `functions/xero_sync.py` | timer | `0 0 4 * * *` | syncs all linked Xero tenants + caches PDFs for invoices missing `pdf_blob_path`; reuses the backfill retry/throttle flow; then syncs bank transactions (spend/receive money), then monthly P&L reports — each with a graceful skip for tenants missing the scope (`accounting.banktransactions` / `accounting.reports.profitandloss.read`) |
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
| `location_scraper_weekly` | `functions/location_scraper.py` | timer | `0 0 8 * * 1` (Mon 08:00 UTC) | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — starts the weekly parent `location_scraper_weekly_orch` (sequential waves of `LOCATION_SCRAPER_WAVE_SIZE`; skips cities already `completed` in the same ISO week via run_id `weekly-{city}-{YYYY-Www}`; a re-trigger terminates+purges an in-flight parent and retries only failed/degraded/missing cities). **Moved off 01:00 UTC on 2026-08-18**: the LoopNet actor's unblocker recovers 95-100% of its listings mid-morning UTC and 19-29% in the small hours |
| `location_scraper_weekly_retry` | `functions/location_scraper.py` | timer | `0 0 8 * * 3` (Wed 08:00 UTC) | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — second pass over the SAME ISO week under instance id `location-scraper-weekly-{week}-retry`; `ls_cities_needing_run` keeps only the cities whose row is not `completed`, so a clean week is a no-op |
| `location_scraper_orch` | `functions/location_scraper.py` | orchestration | — | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — Durable orchestrator |
| `ls_*` (18 activities) | `functions/location_scraper.py` | activity | — | only when `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1` — resolve / enumerate / scrape / grade / enrich / persist / log / alert; raw dataset is streamed Apify→SQL via `ls_fetch_and_persist_raw` and read back by `ls_normalize` (full payload never transits the orchestrator). LoopNet (all cities) runs the memo23 **paginated search** on the space-available-filtered URL — `LOOPNET_SEARCH_PAGES` (15) `?page=N` start URLs with `includeListingDetails: false`, so only the actor's healthy free-mobile-API search stage is used and the 403-ing detail fetch is never invoked. When the mobile SRP 403s the actor serves a `srp-ldjson` fallback (broker name+company, surface in `description`, address in `listingName`, **no email/coords**) — the adapter parses it (surface via `description`, address via `listingName`) and `ls_enrich_loopnet_brokers` back-fills the missing broker emails: directory memory first (free), then Lusha person-search on the unresolved (name, company), written into `silver.location_scraper_broker_directory` (`LOOPNET_LUSHA_MAX_LOOKUPS` cap; no-op without `LUSHA_API_KEY`). `ls_enumerate_loopnet_urls` is **disabled by default since 2026-08-19** (`LOOPNET_ENUMERATION_COUNTRIES` empty; `gb,ca` restores it). `ls_assess_run_health` grades each attempt (detail-fetch losses + volume baseline); the orchestrator additionally downgrades `completed → degraded` when raw items were scraped but 0 buildings survived normalization; `ls_alert_run_health` emails the week's degraded cities |
| `competence_sync` | `functions/competence_sync.py` | timer | `0 30 4 * * 1-6` | only when `ENABLE_COMPETENCE_FUNCTIONS=1` — **incremental** Firebase `competence_new` -> bronze + silver (`updated_at` watermark); needs a Firebase credential + a Firestore index on `competitors.updated_at` |
| `competence_full_reconcile` | `functions/competence_sync.py` | timer | `0 0 4 * * 0` | only when `ENABLE_COMPETENCE_FUNCTIONS=1` — weekly full read of `competence_new` + soft-delete reconcile |
| `hubspot_sync` | `functions/hubspot_sync.py` | timer | `0 45 5 * * *` | only when `ENABLE_HUBSPOT_FUNCTIONS=1` — marketing emails (content + KPI stats) -> bronze + silver + embedded daily soft-delete reconcile; needs `HUBSPOT_ACCESS_TOKEN` |
| `eventbrite_sync` | `functions/eventbrite_sync.py` | timer | `0 50 5 * * *` | only when `ENABLE_EVENTBRITE_FUNCTIONS=1` — all events (all orgs/statuses, venue+ticket expansions) -> bronze + silver + embedded daily soft-delete reconcile; needs `EVENTBRITE_PRIVATE_TOKEN` |

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
- `bronze.xero_bank_transactions` — raw spend/receive-money payloads (latest-payload MERGE on tenant+source_id)
- `bronze.xero_profit_loss_reports` — raw monthly P&L report payloads (MERGE on tenant+period_month)
- `bronze.bamboohr_employees`
- `bronze.nexudus_coworker_invoice_lines`
- `bronze.replyio_sequence_steps`
- `bronze.replyio_sequence_step_performance`
- `bronze.competence_lists` — Firebase `competence_new` parent list docs (string `source_id` = Firestore doc id)
- `bronze.competence_competitors` — Firebase `competence_new` competitor docs (string `source_id` = `{list_id}::{competitor_doc_id}`)
- `bronze.nexudus_calendar_events` — `location_id` denorm (BusinessId)
- `bronze.nexudus_event_attendees` — `location_id`/`calendar_event_id`/`coworker_id` denorms
- `bronze.nexudus_event_products` — `calendar_event_id` denorm (payload has no BusinessId)
- `bronze.hubspot_marketing_emails` — string `source_id` (HubSpot email id), latest-payload MERGE
- `bronze.eventbrite_events` — string `source_id` (Eventbrite event id), latest-payload MERGE

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
- `silver.xero_bank_transactions` — spend/receive-money headers: type (SPEND/RECEIVE + prepayment/overpayment/transfer variants), status (AUTHORISED/DELETED), bank account, contact, dates, totals, `is_reconciled`
- `silver.xero_bank_transaction_line_items` — same shape as `xero_invoice_line_items` (`bank_transaction_source_id` instead of `invoice_source_id`); DELETE+INSERT per transaction
- `silver.vw_xero_bank_transaction_pnl_lines` — P&L serving view for the budget tool: `xero_tenant_id, [date], account_code, line_amount_net, [status], [type]` (+ account/contact/bank-account context). `line_amount_net` subtracts `tax_amount` when `LineAmountTypes='Inclusive'` (the bank-feed common case). Filters `AUTHORISED` + `type IN ('SPEND','RECEIVE')` — prepayment/overpayment variants excluded (double-count risk vs ACCPAY invoices), `*-TRANSFER` excluded (no P&L impact)
- `silver.xero_profit_loss_accounts` — Xero-computed monthly P&L: one row per tenant × month × account (+ section summary rows flagged `is_summary`), with `section` (Turnover / Cost of Sales / Administrative Costs…), `amount`, base `currency_code`; account rows carry `account_id`/`account_code`/`account_name`. Replaced per tenant+month on each sync (last ~3 months re-pulled for late postings)
- `silver.vw_xero_profit_loss_monthly_accounts` — budget-tool serving view over the account-level rows (the canonical P&L actuals feed: includes credit notes, payroll journals, deferred income… by construction)
- `silver.xero_tenants`
- `silver.location_nearby_pois`
- `silver.location_transit_stations`
- `silver.location_neighborhoods`
- `silver.xero_overdue_invoice_contacts` — view joining overdue Xero invoices to Nexudus customer email data
- `silver.bamboohr_employees` — join key: `work_email` → `silver.nexudus_coworkers.email`; carries `is_deleted`/`deleted_at` reconciled daily by `bamboohr_sync`
- `silver.competence_lists` — Firebase `competence_new` lists; carries `is_deleted`/`deleted_at` reconciled weekly by `competence_full_reconcile`
- `silver.competence_competitors` — competitor records (title, address, city, lat/lng, website, placeId, category); `list_source_id` → `silver.competence_lists.source_id`; carries `is_deleted`/`deleted_at`. `country` (NAME) + `country_code` are filled by a cleanup step from the per-country parent list (competitor docs carry no reliable country of their own) — see [Competence country enrichment](#competence-country-enrichment)
- `silver.nexudus_calendar_events` — `location_source_id` → locations; optional `resource_source_id` → resources; reconciled weekly
- `silver.nexudus_event_attendees` — `calendar_event_source_id` → calendar_events; `coworker_source_id` → coworkers (NULL for external guests); `event_product_source_id` → event_products; `coworker_invoice_source_id` → coworker_invoices; `location_source_id` → locations; reconciled weekly
- `silver.nexudus_event_products` — ticket types: `calendar_event_source_id` → calendar_events; `location_source_id` inherited from the parent event by the silver writer (payload has no BusinessId); allocation/sales counters; reconciled weekly
- `silver.hubspot_marketing_emails` — subject, plain-text body + full `content_json`, campaign link, KPI counters (`stat_*`) and ratios (`open_rate`, `click_rate`, ...) + full `stats_json`; `is_deleted` reconciled daily inside `hubspot_sync`
- `silver.eventbrite_events` — schedule (UTC + local + tz), status, capacity, venue (name/address/city/country/lat/lng + `venue_json`), ticket price range + availability (+ `ticket_availability_json`), organizer; `is_deleted` reconciled daily inside `eventbrite_sync`

### AVA

- `ava.product_availability`
  - rebuilt daily
  - populated by stored procedure
  - no incremental logic
  - **Single-price invariant for desks:** the SP's hot_desk (item_type=3) and dedicated_desk (item_type=2) inserts copy `silver.nexudus_products.price` per product with no dedup, so multiple Nexudus desk products at one location land as multiple rows with different prices. These two categories are meant to have exactly ONE price per location (private offices vary by capacity, meeting rooms by member/non-member tier, day passes are MIN-aggregated — all legitimately multi-priced). After each refresh, `ava_refresh._run_duplicate_price_audit()` flags any location with >1 distinct `hot_desk`/`dedicated_desk` price and emails an alert (recipients: `AVA_PRICE_ALERT_RECIPIENTS` → `SYNC_REPORT_RECIPIENTS` → `bryan.swannie@infinitspace.com`) naming the conflicting Nexudus product ids. The fix is at source (Nexudus); the audit is a tripwire, not a de-duplicator — it deliberately does not rewrite the table. Tests: `tests/test_ava_price_audit.py`.
  - **Private-office junk filter:** Nexudus mis-types some Parking bays and Meeting Rooms as **ItemType=1 (Private Office)**, so without a guard they land in `ava.product_availability` under `item_category='private_office'` with capacity 0 or a Parking / Meeting Room name (e.g. `Parking 13`, `Meeting Room QH 4-A`). The SP's private-office section filters `AND p.capacity >= 1 AND p.name NOT LIKE 'Parking%' AND p.name NOT LIKE '%Meeting Room%'` in **both** the `product_contracts` CTE and the `office_avail` query, so these never reach the serving table. Price is deliberately NOT filtered — a real office with a missing price is kept and shown downstream as "Price on request". Added July 2026 after the Ava chatbot quoted a €0 "office" / a parking space to a Heidestrasse customer; the chatbot repo also has a behavioural backstop (`_is_junk_office_row` in `shared/langchain_agent/tools.py`) so Ava is protected before the next nightly refresh.
  - **Meeting rooms are resources-driven (redesigned 2026-08-13):** the room list comes from `silver.nexudus_resources` (`system_resource_type=1`, visible, not archived/deleted, type name mentions meeting/board) grouped into one row per (location, resource type) with a room count in `availability_notes`; prices come from `silver.nexudus_extra_services` rate cards matched on the resource **type name** (globally unique via the site tag, e.g. `8 person meeting room (FC)`) with the card's own `location_source_id` **deliberately ignored** — several cards are filed under the wrong business in Nexudus (all four Fox Court room rates under The Bower, the Zuidtoren 10P rates under Aldgate; verified live 2026-08-13). The old extra-services-driven build trusted the card's location, which erased Fox Court's real rooms and left only a hidden 50-seat classroom at £185/h — the Ava "Claire" misquote incident. Tier pick: `standard`-named cards preferred, ties broken by newest `updated_on`; `non-member` in the card name = external price; no card → price 0 (served as "Price on request"). The card's `ResourceTypes` IDs would be the ideal join key but the `/billing/extraservices` LIST endpoint doesn't return them (only the per-ID detail call does), so the name — derived from the same ID relationship — is the join key.
  - **Day passes are location-corrected via site tag (2026-08-13):** the day-pass section overrides each service's `location_source_id` with a `(AT)/(TB)/(FC)/(ZT)/(REP)/(GB)/(QH)/(C29)` tag→business-id map parsed from `resource_type_names` (several passes are filed under the wrong business in Nexudus: both FC passes under The Bower, both C29 passes under Heidestraße), filters to `charge_period = 2 AND price > 0` (drops the £99/month "Hot Desk (TB)" bundle and the £0 BobW freebie), and writes MIN/MAX price as member/non-member (`external_price`). Add a tag row to the SP's VALUES map when a new location opens.
- `ava.location_plans`
  - rebuilt daily by `ava.sp_refresh_location_plans` (called from `refresh_ava_availability` right after `product_availability`), no incremental logic
  - one row per (location, **plan**) — "Plans" in the Nexudus UI = **Tariffs** in the API (`GET /billing/tariffs`); a denormalized, **filtered serving view of `silver.nexudus_tariffs`**
  - **The silver layer is deliberately left unchanged** — `silver.nexudus_tariffs` keeps EVERY tariff; all filtering happens here. A plan is **excluded** when ANY of: `is_deleted=1`; `price<=0` (the "0 euros services" rule); `SystemTariffType IN (1,3,5)` (1=private office, 3=dedicated desk, 5=hot desk — those are priced/served elsewhere); or its `location_source_id` doesn't resolve to a silver location. Everything else is kept (connectivity/bandwidth, parking, business-address registration, mailbox, rack space, service packages, part-time access, …). Current data → **~88 plans across 8 locations**.
  - `SystemTariffType` is **not** exposed on silver, so the SP reads it back from `bronze.nexudus_tariffs.raw_json` via `JSON_VALUE` (bronze is a latest-payload upsert, UNIQUE on `source_id` → one row per tariff). `system_tariff_type_label`: 8→`part_time_access`, 9→`mailbox_storage`, else→`service`.
  - schema + SP: `scripts/sql_scripts/ava_location_plans_schema.sql`. The refresh is **resilient to the schema not being applied** — `ava_refresh._refresh_location_plans()` skips with a warning if the table/proc are absent, so deploying the code before applying the SQL never breaks the nightly run.

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
  - coworkers (full paginated list `GET /spaces/coworkers`, incremental via `UpdatedSince` watermark — switched from per-ID invoice-driven fetch 2026-06-10)
  - resources (full ID sweep from `GET /spaces/resources` + per-ID detail fetch, every run — no `UpdatedSince`; switched from products-driven per-ID fetch 2026-08-13, which left resources whose own flags changed — e.g. a meeting room hidden by reception — stale in silver indefinitely. Location comes from the record's own `BusinessId`. One-time catch-up: `scripts/python_scripts/backfill_full_resources.py` (also forces a FULL silver reload so rows with unchanged bronze payloads pick up the newly-mapped `system_resource_type` / `is_archived` / `allocation` columns))
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
- then runs `EXEC ava.sp_refresh_location_plans` to rebuild `ava.location_plans` (own `meta.sync_runs` row: `('ava','location_plans','ava')`); skips with a warning if those objects don't exist yet, and a plans-refresh failure is logged but does not fail the product-availability run or the desk-price audit

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

### Xero Bank Transactions

- `shared/xero/bank_transaction_sync.py` — `XeroBankTransactionSyncService`, mirrors the invoice sync; runs inside the same `xero_invoice_sync` timer, after PDF caching
- **why**: bank fees (Bank Fees, Revolut Merchant Fees, direct debits...) are coded straight from the bank feeds as spend-money and never appear on ACCPAY invoices (verified: 1 single "Bank Fees" invoice line of 29.50 across all 158k lines), so P&L actuals had a structural hole
- incremental per tenant via `If-Modified-Since`; watermark `meta.xero_tenants.last_bank_transaction_modified_utc` (+ started/completed/error columns), with a fallback to `MAX(updated_date_utc)` from silver when the meta columns are missing; 5-min lookback like invoices
- **scope-skip**: requires `accounting.banktransactions[.read]` — the connection granted before 2026-07 only has invoices+settings scopes, so tenants hitting 401/403 are skipped with a warning (error recorded on the tenant row) instead of failing the run; data starts flowing the night after the OAuth re-consent, no redeploy needed
- **deploy-before-apply safe**: if the bank transaction tables are missing the run skips with a warning (same convention as `ava_refresh`)
- deletions are status-based (`DELETED` + `UpdatedDateUTC` bump) — no reconcile job, same as `xero_invoices`
- first run per tenant has no watermark → automatic full backfill; `XERO_BANK_TX_SYNC_FORCE_FULL=1` forces it again
- schema: `scripts/sql_scripts/xero_bank_transactions_schema.sql` (bronze + silver + meta columns + `silver.vw_xero_bank_transaction_pnl_lines`)

### Nexudus Invoice PDF Storage

- PDFs are stored in Azure Blob Storage, not in SQL
- container: `nexudus-invoice-pdfs` on `staccinfinitspaceprod001`
- blob path format: `{location_source_id}/{yyyy}/{mm}/{invoice_source_id}.pdf`
- `silver.nexudus_coworker_invoices.pdf_blob_path` holds the reference
- timer function: `nexudus_invoice_pdf_cache` at 03:30 UTC
- **Candidate watermark** — `pdf_blob_path IS NULL AND is_deleted = 0` AND (`updated_on >= 2 days ago` **OR** it is an open finance-worklist invoice — `due_amount>0 AND void=0 AND draft=0 AND paid=0 AND due_date >= 365 days ago`). The worklist branch guarantees finance-dashboard invoices get a PDF nightly even when they were not recently updated (so the nightly run is self-sufficient, not dependent on the one-off backfill).
- **Download mechanism (`NexudusClient.get_invoice_pdf`)** — Nexudus has **no direct PDF endpoint**; the PDF is rendered via a two-step "run command" flow (per Nexudus support):
  1. `POST /api/billing/coworkerInvoices/runCommand` with body `{"Ids":[invoice_id],"Key":"COWORKER_INVOICE_PRINT","Parameters":[]}` → 200 with a `RedirectURL` pointing at a temporary download file (`run_invoice_print_command`, tenacity-retried for 429/5xx). A single `Id` returns a PDF; 2+ Ids return a zip — we always pass exactly one Id.
  2. `GET https://spaces.nexudus.com{RedirectURL}` (`download_temp_file`) — the temp file lives on the **bare host, NOT under `/api`**, and **expires within a few minutes**, so it is downloaded immediately, reusing the session Bearer header. The query is **percent-encoded with `%20`** (via `urllib.parse.quote(query, safe="=&%")`) — passing the raw string to aiohttp/yarl would emit form-style `+` for the spaces in `downloadFileName`. `download_temp_file` is **not** long-backoff retried (the temp URL is short-lived); instead `get_invoice_pdf` re-mints a fresh URL.
  - **Re-mint loop**: `get_invoice_pdf` runs the two steps up to `INVOICE_PDF_MAX_ATTEMPTS` (2) times — on a transient download error or an empty/non-`%PDF` body (an expired temp file returns an HTML page), it re-runs the command for a fresh URL rather than re-hitting the dead link.
  - Returns `None` (genuine "nothing to cache" skip) when the command is unsuccessful / returns no `RedirectURL` / 404, or returns a `.zip` (zip detection parses the `downloadFileName` value, so param order / archive name can't fool it). **Raises** when a download keeps failing (transient error or never a PDF) so the caller counts it as a failure, not a silent skip.
  - The **old endpoint `GET /api/billing/coworkerinvoice/{id}/pdf` did not exist** and reliably failed — replaced 2026-06-24. Invoices it wrongly stamped with the `__unavailable__` sentinel are NOT genuinely unavailable; the backfill's `--reset-unavailable` clears them for retry.
  - The nightly `cache_missing_nexudus_pdfs` no longer writes the `__unavailable__` sentinel — a failed fetch just stays `NULL` and is retried next run. **Observability**: the timer fails the run (so the 06:00 health report flags it) only when there were candidates and **every** fetch failed with **zero** cached (the regression signal); partial failures self-heal.
  - **Defense-in-depth**: both finance worklist SPs (`gold.sp_refresh_invoice_worklist` in `finance_invoice_worklist_sp.sql`, `gold.sp_refresh_finance_dashboard` in `core_finance_dashboard_schema.sql`) wrap the column as `NULLIF(nci.pdf_blob_path, '__unavailable__')` so a leftover sentinel can never surface as a bogus PDF path in the dashboard (takes effect when those SPs are next applied).

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
| `nexudus_calendar_events` | `nexudus_silver_reconcile` | weekly Sun 01:00 | ≤7d |
| `nexudus_event_attendees` | `nexudus_silver_reconcile` | weekly Sun 01:00 | ≤7d |
| `nexudus_event_products` | `nexudus_silver_reconcile` | weekly Sun 01:00 | ≤7d |
| `hubspot_marketing_emails` | embedded in `hubspot_sync` | daily 05:45 | ≤24h |
| `eventbrite_events` | embedded in `eventbrite_sync` | daily 05:50 | ≤24h |
| `xero_invoices` | none — Xero sets `invoice_status = 'DELETED'`, existing sync picks it up | daily | ≤24h (status-based) |
| `xero_bank_transactions` | none — Xero sets `transaction_status = 'DELETED'`, sync picks it up | daily | ≤24h (status-based) |

Invoice reconcile window: default 365 days of `due_date` (configurable via
`NEXUDUS_INVOICE_RECONCILE_LOOKBACK_DAYS`). Invoices older than the window
are ignored — the finance dashboard only cares about recent due dates.

Downstream consumers (gold tables, views, reports) MUST filter
`WHERE is_deleted = 0` on any silver read. `gold.sp_refresh_finance_dashboard`
already enforces this on every silver join.

### Competence country enrichment

Competitor docs in Firestore `competence_new` rarely carry a country of their
own (`last_seen_country_code` is mostly empty), but each competitor belongs to a
**per-country parent list** (`NL_AUTO` → Netherlands / NL). So both
`silver.competence_competitors.country` (NAME) and `country_code` are filled by
a cleanup step between bronze and silver rather than read straight from the
competitor payload.

- Pure resolver: `resolve_competitor_country()` in
  `shared/firebase/transformers/competence.py`. In practice the `competence_new`
  parent lists carry a free-text country NAME and **no** `country_code` (and
  random Firestore doc ids, not `NL_AUTO`), so the country NAME is what fills the
  code. Code precedence: the competitor's own `last_seen_country_code` (when
  present) → the parent list's `country_code` → the **list's country name mapped
  to ISO2** (a name→ISO2 alias map, the usual path) → the ISO2 parsed from a
  `XX_AUTO` list-id prefix. Name precedence: the parent list's `country` name
  (authoritative) → the canonical ISO name. `UK`→`GB`, and names are canonicalised
  (both "USA" and "United States" → "United States" / `US`).
- Ongoing path: `SilverCompetenceWriter._sync_competitors` loads a
  `{list_source_id → (country, country_code)}` map from `silver.competence_lists`
  (synced just before competitors in the same run, so it is complete) and passes
  each competitor's parent country into `transform_competitor`. Only watermark-
  changed rows are re-written, so the nightly run enriches new/changed rows only.
- Backfill (one-off, for rows that predate this step):
  `scripts/python_scripts/backfill_competence_competitor_country.py` reuses the
  same resolver so backfilled values match the sync exactly
  (`--dry-run` / `--all` / `--limit`). Run
  `scripts/sql_scripts/competence_competitor_country_migration.sql` first to add
  the `country` column.

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
- `Coworker invoices: X fetched, Y changed, Z skipped, W written to bronze`
- `Coworkers: X fetched, Y changed, Z skipped, W written to bronze`
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
LOCATION_SCRAPER_WEEKLY_SCHEDULE="0 0 8 * * 1"  # weekly scrape, Monday 08:00 UTC (NOT the small hours — the LoopNet unblocker is throttled at 01:00 UTC)
LOCATION_SCRAPER_WEEKLY_RETRY_SCHEDULE="0 0 8 * * 3"  # mid-week pass over cities that did not complete (no-op on a clean week)
LOCATION_SCRAPER_WAVE_SIZE=3  # weekly cities scraped per sequential wave (OOM guard)
LOCATION_SCRAPER_MAX_ATTEMPTS=3            # scrape attempts per city before a degraded run is accepted
LOCATION_SCRAPER_RETRY_DELAY_MINUTES=30    # wait between attempts
LOCATION_SCRAPER_DETAIL_LOSS_THRESHOLD=0.20  # share of listings the actor may drop before the run is degraded
LOCATION_SCRAPER_BASELINE_RATIO=0.6        # share of the city's best recent volume a run must reach
LOCATION_SCRAPER_BASELINE_RUNS=8           # how many previous runs define that baseline
LOCATION_SCRAPER_ENUM_ATTEMPTS=3           # LoopNet enumeration retries (legacy path, only when re-enabled)
LOCATION_SCRAPER_ENUM_RETRY_MINUTES=5      # wait between enumeration attempts
LOCATION_SCRAPER_ALERT_RECIPIENTS=...      # weekly degraded-cities email (falls back to SYNC_REPORT_RECIPIENTS)
LOOPNET_ENUM_MAX_PAGES=25     # safety ceiling on filtered LoopNet search pages fetched per city
LOOPNET_MAX_ITEMS=1000              # maxItems sent on uncapped LoopNet runs — the key MUST be sent (omitting it makes the actor stop early)
LOOPNET_MAX_UNBLOCKER_REQUESTS=2000 # per-run paid-unblocker budget; unused by the paginated-search path (it never leaves the free mobile API)
LOOPNET_SEARCH_PAGES=15             # result pages of the filtered LoopNet search per city — the actor serves ONE page per start URL, so this sets the volume
LOOPNET_ENUMERATION_COUNTRIES=      # empty = paginated search everywhere (default); `gb,ca` restores the legacy listing-URL enumeration
LOOPNET_LUSHA_MAX_LOOKUPS=150       # per-run cap on Lusha person-search lookups for LoopNet brokers whose email is missing from the payload AND the broker directory (needs LUSHA_API_KEY; no-op without it)

# Function registration
ENABLE_ETL_FUNCTIONS=1
ENABLE_ADMIN_FUNCTIONS=0
ENABLE_COMPETENCE_FUNCTIONS=0
ENABLE_HUBSPOT_FUNCTIONS=0
ENABLE_EVENTBRITE_FUNCTIONS=0

# HubSpot (marketing emails sync; private app token with the `content` scope)
HUBSPOT_ACCESS_TOKEN=...
HUBSPOT_SYNC_SCHEDULE="0 45 5 * * *"
HUBSPOT_RECONCILE_MIN_IDS=5

# Eventbrite (private token from https://www.eventbrite.com/platform/api-keys)
EVENTBRITE_PRIVATE_TOKEN=...
# EVENTBRITE_ORGANIZATION_ID=...     # optional: pin one org (default: all accessible orgs)
EVENTBRITE_SYNC_SCHEDULE="0 50 5 * * *"
EVENTBRITE_RECONCILE_MIN_IDS=1

# Firebase competence_new sync (TeamAndy competitors). Provide ONE of:
#   FIREBASE_CREDENTIALS: service-account JSON as a string (Azure app setting; same
#     value the TeamAndy scraping services use). If not valid JSON it is read as a path.
#   FIREBASE_SERVICE_ACCOUNT_KEY_FILE: a local path to, or a download URL for, the
#     service-account key file (the TeamAndy backend stores a Google Drive URL here).
FIREBASE_CREDENTIALS=...
# FIREBASE_SERVICE_ACCOUNT_KEY_FILE=C:\path\to\serviceAccountKey.json

# Schedule overrides
NEXUDUS_SYNC_SCHEDULE="0 0 2 * * *"
SILVER_SYNC_SCHEDULE="0 30 2 * * *"
AVA_REFRESH_SCHEDULE="0 0 3 * * *"
XERO_INVOICE_SYNC_SCHEDULE="0 0 4 * * *"
XERO_INVOICE_SYNC_FORCE_FULL=0
XERO_BANK_TX_SYNC_FORCE_FULL=0
XERO_PROFIT_LOSS_BACKFILL_START=2024-01
XERO_PROFIT_LOSS_REFRESH_MONTHS=3
XERO_PROFIT_LOSS_SYNC_FORCE_FULL=0
NEXUDUS_PDF_CACHE_SCHEDULE="0 30 3 * * *"
NEXUDUS_INVOICE_RECONCILE_SCHEDULE="0 15 5 * * *"
NEXUDUS_INVOICE_RECONCILE_LOOKBACK_DAYS=365
NEXUDUS_INVOICE_RECONCILE_MIN_IDS=100
NEXUDUS_INVOICE_RECONCILE_WINDOW_DAYS=30                  # DueDate sub-window size; keeps the active-id fetch shallow (avoids the deep-pagination 403)
NEXUDUS_INVOICE_LOOKBACK_DAYS=2                           # from_CoworkerInvoice_UpdatedOn window for the incremental invoice fetch
NEXUDUS_INVOICE_RESYNC_LOOKBACK_MONTHS=12                 # _resync_open_invoices re-fetches open unpaid invoices due within this many months (by-ID, ignores UpdatedOn — Nexudus doesn't bump it on payment/credit)
NEXUDUS_SILVER_RECONCILE_SCHEDULE="0 0 1 * * 0"
BAMBOOHR_RECONCILE_MIN_IDS=10
FINANCE_DASHBOARD_REFRESH_SCHEDULE="0 30 5 * * *"
FINANCE_DASHBOARD_RECHECK_SCHEDULE="0 0 10 * * *"        # second guarded rebuild after morning data settles
FINANCE_DASHBOARD_MIN_WORKLIST_RATIO=0.5                  # guardrail: reject a rebuild below this fraction of the live worklist count
FINANCE_DASHBOARD_GUARDRAIL_MIN_BASELINE=20               # skip guardrail when the live count is smaller than this
REPLYIO_SYNC_SCHEDULE="0 30 5 * * *"
COMPETENCE_SYNC_SCHEDULE="0 30 4 * * 1-6"                # incremental read, Mon-Sat
COMPETENCE_FULL_SYNC_SCHEDULE="0 0 4 * * 0"              # weekly full read + soft-delete reconcile, Sun
COMPETENCE_INCREMENTAL_OVERLAP_MINUTES=60                # re-read buffer before last run start (clock-skew guard)
COMPETENCE_RECONCILE_MIN_IDS=10                          # reconcile safety floor (skip if fewer competitors fetched)
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

Firebase competence_new sync validation:

```powershell
# 1. Apply schema once: run scripts/sql_scripts/competence_schema.sql against the warehouse DB
#    (existing DBs: also run scripts/sql_scripts/competence_competitor_country_migration.sql
#     to add silver.competence_competitors.country)
# 2. Unit tests (no creds / no DB)
.\venv\Scripts\python.exe -m unittest tests.test_competence_transformer
# 3. Dry run — connect to Firestore, read + transform, no writes (needs FIREBASE_CREDENTIALS)
.\venv\Scripts\python.exe scripts\python_scripts\test_competence_sync.py
# 4. Full local run — Firestore -> bronze -> silver -> reconcile (needs FIREBASE_CREDENTIALS + SQL)
.\venv\Scripts\python.exe scripts\python_scripts\test_competence_sync.py --write
# 5. One-off backfill of country/country_code on existing competitor rows (needs SQL only)
.\venv\Scripts\python.exe scripts\python_scripts\backfill_competence_competitor_country.py --dry-run
.\venv\Scripts\python.exe scripts\python_scripts\backfill_competence_competitor_country.py
```

Nexudus events validation (calendar_events / event_attendees / event_products):

```powershell
# Unit tests (no creds / no DB)
.\venv\Scripts\python.exe -m unittest tests.test_nexudus_event_transformers
# Field-shape discovery against the live API (needs Nexudus creds)
.\venv\Scripts\python.exe scripts\python_scripts\inspect_nexudus_events.py
# Dry run — fetch + transform samples, no SQL writes
.\venv\Scripts\python.exe scripts\python_scripts\test_events_sync.py
# Full local run — bronze -> silver (needs Nexudus creds + SQL; schema from
# scripts/sql_scripts/nexudus_events_schema.sql — already applied to prod 2026-06-10)
.\venv\Scripts\python.exe scripts\python_scripts\test_events_sync.py --write
```

HubSpot marketing emails validation:

```powershell
# 1. Apply schema once: scripts/sql_scripts/hubspot_marketing_emails_schema.sql
#    (already applied to prod 2026-06-10)
.\venv\Scripts\python.exe scripts\python_scripts\apply_schema_script.py scripts/sql_scripts/hubspot_marketing_emails_schema.sql
# 2. Unit tests (no creds / no DB)
.\venv\Scripts\python.exe -m unittest tests.test_hubspot_transformer
# 3. Dry run — fetch + print stats key inventory + transformed sample (needs HUBSPOT_ACCESS_TOKEN)
.\venv\Scripts\python.exe scripts\python_scripts\test_hubspot_sync.py
# 4. Full local run — HubSpot -> bronze -> silver -> reconcile (needs SQL too)
.\venv\Scripts\python.exe scripts\python_scripts\test_hubspot_sync.py --write
```

Eventbrite validation:

```powershell
# 1. Apply schema once: scripts/sql_scripts/eventbrite_events_schema.sql
#    (already applied to prod 2026-06-10)
.\venv\Scripts\python.exe scripts\python_scripts\apply_schema_script.py scripts/sql_scripts/eventbrite_events_schema.sql
# 2. Unit tests (no creds / no DB)
.\venv\Scripts\python.exe -m unittest tests.test_eventbrite_transformer
# 3. Dry run — list orgs + fetch events + transformed sample (needs EVENTBRITE_PRIVATE_TOKEN)
.\venv\Scripts\python.exe scripts\python_scripts\test_eventbrite_sync.py
# 4. Full local run — Eventbrite -> bronze -> silver -> reconcile (needs SQL too)
.\venv\Scripts\python.exe scripts\python_scripts\test_eventbrite_sync.py --write
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

Nexudus PDF validation (COWORKER_INVOICE_PRINT run-command flow):

```powershell
# Unit tests (no creds / no DB) — request shape, RedirectURL parse, guards
.\venv\Scripts\python.exe -m unittest tests.test_nexudus_pdf
# Single PDF fetch against the live API (saves to disk; needs Nexudus creds)
.\venv\Scripts\python.exe scripts\python_scripts\test_nexudus_pdf.py
# Full round-trip: run-command -> blob upload -> SQL -> read back (needs SQL too)
.\venv\Scripts\python.exe scripts\python_scripts\test_nexudus_pdf_cache.py
# Backfill: clear the old endpoint's bogus '__unavailable__' sentinels and fetch
.\venv\Scripts\python.exe scripts\python_scripts\backfill_nexudus_pdfs.py --reset-unavailable --dry-run
.\venv\Scripts\python.exe scripts\python_scripts\backfill_nexudus_pdfs.py --reset-unavailable
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

Xero bank transactions validation:

```powershell
# 1. Apply schema once (idempotent): bronze/silver tables + meta watermark columns + P&L view
.\venv\Scripts\python.exe scripts\python_scripts\apply_schema_script.py scripts/sql_scripts/xero_bank_transactions_schema.sql
# 2. Unit tests (no creds / no DB)
.\venv\Scripts\python.exe -m unittest tests.test_xero_bank_transactions
# 3. Dry run — scope probe per tenant (403 = re-consent not done yet) + sample payload
#    NB: needs the PROD INTEGRATIONS_ENCRYPTION_KEY in .env to decrypt the stored tokens
.\venv\Scripts\python.exe scripts\python_scripts\test_bank_transactions_sync.py
# 4. Volumetry — full pagination count per tenant (sizes the backfill, API reads only)
.\venv\Scripts\python.exe scripts\python_scripts\test_bank_transactions_sync.py --count
# 5. Full local run — bronze + silver writes; first run = full backfill per tenant
.\venv\Scripts\python.exe scripts\python_scripts\test_bank_transactions_sync.py --write
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
| AVA location plans | done | `ava.location_plans` (per-location plans = filtered `silver.nexudus_tariffs`; excludes desks/offices via `SystemTariffType` 1/3/5 + €0 plans); SP `ava.sp_refresh_location_plans`, wired into `refresh_ava_availability`; schema `scripts/sql_scripts/ava_location_plans_schema.sql` **applied + populated to prod 2026-06-19 (87 plans / 8 locations)**; silver layer unchanged. Code (`ava_refresh.py`) **not yet deployed** — push to `main` so the nightly run keeps it fresh |
| Xero OAuth + tenant storage | done | DB-backed |
| Xero auto-refresh | done | disconnects on `invalid_grant` |
| Xero invoice sync | done | incremental by tenant |
| Xero tenant directory | done | refreshed after Xero sync and exposed as `xero.silver_tenants` |
| Xero invoice PDF caching | done | blob storage (`xero-invoice-pdfs`); path in `silver.xero_invoices.pdf_blob_path`; auto-cached nightly for invoices missing `pdf_blob_path` |
| Xero P&L report sync | done | **Live since 2026-07-08**: OAuth re-consent granted (scope `accounting.reports.profitandloss.read` now on the token) + full backfill complete — **8/8 tenants, 240 tenant-months (2024-01→2026-06), 5,302 silver rows, 0 skip / 0 fail**. Monthly `GET /Reports/ProfitAndLoss?standardLayout=true` per tenant → `bronze.xero_profit_loss_reports` + `silver.xero_profit_loss_accounts` + view `silver.vw_xero_profit_loss_monthly_accounts`; runs last in the `xero_invoice_sync` 04:00 timer, re-pulls last 3 months each run. Currencies GBP (UK) / EUR (NL+DE) |
| Xero bank transactions sync | done | **Live since 2026-07-08**: OAuth re-consent granted (granular scopes incl. banktransactions), deployed, schema applied, initial backfill complete — 5,193 transactions / 5,315 lines across 8 tenants, exact match vs API volumetry. Bank Fees hole filled: ~114.9k GBP + ~25.9k EUR net (vs a single 29.50 invoice line before). Nightly incremental via the 04:00 timer. Serving view for the budget tool: `silver.vw_xero_bank_transaction_pnl_lines` (net-of-tax, SPEND/RECEIVE only; group by `currency_code`) |
| Nexudus coworker invoices + coworkers | done | incremental via UpdatedSince watermark |
| Xero ↔ Nexudus invoice linking | done | `silver.xero_overdue_invoice_contacts` view; 5/12 tenants connected |
| Optional admin HTTP routes | done | separate deployment mode |
| Google Maps scheduled pipeline | not wired | utilities exist, not registered in default app |
| Core layer population | planned | not implemented |
| Real Estate CoStar extractor HTTP function | done | `ENABLE_REAL_ESTATE_FUNCTIONS=1` to activate |
| BambooHR employee sync | done | bronze + silver; `work_email` is join key to Nexudus coworkers |
| Reply.io stats sync | done | bronze only; sequence steps + daily step performance; 4 AB test sequences |
| Nexudus coworker invoice lines | done | bronze + silver; `financial_account_code`/`financial_account_name` per line item |
| Nexudus invoice PDF caching | done | blob storage (`nexudus-invoice-pdfs`); path in `silver.nexudus_coworker_invoices.pdf_blob_path`; downloads via the `COWORKER_INVOICE_PRINT` run-command flow (the old direct `/pdf` endpoint never existed); backfill `scripts/python_scripts/backfill_nexudus_pdfs.py` |
| Finance dashboard gold layer | done | Nexudus-only; `gold.finance_dashboard_invoice_worklist` + `gold.finance_dashboard_user_access`; rebuilt by `gold.sp_refresh_finance_dashboard`; filters `is_deleted = 0` on all silver reads; rebuild is **guarded** (rolls back + fails the run if the worklist collapses < 50% of the live count) and runs **twice daily** (05:30 + 10:00 recheck) so an under-built morning snapshot is republished in full |
| Landlord dashboard gold views | done | 3 views in `scripts/sql_scripts/landlord_dashboard_schema.sql`: `gold.vw_landlord_current_contracts`, `gold.vw_landlord_contract_book_monthly` (±12 months), `gold.vw_landlord_pricing_summary`; cancellation_date-only semantics for forecasting; list price from product join |
| Soft-delete / source reconciliation | done | `is_deleted`/`deleted_at` on all Nexudus + BambooHR silver tables; daily `nexudus_invoice_reconcile` (invoices + cascaded lines), daily roster reconcile inside `bamboohr_sync`, weekly `nexudus_silver_reconcile` for other entities |
| Sync health report email | done | daily 06:00 UTC via Microsoft Graph; subject `[OK]`/`[FAIL]`; green/red table per entity + record-level error summary; sends to `SYNC_REPORT_RECIPIENTS` from `GRAPH_SENDER_UPN` |
| Location Scraper (Idealista + Otodom + IS24 + LoopNet) | done | HTTP-triggered Durable Functions pipeline; `ENABLE_LOCATION_SCRAPER_FUNCTIONS=1`; Idealista (ES/IT) + Otodom (PL) + Immobilienscout24 (DE) + LoopNet (UK/London + US: New York, San Francisco, Palo Alto, Los Angeles, Austin, Seattle, Redwood City, San Mateo, San Bruno, Cupertino + Canada/Toronto); LoopNet runs the memo23 broad search directly on the space-available-filtered URL with `moreResults` (bypasses the 500-item cap; the enumeration 2-step was retired 2026-06-29 — see changelog); Lusha enrichment via fan-out; free Nominatim geocode fallback when no `GOOGLE_MAPS_API_KEY`; bronze schema; see `docs/location_scraper.md` |
| Nexudus events (calendar events + attendees + ticket products) | done | bronze + silver via the standard fanout (3 new entities in `nexudus_to_bronze` / queue worker); linking: event → location (BusinessId), attendee → event/coworker/ticket/invoice, product → event (+ location inherited from parent event); weekly soft-delete via `nexudus_silver_reconcile`; schema applied to prod + backfilled 2026-06-10 (729 events / 1,553 attendees / 205 products) |
| HubSpot marketing emails sync | done (code) | Optional (`ENABLE_HUBSPOT_FUNCTIONS=1` + `HUBSPOT_ACCESS_TOKEN`); daily 05:45 UTC full fetch with `includeStats=true`; silver carries subject/body/content + KPI counters & ratios + raw `stats_json`; schema applied to prod 2026-06-10; waiting on a private-app token to enable |
| Eventbrite events sync | done (code) | Optional (`ENABLE_EVENTBRITE_FUNCTIONS=1` + `EVENTBRITE_PRIVATE_TOKEN`); daily 05:50 UTC, all orgs + all statuses with venue/ticket/organizer expansions; schema applied to prod 2026-06-10; waiting on a private token to enable |
| TeamAndy Firestore → Azure migration | data done | One-shot cutover migration (goal: fully close Firebase). New **`teamandy`** operational schema, **42 tables**, **262,877 rows / 0 rejects** validated (`scripts/sql_scripts/teamandy_00…50.sql`). Caches → Azure **Table Storage** (`staccinfinitspaceprod001`): **11 tables / 124,784 entities**, bodies >32 KB → Blob `cache-bodies` (8,067). Location images → Blob **`location-images`** (11 images, all 9 locations linked via `image_blob_url`). ETL: `scripts/python_scripts/teamandy_migration/` (one-shot, idempotent). User-excluded (not migrated): interactions/tasks/integrations/analytics/deleted/cache_main/cache_main_competence/pre-leads/test_leads. **Next (separate task): repoint AI-teamandy app reads/writes/deletes Firebase → Azure, then close Firebase.** Table-plane writes used the storage account key (deploy identity lacks `Storage Table Data Contributor`; grant later for AAD) |
| Firebase competence_new sync | done | Optional (`ENABLE_COMPETENCE_FUNCTIONS=1` + Firebase credential); **incremental** `competence_sync` (Mon-Sat 04:30 UTC — only competitors changed since last run via an `updated_at` collection-group watermark) + weekly `competence_full_reconcile` (Sun 04:00 UTC — full read + soft-delete). Reads Firestore `competence_new` (parent lists + `competitors` subcollection, v1 array fallback) → `bronze.competence_*` → `silver.competence_*`. Needs a Firestore index on `competitors.updated_at` (full-read fallback while it builds); reuses AI-teamandy firebase-admin logic. Competitor `country`/`country_code` are enriched from the per-country parent list in a cleanup step between bronze and silver (one-off backfill script for existing rows) |

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

Last updated: 2026-08-31 (**LoopNet — the THIRD payload shape (`srp-ldjson`) + Lusha broker-email tail + grade on buildings-kept.** Symptom: the W36 weekly run showed London **0 buildings** ("we spend our life fixing this"). It was NOT our code and NOT the 2026-08-19 pagination fix — the scrape worked (609 raw items, ≈ W35's 616). **Root cause, proven from `bronze.location_scraper_raw`:** LoopNet's mobile SRP 403'd on every London page this morning, so the memo23 actor fell back to a shape we'd never parsed, `_dataSource: "srp-ldjson"` (it scrapes the search-results page's LD+JSON). Its keys are `propertyId, listingUrl, listingName, description, brokerName, brokerCompany, position` — **no `sizeSf`/`spaces`/`header`, no coordinates, no `address`/`zip`, and no `brokerEmail`.** Surface lives ONLY in `description` ("27,568 sq ft Office Property Offered in …"), address ONLY in `listingName` ("450 Bath Rd, West Drayton UB7 0EB"). The old surface parse (sizeSf / "…SF…Available" name) found nothing → every item failed the ≥1500 m² guardrail → 0 buildings, and the grading (which scores RAW volume) still marked the week `completed`. NB nothing was lost from the map: gold keeps the latest run WITH globe rows, so London kept its 284 W35 markers — the dashboard's 0 came from the run-log table. **Decisive site fact (tested live in a real browser 2026-08-31, bot-check cleared): the broker email is NOT on LoopNet's website at all** — a fully-rendered listing-detail page has 0 emails / 0 phones; contact is a *form* LoopNet relays. The email only ever lived in the mobile API. So **building our own LoopNet scraper cannot get emails** — memo23 dominates it (emails when the mobile API is open, the same emailless HTML we'd scrape when closed); don't build one. The answer is directory + Lusha on the broker's person name + company (**97% of srp-ldjson brokers are a real first+last**, 293/300 London; only ~2% "Sales Enquiries" desks). **Fix (3 changes, py_compile clean; location-scraper suite 155 pass / 7 pre-existing berlin_simulation fails):** (1) **Adapter parses srp-ldjson** — [shared/location_scraper/adapters/loopnet.py](shared/location_scraper/adapters/loopnet.py) `available_surface_sqft_from_payload` adds `description` as the LAST surface source (never overrides `sizeSf`/name — those are tried first, so the healthy `free-mobile-api` path is untouched); `normalize` falls `address` back to `listingName`; [shared/location_scraper/activities/materialize_globe.py](shared/location_scraper/activities/materialize_globe.py) `_map_row` adds `listingName` to its address picks so geocoding has an address. Replay on the real W36 raw: **0 → 323 buildings** (300 distinct), **224 (74%) already emailed via the broker directory**. (2) **Lusha broker-email tail-enrichment** — new `ls_enrich_loopnet_brokers` activity ([shared/location_scraper/activities/enrich.py](shared/location_scraper/activities/enrich.py) `enrich_loopnet_brokers`): LoopNet still skips the agency Lusha fan-out, but the orchestrator now runs a person-based pass — every (broker name, company) the directory can't resolve → `lusha.search_individual` → `extract_best_email` → written into `silver.location_scraper_broker_directory`, which `materialize_globe` reloads THIS run and every future run gets free (the LoopNet broker pool is finite, so it compounds toward ~95%). Directory hits cost nothing; cap `LOOPNET_LUSHA_MAX_LOOKUPS` (default 150); no-op without `LUSHA_API_KEY`. (3) **Grade on buildings-kept** (closes the open gap from 2026-08-19) — in `location_scraper_orch`, `raw_item_count > 0` with `len(listings) == 0` downgrades the run `completed → degraded`, so `ls_cities_needing_run` retries it in the Wednesday pass + the weekly alert names it, instead of banking an empty green week. `== 0` only, so genuinely small cities (Cupertino=5) stay `completed`. **Tests**: +4 srp-ldjson adapter cases (surface from description, address fallback, currency-from-city, below-floor drop, description-never-overrides-sizeSf) in [tests/test_location_scraper_loopnet_adapter.py](tests/test_location_scraper_loopnet_adapter.py); new [tests/test_location_scraper_broker_enrich.py](tests/test_location_scraper_broker_enrich.py) (no-key no-op + lookup cap). **Deploy = push to `main`** (GitHub Actions). No SQL/schema change — the broker directory table already exists. After deploy: London gold shows 224 markers pre-Lusha and climbs as the directory fills; the Wednesday retry re-tries any city the mobile SRP 403'd. **Still open (upstream, memo23's to fix):** the App Check breakage — when the SRP 403s the actor still falls back and small cities can be diluted; the retries mask it, they don't repair it.)

Previous: 2026-08-19 (**LoopNet fixed at the root: stop asking the actor for the stage that is broken.** Follow-up to 2026-08-18 below, which added graded retries + a degraded status but left the underlying breakage in place (and was never deployed — the commit sat unpushed, so W34 ran on the old code at 01:14 UTC). **The insight**: the memo23 actor has TWO stages and only one is broken. Its SEARCH stage reads LoopNet's **free** mobile API (`_dataSource: free-mobile-api`) and is perfectly healthy; its per-listing DETAIL stage is the one that 403s (App Check) and has to recover every listing through the paid unblocker chain that is throttled to a single provider. Every fix so far — enumeration, retries, a daytime slot — kept feeding the pipeline through that detail stage. **Measured 2026-08-19, London, live**: `includeListingDetails: false` returned 30/30 items with **0 detail-fetch attempts** and brokerEmail on 28/30 — the broken stage is simply never entered. The remaining problem was volume (a single search URL yields 48 items), solved by the fact that **memo23 serves one result page per start URL** and honours `?page=N` on the filtered search: 12 page URLs → **550 items / 320 distinct buildings**, 98% with a broker email, still 0 detail attempts. New York (loopnet.com): **272 items / 197 distinct**, 98% email. Against the W34 production runs of 11 and 24 buildings. **Bonus discoveries**: the search placard carries **coordinates** (549/550) which the detail payload NEVER did (0/267) — LoopNet listings no longer need geocoding at all; and `sizeSf` is the **available** area, not the building total (proven by joining 26 common propertyIds against the detail path: ratio 1.00 on every one, and the placard's own `stats` label it "SF Avail"), and it even resolves surface for the coworking listings the detail path left at None. **Changes**: [shared/location_scraper/adapters/loopnet.py](shared/location_scraper/adapters/loopnet.py) — `build_input` now expands a search URL into `LOOPNET_SEARCH_PAGES` (default **15**) `?page=N` start URLs with `includeListingDetails: false` (a *list* input still means the legacy listing-URL mode and keeps the detail fetch on, so the old path stays reversible); new `search_page_urls` + `coordinates_from_payload` helpers; `normalize` now reads lat/lng from the payload and derives the currency from the scraped city via the existing `get_country_code_for_city` — the placard drops `country` and its own `currency` field says **USD for London**, which would have silently mispriced the UK/CA markets. [functions/location_scraper.py](functions/location_scraper.py) — `LOOPNET_ENUMERATION_COUNTRIES` is now **empty** (was `{gb, ca}`): the enumeration produced bare listing URLs, which is precisely what forces the broken detail fetch, and the enumeration actor itself was refusing to run again while this was measured (`Connection refused` x10 → 0 URLs, 07:46 UTC). Overridable to `gb,ca`. [shared/location_scraper/activities/scrape.py](shared/location_scraper/activities/scrape.py) — `normalize_listings` drops repeats by `external_id` within a run (result pages overlap: 550 items → 320 buildings). [shared/location_scraper/run_health.py](shared/location_scraper/run_health.py) — `retry_useless` is now raised only when the run actually **needed** the unblocker (detail attempts > 0, or zero items); the NY probe returned 272 good items with the quota marker in its log, and the old unconditional rule would have disarmed the retry loop on a healthy run. **Validation**: location-scraper suite **149 pass** (+7 pre-existing berlin_simulation failures, re-confirmed identical at HEAD via `git stash`); 5 new adapter tests (placard → building + broker email, payload coordinates, 0/0 coords still geocode, currency by city with payload `country` still winning, per-run dedupe), a new `test_uk_no_longer_enumerates_by_default`, and the two enumeration tests kept alive behind the env override. Plus a **read-only E2E through the real production path** (`resolve_source` → `start_apify_run` input → `normalize_listings` on the live 550-item London dataset): 15 page URLs, `includeListingDetails: False`, **297 buildings kept**, 96% with a broker email, **100% with coordinates**, currency GBP, surfaces 1514–30835 m²; and the globe layer (`_map_row`, the map/dashboard feed) builds **297 rows / 288 with a broker contact** from the same payloads. **Cost**: unchanged in kind (memo23 bills ~$0.0015/result) — London ~550-690 results ≈ $0.85/run; small cities cost proportionally less because empty pages return nothing. `LOOPNET_SEARCH_PAGES` is the volume/cost lever, `LOOPNET_MAX_ITEMS` (1000) the hard ceiling. **Deploy**: push to `main`. ⚠️ Local `main` was **10 commits behind** origin and the 2026-08-18 grading commit was **never pushed** — rebase before pushing (the 10 remote commits are all finance/nexudus/ava and touch nothing here). ⚠️ Still check that `LOCATION_SCRAPER_WEEKLY_SCHEDULE` is not pinned to 01:00 in the Function App settings — the 08:00 default matters much less now (the free search stage is not time-of-day sensitive the way the unblocker was) but W34 proves the override is live. **Known limitation accepted deliberately**: `price_monthly` is now NULL on LoopNet — the placard quotes `$X /SF/YR` strings with no numeric field, and the adapter has always refused to coerce an ambiguous unit (the detail payload used to fill it for ~47% of London). Per the brief — building information + agent email — this is out of scope; say so if pricing is ever needed and it can be derived from `price` × `sizeSf`. **Re-ran all 12 LoopNet cities the same day on the new code** (run_id `fix-{city}-20260819`, driven locally because the fix was not deployed yet; full orchestrator sequence incl. globe + gold refresh). **Gold LoopNet markers 417 -> 748, still 100% with an email.** Per city (W34 -> now): london 11 -> **260**, new york 24 -> **220**, toronto 24 -> **93**, seattle 19 -> **49**, cupertino 3 -> 5, austin 11 -> 13, redwood 15 -> 14, los angeles 74 -> 69, san francisco 14 -> 11, **palo alto 9 -> 1, san mateo 27 -> 6, san bruno 22 -> 0** (san bruno returned nothing, so gold kept its previous 20 markers; palo alto and san mateo returned thin non-empty sets, which DID supersede their better previous ones). **What the re-run taught us — the search stage is not infallible**: when the mobile SRP answers, our `min-space-size` filter is honoured and the placards are complete; when it 403s (`SRP ...: mobile search 403 (App Check)`, seen on loopnet.com) the actor re-queries from the geocoded city area **without the size filter** and recovers listings through the paid unblocker, which was dry again (`quota-throttled`) after a day of runs. That dilutes small cities with sub-threshold listings — San Mateo returned 50 raw items of which only 12 cleared 1500 m², against 27/27 on the old detail path — while dense cities still gain hugely. 6/12 runs were graded `degraded`, correctly, and the reason is in `error_message`. So the earlier "zero detail fetches" claim holds only while the SRP answers; the fix removes the *dependency* on the broken stage for the bulk of the data, it does not remove the stage. **Known grading gap (documented, deliberately not fixed here)**: `ls_assess_run_health` scores RAW item count, measured before the 1500 m² guardrail, so a fallback run with more raw items but fewer qualifying buildings is graded `completed` (San Mateo) — it is then not retried and its thinner marker set supersedes the good one. Closing it means grading on buildings-kept, which is computed after `ls_normalize`, i.e. restructuring the retry loop — not worth bundling into the same change as the fix. **Still open**: the App Check breakage is memo23's to fix; palo alto / san mateo / san bruno / san francisco deserve a re-run when the SRP and the unblocker quota recover (the deployed weekly + Wednesday retry pass does this automatically for the 6 `degraded` cities, but NOT for palo alto / san mateo, which were graded `completed`).)

Previous: 2026-08-18 (**LoopNet weekly runs collapsed across EVERY city since W30 — graded retries, a daytime slot, a mid-week second pass and a degraded status.** Symptom reported: London W34 returned **11 buildings / 1 new**. It was never London-specific — `bronze.n8n_location_scraper_logs` W28→W34: New York 456→24, Los Angeles 562→74, Austin 119→11, Seattle 104→19, San Francisco 155→14, London 25→11; Idealista / Immobilienscout / Otodom untouched, and the small LoopNet cities (cupertino 3, redwood 15, san bruno 22, san mateo 27) looked stable only because their candidate pool is a handful of listings. **Root cause is upstream and was proven from the actors' own run logs**: LoopNet locked its mobile API behind App Check, so memo23 403s on EVERY listing and recovers each through a paid unblocker chain that is now down to one throttled provider (its own log: `detail chain has ONE provider (scrapingbee) — a throttle there drops every detail row this run`). The search stage is fine — NY still enumerates 462 candidates — it is the detail recovery that collapses: W34 lost 34/48 London, 252/329 LA, 78/89 Austin, 57/76 Seattle. **The loss rate is time-of-day dependent**: London 2026-07-20 01:13 UTC = 39/48 lost, the SAME city at 08:46 the same day = 2/48, and 2026-07-31 10:36 UTC = 21/395 (374 buildings). All 12 LoopNet city runs fire between 01:13 and 01:45 UTC and compete for that one provider. Second, independent failure on London/Toronto: `ls_enumerate_loopnet_urls` (`abotapi/loopnet-scraper`) returns **0 items** when the residential edge refuses it (`Connection refused` x10 then its own stop-after-10-attempts guard) — the run still reports SUCCEEDED and the orchestrator silently fell through to the degraded broad search (Toronto has enumerated 0 EVERY week since it was added). Verified transient: the identical input returned the full 25 placards at 08:04 UTC on 2026-08-18. **Fix (4 parts)**: (1) new [shared/location_scraper/run_health.py](shared/location_scraper/run_health.py) — pure verdict from the actor's own per-listing log markers (`mobile API 403` / `unblocker fallback unavailable/failed` / `recovered via website unblocker`; verified `lost + recovered == attempted == items saved` on 6 real runs) plus the city's best raw volume over the last `LOCATION_SCRAPER_BASELINE_RUNS` runs, with new activity [shared/location_scraper/activities/run_health.py](shared/location_scraper/activities/run_health.py) (`ls_assess_run_health`) and `apify.get_run_log`; (2) the orchestrator's scrape phase is now the standalone generator `_scrape_with_retries` — up to `LOCATION_SCRAPER_MAX_ATTEMPTS` (3) graded attempts `LOCATION_SCRAPER_RETRY_DELAY_MINUTES` (30) apart, enumeration retried `LOCATION_SCRAPER_ENUM_ATTEMPTS` (3) times before the broad-search fallback, a FAILED/TIMED-OUT Apify run retried like a degraded one, and **the best attempt wins**: `fetch_and_persist_raw` replaces the run's bronze rows, so a weaker retry would otherwise overwrite a better one — its dataset is re-persisted at the end; (3) a run still degraded after every attempt keeps its buildings but is logged **`degraded`** (new status alongside `completed`, reason in `error_message`) — `ls_cities_needing_run` only skips `completed`, so the new **`location_scraper_weekly_retry`** timer (Wed 08:00 UTC, own instance id, no-op on a clean week) gives every bad city a second independent attempt, and `ls_alert_run_health` emails the week's city table when anything is unhealthy (`LOCATION_SCRAPER_ALERT_RECIPIENTS`, falls back to `SYNC_REPORT_RECIPIENTS`); (4) `LOCATION_SCRAPER_WEEKLY_SCHEDULE` default moved **01:00 → 08:00 UTC Monday** — the cheapest half of the fix. **Validation**: 22 new tests in [tests/test_location_scraper_run_health.py](tests/test_location_scraper_run_health.py) (thresholds pinned to the real W34 numbers, plus a fake orchestration context driving `_scrape_with_retries` for retry / best-attempt-wins / enumeration-retry / failed-run paths) — location-scraper suite **137 pass** (+7 pre-existing berlin_simulation failures, confirmed identical at HEAD in a clean worktree). Plus a **live read-only E2E** at 09:08 UTC: enumeration 25/25 URLs → memo23 detail 25/25 recovered, **0% loss, verdict ok** (the same pipeline that graded 71% loss at 01:15). **Cost**: memo23 bills $0.0015/result, so a full London run is ~$0.60 and a degraded attempt (14 results) ~$0.02 — retries are cheapest exactly when they trigger. **Deploy**: push to `main`. ⚠️ If `LOCATION_SCRAPER_WEEKLY_SCHEDULE` is set in the Function App app settings it overrides the new default — check/remove it, otherwise the runs stay at 01:00 UTC. **Recovery attempt the same day, and what it taught us**: London re-run on demand at 09:30 UTC came back **180 buildings / 28 new** (enumeration 317 URLs, 16% detail loss vs 71% at 01:15) and gold markers went 11 -> 177. The other 11 cities, fired in waves of 3 right after, ALL returned 0: that London run drained the actor's shared unblocker budget and every subsequent run logged `scrapingbee is quota-throttled (HTTP 401)`. On loopnet.com that kills the SEARCH stage too (no enumeration path there), so the US cities returned nothing at all — 8 FAILED, 3 `completed` with 0 buildings. No data was lost: `gold.sp_refresh_location_scraper_map_markers` keeps the latest run WITH globe rows, and empty runs contribute none. Added off the back of it: `parse_detail_health` detects the quota marker and `assess_run` returns `retry_useless`, which ends the attempt loop at once (3 identical failures per city still bill 3 actor starts) and puts the reason in `error_message`; the failed-Apify-run path now also grades the run so SQL says WHY. The quota is deliberately a RETRY signal only, never a quality one — the good London run drained it and must still read `ok`. **Still open**: the App Check / single-provider problem is the actor author's (memo23) to fix; the retries mask it, they do not repair it, and while the quota is dry no amount of retrying will bring the US cities back. Weeks W30–W34 were never re-run — those cities are missing new buildings until a manual re-run.)

Previous: 2026-08-10 (**Finance dashboard "Sync" button never pulled live Nexudus data — bronze open-invoice resync added to the HTTP refresh endpoint.** Symptom (reported from the finance app): pressing the dashboard's Sync button "does nothing" — verified live: `POST /api/finance/refresh-invoice-worklist` returned `{"ok": true, "silver": {"coworker_invoices": 0, ...}, "gold_rows": 505, "duration_s": 2.8}` — all-zero silver counts because [functions/finance_invoice_worklist_refresh.py](functions/finance_invoice_worklist_refresh.py) started at SILVER: it only re-promoted the same ~02:00 nightly bronze snapshot and rebuilt gold, so the numbers never changed no matter how often it ran. **Fix**: new step 0 re-fetches every open unpaid invoice OBJECT from Nexudus by ID before the silver→gold pass, reusing the nightly `_resync_open_invoices` from [functions/bronze_nexudus.py](functions/bronze_nexudus.py) (measured 2–46 s; SHA-256 hash-dedup makes unchanged invoices one GET + no write). Best-effort: Nexudus auth/API failure logs + reports `bronze_error` in the 200 response and the silver→gold pass still runs on existing bronze. Deliberately NOT fetched on-demand: payment histories (the `processing` flag — nightly/pre-send concern, and slow) and brand-new invoices outside the open-invoice candidate set. NB the resync run is tracked under the same `(nexudus, coworker_invoices_resync, bronze)` entity as the nightly run, so a manual daytime sync replaces the nightly row in the 06:00 sync-health report — sporadic button presses, acceptable (the pending `fix/dd-gateway-rejections-and-presend-refresh` branch adds `entity_suffix` for the systematic daily case; this change takes no dependency on it and merges cleanly either side). Paired AI-FINANCE app changes (same date, that repo): `/api/invoices/refresh` now calls the function synchronously and toasts the real outcome (was a fire-and-forget BackgroundTask that swallowed every failure), and the app's default `ETL_REFRESH_URL` hostname was fixed from the dead legacy `func-infinitspace-etl.azurewebsites.net` (DNS NXDOMAIN — Azure regionalized hostnames) to `func-infinitspace-etl-d0apgnfpcpdjcwdx.northeurope-01.azurewebsites.net` (prod App Settings already had the right one; only local dev was hitting the dead default). `py_compile` clean. Deploy = push to `main` (GitHub Actions).)

Previous: 2026-07-23 later (**LoopNet volume collapse — root-caused on BOTH sides and fixed; `maxItems` must always be sent.** Follow-up to the broker fix below. The collapse (LA 563→31 items, NY 456→58, Seattle 104→23 while small cities were untouched) was **forensically proven NOT to be ours** first: Apify archives each run's resolved INPUT, and the W28 (build 0.0.179) vs 07-23 (0.0.209) inputs are identical — same URL, `moreResults:true`, `includeListingDetails:true`, same proxy; `unlimited_items` defaults True on the HTTP path too so on-demand ≡ weekly; our SQL ingestion matches Apify's dataset count exactly for every city (LA 31=31, NY 58=58, Toronto 42=42); and ground truth from loopnet.ca showed **188** Toronto listings for the exact URL we send vs **42** returned. The actor dev confirmed: **a bad deploy had reverted the actor to an older search path LoopNet blocks**, since fixed. BUT his reply also exposed a real issue **on our side**: `LoopnetAdapter.build_input` OMITTED `maxItems` entirely on uncapped runs (`unlimited_items=True` → `max_items=None` → key never set), and the actor stops a search early at a small internal default when the key is absent. Measured live on Los Angeles, same build 0.0.209, same URL: **31 items (this morning) → 67 (post-dev-fix, key omitted) → 147 (explicit `maxItems`)**, with broker coverage 147/147 names and 141/147 emails. **Fix** [shared/location_scraper/adapters/loopnet.py](shared/location_scraper/adapters/loopnet.py): `build_input` now always sends `maxItems` — the explicit value when given, else `get_loopnet_unlimited_max_items()` (`LOOPNET_MAX_ITEMS`, default **1000**) — plus `maxUnblockerRequests` (`LOOPNET_MAX_UNBLOCKER_REQUESTS`, default **2000**), the per-run paid-unblocker budget a dense city can exhaust alone. New config helpers in [shared/location_scraper/config.py](shared/location_scraper/config.py). Tests updated (the old `test_build_input_unlimited_omits_max_items` asserted the buggy behaviour — now `test_build_input_unlimited_sends_explicit_ceiling`), location-scraper suite **112 pass**. **Cost note**: uncapping raises Apify spend (~$1.50/1,000 results) — 12 LoopNet cities at the new depth is a few $ per weekly run, still small, but `LOOPNET_MAX_ITEMS` is the lever if it ever needs trimming. Also audited our ≥1500 m² guardrail: it drops 45/305 items (32 genuinely under-threshold — concentrated in London 15/31 and Toronto 17/42, the two non-.com domains, reconfirming the actor ignores the `min-space-size` URL filter — plus 13 coworking listings carrying NO size anywhere in the payload; not a parse bug). **Still pending from the dev**: he is restoring the faster internal search path, which will make large searches quicker/cheaper and remove the unblocker budget entirely — re-run the cities once that lands to see whether LA returns toward its old ~563.)
Previous: 2026-07-23 (**LoopNet broker outage resolved end-to-end + new self-enriching broker directory (name→email memory).** Context: since ~2026-07-13 the memo23 actor returned `sourceType="listingWeb"` payloads with ZERO broker fields (LoopNet locked its mobile API behind a Firebase App Check token; the actor's website fallback didn't extract the broker card) — and because `gold.sp_refresh_location_scraper_map_markers` keeps only the LATEST run per (source, run_city) AND filters `lusha_email_1 IS NOT NULL`, every email-less weekly run silently WIPED its city's markers (only LA survived, ironically because its W30 run failed before materializing). An issue was filed with the actor dev (memo23/Muhamed Didovic) on 07-22; **he fixed it within a day** — `brokerName`/`brokerCompany`/`brokerPhone`/`brokerEmail` + a `brokers` co-broker list are back on listingWeb rows, verified live on all three domains (loopnet.com / loopnet.co.uk / loopnet.ca). Our adapter needed zero changes (reads exactly those flat keys). **All 12 LoopNet cities re-run on-demand 2026-07-23** → gold markers restored for every city (Toronto live for the first time). **New broker directory system** (this session's code): `silver.location_scraper_broker_directory` ([scripts/sql_scripts/location_scraper_broker_directory.sql](scripts/sql_scripts/location_scraper_broker_directory.sql), **applied to prod**) remembers every (broker name → email) pair ever observed; [shared/location_scraper/broker_directory.py](shared/location_scraper/broker_directory.py) (pure logic: `normalize_broker_name`, `extract_broker_records` — flat fields + `brokers` list, `resolve_email` — **conservative**: ambiguous names (several emails) resolve only on an exact company tie-break, else None) + hooks in [shared/location_scraper/activities/materialize_globe.py](shared/location_scraper/activities/materialize_globe.py): every LoopNet globe materialization (1) upserts the pairs it sees (self-enriching, graceful 42S02 skip if the table is missing — deploy-before-apply convention) and (2) back-fills email-less brokers (primary + co-brokers) from the directory into the globe email slots, titled `Broker — {company} (directory)`. **Backfill RAN against prod**: [scripts/python_scripts/backfill_broker_directory.py](scripts/python_scripts/backfill_broker_directory.py) seeded **2,447 (name, email) pairs / 2,436 brokers** from the 11,430-payload raw archive, then `--rematerialize` re-ran the latest globe run per city + gold refresh. **NB: local rematerialization MUST export the prod `GOOGLE_MAPS_API_KEY`** (pulled from the app settings) — the first pass without it fell back to Nominatim and shifted marker clustering; re-run with the key restored Google-cached coords. Final: **258 gold markers across 12 cities, 100% with email** (+19 vs payload-only, e.g. NY 48→53, Austin 27→30, Seattle 19→22). Tests: [tests/test_location_scraper_broker_directory.py](tests/test_location_scraper_broker_directory.py) (17: normalize/extract/resolve/globe-fallback incl. ambiguity-never-guessed) — location-scraper suite 111 pass (+ 7 pre-existing berlin_simulation failures). **Limitation**: the directory needs a broker NAME in the payload — during the total W30 blackout (no names at all) it would have recovered nothing. **STILL OPEN**: result-count collapse vs early July (LA 531→31 buildings, Seattle 104→22, London ~100→13 — `moreResults` seems ineffective on the new actor path; follow up with the dev, point 3 of the filed issue). Deploy = push to `main`; after deploy the weekly Monday runs self-enrich the directory with no further action.)
Previous: 2026-07-22 (**Location Scraper — Toronto added as the first LoopNet Canada city.** Pure config addition following the US-cities pattern (2026-06-26), but Canada needed its own `COUNTRY_CONFIG["canada"]` block: LoopNet Canada lives on **`loopnet.ca`** (own domain like the UK's loopnet.co.uk — `loopnet.com` does not serve Canadian searches), uses the **US-style path + suffix** (`office-space` + `for-lease`, not the UK `for-rent`), and the city slug needs the **province + `--canada`** (UK convention): `toronto-on--canada` — the bare `toronto-on` **404s** (verified live in the browser; the working URL `https://www.loopnet.ca/search/office-space/toronto-on--canada/for-lease/?min-space-size=16146` returns **189 Toronto office results**, prices in CAD). Edited: [shared/location_scraper/config.py](shared/location_scraper/config.py) (new `canada` block, `country_code="ca"`, min-space filter inherited); [functions/location_scraper.py](functions/location_scraper.py) `SCRAPE_CITIES` (now **23** cities); [shared/location_scraper/geocoding.py](shared/location_scraper/geocoding.py) `COUNTRY_NAME_BY_SOURCE["loopnet"]["toronto"]="Canada"`; [shared/location_scraper/adapters/loopnet.py](shared/location_scraper/adapters/loopnet.py) `currency_for_country` gains **CA/CAN/CANADA → CAD** (LoopNet has no currency field; without this Toronto would price in USD); `docs/location_scraper.md`. Tests: +1 `test_toronto_loopnet_canada` in `tests/test_location_scraper_resolve_source.py` + CAD cases in `tests/test_location_scraper_loopnet_adapter.py` — location-scraper suite 94 pass / 7 pre-existing berlin_simulation failures (IS24 fixture, unrelated), `py_compile` clean. Also this session: **resolved the CLAUDE.md changelog merge conflict** from `git pull` (local `d9fec05` "2026-07-08 evening" P&L entry vs remote 2026-07-16 open-invoice-resync entry — both kept in chronological order). To activate Toronto: push to `main` (GitHub Actions deploys); the next Monday 01:00 UTC weekly run picks it up, or trigger on-demand via `POST /api/scrape {"City":"Toronto"}` after deploy. NB: `LUSHA_SKIP_SOURCES` already covers loopnet, so Toronto broker emails come straight from the payload like the other LoopNet cities.)
Previous: 2026-07-16 (**Finance dashboard overstated outstanding balances — open-invoice resync added to the nightly Nexudus bronze sync.** Symptom (reported from the finance app): a settled invoice showed its full gross amount as "outstanding" when Nexudus's live balance was near-zero. **Root cause**: every layer is a faithful pass-through of the Nexudus invoice object's `DueAmount` (`transform_coworker_invoice: due_amount = raw["DueAmount"]` → silver → `gold.sp_refresh_invoice_worklist` `SELECT nci.due_amount` → app). The LIVE invoice object already had the correct near-zero `DueAmount` (the payment/credit recorded on `PaidAmount`/`CreditedAmount`) — but Nexudus applied the payment/credit **without bumping the invoice's `UpdatedOn`** (it stays frozen, `UpdatedBy='[System]'`). Because [functions/bronze_nexudus.py](functions/bronze_nexudus.py) `_sync_coworker_invoices` fetches incrementally on `from_CoworkerInvoice_UpdatedOn` (2-day window, `NEXUDUS_INVOICE_LOOKBACK_DAYS`), a settled invoice whose `UpdatedOn` never advances is **never re-pulled** and bronze/silver/gold freeze at the first snapshot's gross amount. Histories are fetched fresh via their own DB-driven candidate list, so they misleadingly showed only failed/AWAITING DD attempts (no SUCCESS) even though a credit had settled the balance. Widening the window is NOT a fix (UpdatedOn never advances). **Fix** (mirrors the existing PDF-cache "open finance-worklist invoice" candidate pattern): new `_resync_open_invoices` step in `nexudus_to_bronze` (runs right after `_sync_coworker_invoices`) re-fetches every open unpaid invoice OBJECT by ID via `client.get_one("billing/coworkerinvoices/{id}")`, independent of `UpdatedOn`; candidate list `_load_open_invoice_ids` reads SILVER (`due_amount>0 AND paid/void/draft/is_deleted=0 AND due_date >= now - NEXUDUS_INVOICE_RESYNC_LOOKBACK_MONTHS` (default 12)) — silver-sourced so `is_deleted` filters out invoices removed in Nexudus (avoids a nightly flood of 404s). SHA-256 hash-dedup in `BronzeWriter.write_coworker_invoices` means unchanged invoices cost one GET and no write; the existing silver watermark + `gold.sp_refresh_invoice_worklist` then materialize the corrected balance. New RunTracker entity `coworker_invoices_resync`. `functionTimeout` is 45 min so ~500 by-ID GETs at `max_concurrent=3` is safe. **Backfill RAN against prod 2026-07-16** via new one-off [scripts/python_scripts/resync_open_invoices.py](scripts/python_scripts/resync_open_invoices.py) (`--apply` = re-fetch open invoices → bronze, rebuild silver coworker_invoices, `EXEC gold.sp_refresh_invoice_worklist`; dry-run by default) — **7 open invoices had their stale balances corrected** (each dropped from a near-gross figure to its true near-zero live balance). **Status: code committed + not yet deployed** (`py_compile` clean; verified end-to-end against prod — gold now reads the corrected values). Deploy = push to `main` (GitHub Actions); after deploy the nightly 02:00 bronze sync keeps balances fresh with no further action. No AI-FINANCE app change needed — the app was faithful.)
Previous: 2026-07-08 evening (**Xero monthly P&L — RE-CONSENT DONE + FULL BACKFILL COMPLETE; P&L is now live**. The `accounting.reports.profitandloss.read` re-consent (last blocker from the entry below) was granted via the manual link flow: `xero_start_oauth.py` → link sent to Baptiste's manager (a Xero login with access to all 8 orgs) → Allow → the manager returned the redirect URL (`https://www.xero.com/?code=...&scope=...&state=...`) copied from the address bar → `xero_complete_oauth.py --redirect-url "<url>"` within the ~5-min code window. Result: `handle_callback` stored the token with **`deleted_stale_rows: 0`** (all 8 tenants preserved — the manager's login covers every org, so `save_tenants` pruned nothing) and the token now carries the P&L scope (verified on `meta.xero_connections.scope`). Then `xero_sync_profit_loss.py --force-full` → **8/8 tenants, 240 tenant-months (2024-01→2026-06), 5,302 silver rows (4,759 account + 543 summary), 0 skip / 0 fail**, uniform coverage across all tenants, currencies GBP (UK) / EUR (NL+DE). Aldgate June 2026 sanity check: Total Income 419,174.49 GBP, Total Cost of Sales 4,827.36, Total Operating Expenses 150,870.31 (validate against finance's expected figures via `silver.vw_xero_profit_loss_monthly_accounts`). The token lives in the prod DB (`meta.xero_connections`), so the deployed nightly 04:00 `xero_invoice_sync` P&L step now succeeds on its own and re-pulls the last 3 months each run — no redeploy, no code change (the P&L sync code was already shipped, see the entry below). **State-TTL trick used**: `UPDATE meta.xero_oauth_states SET expires_at = DATEADD(HOUR, 72, GETUTCDATE()) WHERE state = ? AND consumed_at IS NULL` removed the 10-min rush on sending the link to a third party (the Xero redirect `code` itself is still ~5 min, non-extensible). Confirmed local `INTEGRATIONS_ENCRYPTION_KEY` == prod (sha256 `7abf3ef3…`, 44-char Fernet) so the locally-completed consent is decryptable by prod — the old "invalid 37-char local key" note is now STALE. NB: a "zero-copy-paste" consent (loopback/ngrok, or making the deployed `integrations/xero/callback` anonymous) was explored and deliberately NOT pursued — the manual one-URL flow sufficed and touched no prod config/code. No code committed this session.)
Previous: 2026-07-08 later same day (**Xero monthly P&L report ingestion (by a parallel agent session) + the OAuth scope mystery SOLVED: the app is granular-scopes-only, `accounting.reports.read` no longer exists for it — use `accounting.reports.profitandloss.read`**. Timeline of the day: (1) morning — bank transactions re-consent by Bryan (granular list) + deploy + full backfill **5,193 txns / 5,315 lines, 8/8 tenants exact vs API volumetry; Bank Fees hole filled: ~114.9k GBP + ~25.9k EUR net** (vs one 29.50 line); incremental watermarks live, budget tool reads `silver.vw_xero_bank_transaction_pnl_lines` (group by `currency_code`, sign via `[type]`). (2) midday — a parallel agent session shipped the **monthly P&L report ingestion** per finance's request (replaces the credit-notes + manual-journals asks): [shared/xero/profit_loss_sync.py](shared/xero/profit_loss_sync.py) (`XeroProfitLossSyncService`, one `GET /Reports/ProfitAndLoss?standardLayout=true` per tenant×month; backfill from `XERO_PROFIT_LOSS_BACKFILL_START`=2024-01; re-pulls last `XERO_PROFIT_LOSS_REFRESH_MONTHS`=3 months each run for late postings; runs LAST in the `xero_invoice_sync` 04:00 timer; scope-skip + schema-skip like the bank tx sync), schema [scripts/sql_scripts/xero_profit_loss_schema.sql](scripts/sql_scripts/xero_profit_loss_schema.sql) (**applied to prod**: `bronze.xero_profit_loss_reports`, `silver.xero_profit_loss_accounts` one row per tenant×month×account + summary rows, view `silver.vw_xero_profit_loss_monthly_accounts`, meta watermark columns), tests `tests/test_xero_profit_loss.py`, runner `scripts/python_scripts/xero_sync_profit_loss.py`; CI deployed. BUT that session could not produce a working consent link and made 3 blind scope-name commits (`4688ae2`,`d0c81a3`,`a41ee8a`) switching everything to the broad `accounting.transactions.read` family. (3) afternoon — root cause proven with an **HTTP pre-flight oracle** (GET the authorize URL; the invalid_scope error page renders BEFORE login, so scope lists are testable without consuming a consent — scratch trick worth reusing): the broad family (`accounting.transactions[.read]`, `accounting.reports.read`) → `invalid_scope`; the granular family → login page. Per Xero's 2026-03-02 change (developer.xero.com/faq/granular-scopes), **apps created on/after 2026-03-02 — "iS Finance Dev" was created 2026-04-03 — can ONLY use granular scopes**; `accounting.reports.read` is replaced by per-report scopes; the P&L one is **`accounting.reports.profitandloss.read`** (pre-flight PASS, incl. combined with the full granular list). All scope references reverted/fixed accordingly (`.env`, [.env.example](.env.example) — now carries a warning comment, [shared/xero/oauth.py](shared/xero/oauth.py) default, [scripts/python_scripts/xero_open_auth.py](scripts/python_scripts/xero_open_auth.py), warning strings in [functions/xero_sync.py](functions/xero_sync.py)/[shared/xero/bank_transaction_sync.py](shared/xero/bank_transaction_sync.py)/[shared/xero/client.py](shared/xero/client.py)/harness/tests). Full Xero suite **48 pass**. Canonical `XERO_SCOPES` (pre-flight-verified): `offline_access accounting.settings accounting.settings.read accounting.invoices accounting.invoices.read accounting.payments accounting.payments.read accounting.banktransactions accounting.banktransactions.read accounting.manualjournals accounting.manualjournals.read accounting.reports.profitandloss.read`. **Remaining: ONE re-consent by Bryan with that list** (same live runbook: `xero_start_oauth.py` → Allow → `xero_complete_oauth.py` within 10 min), then the P&L backfill (2024-01→now, 8 tenants) via `xero_sync_profit_loss.py` or the nightly timer; validation target: Aldgate June 2026 vs finance's expected figures.)
Previous: 2026-07-08 (**Xero bank transactions ingestion built — bronze + silver + P&L view; BLOCKED on OAuth re-consent for the `accounting.banktransactions` scope**. Why: bank fees (Bank Fees 404, Revolut Merchant Fees, direct debits) are coded straight from the Revolut bank feeds as spend-money and never appear on ACCPAY invoices — verified in prod: exactly **1** "Bank Fees" invoice line (29.50) across all 158,552 lines, while the 404 account exists in all 8 tenants — so the budget tool's P&L actuals had a structural hole. New [shared/xero/bank_transaction_sync.py](shared/xero/bank_transaction_sync.py) (`XeroBankTransactionSyncService`, mirrors `invoice_sync.py`): per-tenant incremental via `If-Modified-Since` with watermark `meta.xero_tenants.last_bank_transaction_modified_utc` (+ started/completed/error columns; falls back to silver `MAX(updated_date_utc)` when the meta columns are absent), bronze latest-payload MERGE → silver header MERGE → lines DELETE+INSERT; runs inside the existing `xero_invoice_sync` timer (04:00 UTC) after PDF caching ([functions/xero_sync.py](functions/xero_sync.py)); `XERO_BANK_TX_SYNC_FORCE_FULL=1` to force full. `XeroApiClient.get_bank_transactions()` added ([shared/xero/client.py](shared/xero/client.py)). **Scope-skip**: `meta.xero_connections.scope` (checked 2026-07-07) only carries `accounting.settings[.read] accounting.invoices[.read]` — NO banktransactions — so per-tenant 401/403 are caught, logged as a warning, and stamped into `last_bank_transaction_sync_error` (same pattern as the accounts sync): **deployable now, data flows the night after the re-consent, no redeploy**. Re-consent runbook: local `.env` already has the full `XERO_SCOPES` list (incl. banktransactions + manualjournals + payments); needs the **prod `INTEGRATIONS_ENCRYPTION_KEY`** in local `.env` (current local value is an invalid 37-char Fernet key — tokens undecryptable locally) and the consent click by **the Xero login that holds the original connection** (created 2026-04-03; Baptiste's own login lacks the role: "You must be a Standard, Advisor, or Administrator user with the Connected Apps feature") — a partial-access user would make `save_tenants` PRUNE the missing tenants (it deletes rows absent from `/connections`). One pass by the original user upgrades all 8 tenants (scopes live on the token, not per-org); the state link expires in 10 min and the redirect code in ~5, so do it live (start → consent → `xero_complete_oauth.py`). Schema [scripts/sql_scripts/xero_bank_transactions_schema.sql](scripts/sql_scripts/xero_bank_transactions_schema.sql) (idempotent, **NOT yet applied**): `bronze.xero_bank_transactions`, `silver.xero_bank_transactions` (type/status/is_reconciled/bank account/contact/totals), `silver.xero_bank_transaction_line_items` (same shape as invoice lines), 4 meta watermark columns, and **`silver.vw_xero_bank_transaction_pnl_lines`** — the budget-tool shape (`xero_tenant_id, [date], account_code, line_amount_net, [status], [type]`): `line_amount_net = line_amount - tax_amount` when `LineAmountTypes='Inclusive'` (bank-feed common case; 1,248 existing ACCPAY invoices are already Inclusive so the pattern is real), filtered to `AUTHORISED` + `type IN ('SPEND','RECEIVE')` — prepayment/overpayment variants excluded (they hit balance-sheet accounts then get allocated to invoices → double-count risk; "Prepayments" is already the #1 ACCPAY account at 9.1M), `*-TRANSFER` excluded (no P&L impact). Deploy-before-apply safe: missing tables → run skips with a warning (ava_refresh convention). Deletions are status-based (DELETED + UpdatedDateUTC bump) — no reconcile job, same as xero_invoices. Tests [tests/test_xero_bank_transactions.py](tests/test_xero_bank_transactions.py) (16: transform mapping incl. .NET dates, MERGE placeholder parity machine-checked, 403-skip, happy path, tenant-error isolation, watermark resolution incl. naive-datetime + silver fallback) — full Xero suite 41 pass, `py_compile` clean. Local harness [scripts/python_scripts/test_bank_transactions_sync.py](scripts/python_scripts/test_bank_transactions_sync.py) (dry-run scope probe / `--count` volumetry — unknown until the scope lands / `--write` backfill). **Run order: (1) Bryan-or-original-user re-consent, (2) apply schema via `apply_schema_script.py`, (3) push to `main` (GitHub Actions deploys), (4) `test_bank_transactions_sync.py --count` then `--write` for the initial backfill (or let the 04:00 timer do it), (5) verify via the view + reconcile one tenant's Bank Fees month vs Xero UI.** NB CLAUDE.md previously said "all 12 Xero tenants connected" — prod has **8** (`meta.xero_tenants`).)
Previous: 2026-07-08 (**Applied the coworkers `tariff_name` widen migration to prod + healed the entity — closes out the recurring `nexudus/coworkers/silver` truncation red row.** The 2026-07-07 fix (widen `silver.nexudus_coworkers.tariff_name`/`next_tariff_name` `NVARCHAR(512)`→`NVARCHAR(MAX)`) was committed + deployed in `01831fe`, but a change to the schema *file* never touches the live DB, so the 2026-07-08 06:00 health report still flagged the same `String or binary data would be truncated ... column 'tariff_name'` (0 read / 0 written). The outstanding step (a) from the 07-07 run order — apply the migration to the warehouse DB — had simply never been run. **Root cause reconfirmed live**: exactly **one** coworker — `source_id=1421023461` (Neal Swaelens) — whose aggregated (comma-joined) `TariffName` grew to **578 chars** on 2026-07-07 (the 07-06 run was green, 604 written), overflowing the 512-wide column and aborting the entire `execute_many` MERGE (0/0). **Action (no code change — code was already correct + deployed)**: (1) ran `scripts/python_scripts/apply_schema_script.py scripts/sql_scripts/nexudus_coworkers_tariff_name_widen_migration.sql` against the warehouse — both columns now `CHARACTER_MAXIMUM_LENGTH=-1` (MAX), matching the sibling `coworker_contract_tariff_names`; (2) re-ran the coworkers Bronze→Silver step manually under `RunTracker` (mirrors `functions/silver_worker.py::silver_entity_worker`) → **864 read / 805 written / 0 errors**, the 578-char value now stored in silver, run recorded `status='success'` (`triggered_by='manual'`) so the watermark advanced past 07-06 and the ~2-day coworker backlog is healed. The next nightly 02:30 UTC fanout is now self-sufficient and the 06:00 health-report row goes green. The 07-07 invoice-reconcile 403 fix shipped in the same `01831fe` and needs no DB action. Git tree unchanged apart from this changelog note.)
Previous: 2026-07-07 (**Two sync-health failures fixed: coworkers-silver truncation + invoice-reconcile 403**. The 2026-07-07 06:00 health report flagged 2 red rows, both independent. **(1) `nexudus/coworkers/silver` — `String or binary data would be truncated ... column 'tariff_name'`** (0 read / 0 written, 1s): `silver.nexudus_coworkers.tariff_name` was `NVARCHAR(512)` but the transformer [shared/nexudus/transformers/coworkers.py](shared/nexudus/transformers/coworkers.py) copies `TariffName` through with no length cap, and since the 2026-06-10 switch to the list endpoint `GET /spaces/coworkers` that field can be an aggregated (comma-joined) value across all of a member's tariffs — its sibling `coworker_contract_tariff_names` is already `NVARCHAR(MAX)` for the same reason. One oversized value aborts the whole `execute_many` MERGE in [shared/azure_clients/silver_writer_coworkers.py](shared/azure_clients/silver_writer_coworkers.py) (writer uses the default `fast=False` path, so widening to MAX is safe — no `setinputsizes`/`fast_executemany` truncation exposure). **Fix**: widen `tariff_name` **and** `next_tariff_name` (identical exposure) to `NVARCHAR(MAX)` — source schema [scripts/sql_scripts/nexudus_billing_sync_schema.sql](scripts/sql_scripts/nexudus_billing_sync_schema.sql) updated + idempotent migration [scripts/sql_scripts/nexudus_coworkers_tariff_name_widen_migration.sql](scripts/sql_scripts/nexudus_coworkers_tariff_name_widen_migration.sql) (guards on `CHARACTER_MAXIMUM_LENGTH <> -1`). No transformer/writer change. **(2) `nexudus/coworker_invoices_reconcile/silver` — `403 Forbidden ... coworkerinvoices?page=58...`** (0/0, 23s): `nexudus_invoice_reconcile._fetch_active_invoice_ids` did one unbounded `get_all` over `DueDate >= now-365d`, which paginated ~58 pages deep and hit a Nexudus deep-pagination 403; the client's `_is_retryable` ([shared/nexudus/client.py](shared/nexudus/client.py)) only retries 429/5xx, so the single 403 aborted the run (before any soft-delete — nothing was wrongly deleted, the sweep just didn't run). **Fix** [functions/nexudus_invoice_reconcile.py](functions/nexudus_invoice_reconcile.py): the active-id fetch is now split into contiguous `DueDate` sub-windows (`NEXUDUS_INVOICE_RECONCILE_WINDOW_DAYS`, default 30) so every request stays shallow, plus one unbounded future window (`due_date >= now-1d`) so future-dated invoices are still covered (else they'd be wrongly soft-deleted); a window error still propagates and fails the run (fail-safe — under-counting active ids must never silently soft-delete). New helper `_fetch_invoice_ids_in_window` uses the Nexudus `from_`/`to_CoworkerInvoice_DueDate` range convention. `403` is deliberately NOT added to the global retry set (it can be a genuine permanent auth error elsewhere). `py_compile` clean. **Status: code UNCOMMITTED + not deployed.** Run order: (a) apply the widen migration to the warehouse DB (`scripts/python_scripts/apply_schema_script.py scripts/sql_scripts/nexudus_coworkers_tariff_name_widen_migration.sql`), then (b) push to `main` (GitHub Actions) to deploy the reconcile change. After the migration, the next `bronze_to_silver` fanout self-heals coworkers; the reconcile self-heals on its next 05:15 UTC firing.)
Previous: 2026-07-01 (**TeamAndy Firestore → Azure one-shot migration — DATA MIGRATION COMPLETE (new `teamandy` operational schema + cache Table Storage + location-images Blob)**. First step of the Google→Azure move; goal is to **fully close Firebase**. Ported the proven AI-teamandy ETL kit into this repo at [scripts/python_scripts/teamandy_migration/](scripts/python_scripts/teamandy_migration/) (`extract`→`transform`→`load_sql`/`load_tables`→`validate`), reusing this repo's `shared.azure_clients.sql_client` + `shared.firebase.client`. **SQL**: new **`teamandy`** schema — operational OLTP, deliberately **NOT** bronze/silver/gold (a purpose-named schema like `ava`; DDL only `CREATE SCHEMA`, no `ALTER DATABASE`, DB-level RCSI already ON) — **42 tables** created on prod via `scripts/sql_scripts/teamandy_00…50.sql` (00 schema, 10 core CRM ×21, 20 scraping/ref ×9, 30 ops/config ×8, 40 runtime graph_subscriptions, 50 company index ×3); data imported **262,877 rows / 0 rejects / validated** (29,206 leads, 123,855 contact-persons, 20,081 company_index; dual-shape leadListId = 12,676 legacy scalar + 16,530 junction rows; FKs made **soft** + several `ISJSON`/UNIQUE/NOT-NULL constraints relaxed after a 13,950-reject rehearsal, so a bad row NULLs instead of aborting the batch). **Caches → Azure Table Storage** on `staccinfinitspaceprod001` (point-get by key; kept so cutover doesn't re-spend paid Apollo/Lusha budget): **11 tables / 124,784 entities / 0 errors** (apolloNeg 26,962, lushaNeg 19,226, lusha 13,109, apollo 4,460, cacheUnifiedV2 24,517, companyEnrichmentCache 29,424, cacheData 6,989, maps 76, cacheScrapingSettings 18, lushaQuota 1, cacheRoot 2); bodies >32 KB spill to Blob **`cache-bodies`** (8,067 blobs); epoch-ms ints stored as strings to dodge Edm.Int32 overflow; `pyodbc fast_executemany` disabled (segfault on NVARCHAR(MAX)). **Location images → Blob `location-images`**: 11 images (~11.9 MB) copied **byte-exact** from `gs://wearebeyond-bd776/locations/`, and `teamandy.locations.image_blob_url` set on **all 9 locations** (2 orphan Firestore-auto-id images preserved in Blob but unlinked — no matching numeric location uid). **User-excluded after a usage audit (NOT migrated)**: collections interactions / tasks / integrations / analytics / deleted / cache_main / cache_main_competence / pre-leads + the test_leads junk collection; their tables were dropped from the ported DDL. Table-plane writes used the storage **account-key connection string** (the deploy identity has Blob AAD but not `Storage Table Data Contributor`) — grant that role later for AAD reads. New pip deps: `azure-data-tables>=12.7.0`, `google-cloud-storage>=2.14.0`. **Status: ALL Firebase data now lives in Azure (SQL + Table + Blob); the migration ETL is one-shot + idempotent, safe to re-run for a final freeze-time sync right before cutover.** NEXT (separate, large task): repoint every AI-teamandy folder's Firebase reads/writes/deletes to Azure SQL / Blob, then turn Firebase off. **NB: found + resolved a pre-existing UNRESOLVED git merge conflict in this changelog (HEAD 2026-07-01 competence-sync vs branch `bce0d5a` 2026-06-30 location-scraper) — kept BOTH sides in chronological order below, nothing dropped; please verify against your git state.**)
Previous: 2026-07-01 (**Competence sync stuck "running" forever — fixed a self-perpetuating watermark/perf bug**. Symptom: the daily sync-health-report showed `competence/competence/silver` permanently "still running". Diagnosis via `meta.sync_runs`: this was the deployed `competence_sync` timer's **first-ever real executions** (2026-06-30, 08:02 and 08:56 UTC — `silver.competence_*` had been populated entirely by earlier one-off backfill scripts, never by the timer), and both got stuck in the exact same step and never returned. Root cause chain: (1) [shared/azure_clients/silver_sync.py](shared/azure_clients/silver_sync.py) `get_last_successful_run_started_at()` requires `status='success'`, which had never existed for this entity, so the incremental watermark was permanently `NULL`; (2) with no watermark, `load_latest_bronze_rows()` drops its `WHERE b.synced_at >= ?` clause and [shared/azure_clients/silver_writer_competence.py](shared/azure_clients/silver_writer_competence.py) reprocesses **all ~15,500 competitors every run** instead of just the day's changes; (3) [shared/azure_clients/sql_client.py](shared/azure_clients/sql_client.py) `execute_many()` called `cursor.executemany()` without `fast_executemany`, so pyodbc did one network round-trip per row — a 44-parameter MERGE × 15,516 rows this way is slow enough to risk `host.json`'s `functionTimeout: 00:45:00`; (4) when the invocation is killed mid-write, `RunTracker.__aexit__` ([shared/azure_clients/run_tracker.py](shared/azure_clients/run_tracker.py)) never runs, so the row inserted as `status='running'` is orphaned forever; (5) since the run never succeeds, the watermark never advances, so the next attempt repeats the identical full reprocess — a self-sustaining loop, not a one-off blip. Confirmed via `sys.dm_exec_requests`/`sys.dm_tran_active_transactions` that neither stuck run's underlying process is still alive (no active session at those timestamps) — the DB rows are pure orphaned metadata, not a live hang. **Fix**: `SQLClient.execute_many()` gained opt-in `fast: bool` + `input_sizes: list` params (default False/None, zero behavior change for the ~30 other callers of this shared method) — `fast_executemany` alone is **not safe** on variable-length NVARCHAR columns (empirically reproduced "String data, right truncation" even on bounded, non-MAX columns whose data length varies across a batch); pinning `cursor.setinputsizes()` to the real column widths before executemany eliminates that risk. `silver_writer_competence.py._sync_competitors()` now calls `execute_many(..., fast=True, input_sizes=...)` in chunks of 1000. Benchmarked against a schema-faithful scratch table: the real 44-param MERGE went from ~26 ms/row (~6-7 min extrapolated for 15,516 rows from a non-Azure network — likely worse from a loaded/blocked connection in prod) to ~0.6 ms/row (~40x faster), so a full reprocess now finishes in seconds regardless of network location. `_sync_lists()` (only ~31 rows, and its `last_error` column is `NVARCHAR(MAX)`, which is exactly the truncation-risk case) was deliberately left on the slow/default path — not worth the risk for 31 rows. **Recovery**: the two orphaned `meta.sync_runs` rows (`FD2D2BDC-...`, `2B2B933A-...`) were manually marked `status='failed'` with a diagnostic `error_message`, clearing the way for the next run to finally record a `success` and let the watermark self-heal back to true incremental. Tests: `tests.test_competence_transformer` + `tests.test_competence_classification` (34 pass, unaffected). **Separately flagged (out of scope, not fixed here)**: while diagnosing, found 7 unrelated sessions in `sys.dm_tran_active_transactions` sitting idle-in-transaction for up to ~10 hours (not blocking anything currently, but a real connection/transaction leak somewhere in the function app — timestamps don't line up with competence_sync, one cluster of 5 logged in simultaneously at 2026-07-01 06:00:00 UTC, suspiciously matching `sync_health_report`'s schedule). Worth a follow-up investigation. **Status: code fixed + orphaned rows cleaned up in prod DB; code changes UNCOMMITTED — push to `main` to deploy** so the next `competence_sync` firing (Mon-Sat 04:30 UTC) actually completes and re-establishes the watermark.)
Previous: 2026-06-30 (Location Scraper — **Phoenix + Atlanta removed from the weekly US LoopNet scrape**. Pure config removal, the inverse of the 2026-06-26 add. Edited: [shared/location_scraper/config.py](shared/location_scraper/config.py) `COUNTRY_CONFIG["us"]["cities"]` (dropped `phoenix`/`atlanta`); [functions/location_scraper.py](functions/location_scraper.py) `SCRAPE_CITIES` (now **22** cities); [shared/location_scraper/geocoding.py](shared/location_scraper/geocoding.py) `COUNTRY_NAME_BY_SOURCE["loopnet"]` (dropped both); `tests/test_location_scraper_resolve_source.py::test_us_cities_all_resolve` (dropped both → 18 expected US cities) + `docs/location_scraper.md`. NB the LoopNet adapter + `tests/test_location_scraper_loopnet_adapter.py` keep their *Phoenix sample listings* — those exercise the surface-parsing logic (`name`-range / `sizeSf` parse), not the city schedule, so they stay. **DB cleanup**: new one-off script [scripts/sql_scripts/location_scraper_delete_phoenix_atlanta.sql](scripts/sql_scripts/location_scraper_delete_phoenix_atlanta.sql) deletes all Phoenix/Atlanta rows across every Location-Scraper table (buildings/listings/listing_contacts/contacts cascade by run_id + orphan-safe building/contact cleanup; raw/run_quality/lusha_diagnostics/globe_v2/globe_quality/logs by city+run_id; then `EXEC gold.sp_refresh_location_scraper_map_markers` to rebuild gold from the trimmed silver globe). Transactional — inspect the PRINT counts, then COMMIT. **Not run by me** (user runs it against the warehouse DB). To activate the config change: push to `main` (GitHub Actions deploys); the next Monday 01:00 UTC weekly run then skips both cities. No schema change.)
Previous: 2026-06-29 (Location Scraper — **LoopNet fully broken (0 buildings, ALL cities incl. the new US ones) FIXED — memo23 actor changed its output schema; flow switched off the enumeration 2-step back to the broad search**. Symptom: the 6 new US cities (Redwood City, San Mateo, San Bruno, Cupertino, Phoenix, Atlanta) added 2026-06-26 produced **0 buildings**, and `bronze.n8n_location_scraper_logs` showed every LoopNet city (London + all US) `failed`/`completed` with `buildings_found=0` from W27 (06-29) onward, while W26 (06-22) worked (LA 387, SF 155, NY 58). **Root cause (confirmed against `bronze.location_scraper_raw` + Apify actor metadata):** the memo23 LoopNet actor `0ZCQONxB3BdyOzrbD` (memo23/loopnet-scraper-ppe) was **rebuilt 2026-06-27 (build 0.0.167)** and changed its output. Three payload shapes seen — OLD detail (`header.subtext`+`spaces[]`+`broker*`, worked ≤W26), NEW enumerated listing-URL ("listingWeb": surface only in `name`, facts in `propertyFacts`, **no header/spaces, no broker fields at all**, broke 06-26+), and broad-search+`moreResults` (`sizeSf` abbreviated like "36.8K", full `broker*` incl. `brokerEmail`). The adapter only read `header.subtext`/`spaces[].size`, so `available_surface_sqft_from_payload` returned None → every building failed the ≥1500 m² guardrail → 0 buildings. Many W27 cities also `failed` on the `ls_enumerate_loopnet_urls` (abotapi) 45-min timeout. **Fix:** (1) [shared/location_scraper/adapters/loopnet.py](shared/location_scraper/adapters/loopnet.py) — `available_surface_sqft_from_payload` now tries header.subtext → spaces → **`name`/`listingName` range parse (upper bound)** → **`sizeSf` (K/M abbrev)**, robust to all 3 schemas; `_first_sf` now matches "sq ft" (UK) as well as "SF"; new helpers `_available_sf_from_name`, `_parse_abbrev_sf`; `build_input` adds `"moreResults": True` (bypasses memo23's 500-item cap). (2) [functions/location_scraper.py](functions/location_scraper.py) — orchestrator **no longer calls `ls_enumerate_loopnet_urls`** (LoopNet now scrapes the space-available-filtered broad-search URL directly; the enumerated listing-URL path lost broker contact + surface in the rebuild). The activity + `enumerate_loopnet.py` remain registered but unused (reversible). (3) [shared/location_scraper/activities/materialize_globe.py](shared/location_scraper/activities/materialize_globe.py) — LoopNet currency now derived from the resolved `country_code` (broad-search payload has no `country` field, so UK would otherwise default to USD). **Validation (no deploy — user pushes):** re-normalized the real broken raw in SQL with the patched adapter — Phoenix 0→97, San Mateo 0→19, San Bruno 0→12, London 0→16, Cupertino 0→5, Redwood 0→1; OLD NY-W26 schema still parses (58); end-to-end production-path probe (`resolve_source`→`build_input`→live Apify→`normalize`) for Atlanta returned **12/12 buildings, all with broker emails**; the redwood broad-search probe returned **16 qualifying buildings (vs 1 on the enumeration path)** — the flow switch also improves coverage. Tests: `tests/test_location_scraper_loopnet_adapter.py` +6 (18 pass, incl. K/M `sizeSf`, name-range, broad-search-with-broker, listingWeb-name normalize, `moreResults`); full LS suite 93 pass / 7 pre-existing berlin_simulation failures (IS24 region/city fixture, unrelated, confirmed fail on un-edited code). **Cost note:** `moreResults` returns the full qualifying set (Apify $0.005/result) — more than the capped broad search but the intended full-coverage behaviour; dense-city >500 bypass relies on the actor's documented `moreResults` and should be eyeballed on the first live weekly run. To activate: push to `main` (GitHub Actions deploys), then let the Monday 01:00 UTC weekly run go or trigger per-city `POST /api/scrape {"City":"Phoenix"}`. No schema/SQL change.)
Previous: 2026-06-26 (Location Scraper — **6 LoopNet US cities added: Redwood City, San Mateo, San Bruno, Cupertino (CA), Phoenix (AZ), Atlanta (GA)**. Pure config addition — reuses the existing LoopNet 2-step flow (`ls_enumerate_loopnet_urls` → memo23), adapter, persist, globe, and the `LUSHA_SKIP_SOURCES={"loopnet"}` path unchanged. Edited: [shared/location_scraper/config.py](shared/location_scraper/config.py) `COUNTRY_CONFIG["us"]["cities"]` (slugs `redwood-city-ca`, `san-mateo-ca`, `san-bruno-ca`, `cupertino-ca`, `phoenix-az`, `atlanta-ga` — `{city}-{state}` convention, `min_space_size_sqft=16146`/≥1500 m² floor inherited from the `us` block); [functions/location_scraper.py](functions/location_scraper.py) `SCRAPE_CITIES` (now **24** cities — the weekly Monday 01:00 UTC parent picks them up automatically, multi-word names like `redwood city`/`san mateo`/`san bruno` slugified for run_id/instance_id by the existing `c.replace(' ', '-')`); [shared/location_scraper/geocoding.py](shared/location_scraper/geocoding.py) `COUNTRY_NAME_BY_SOURCE["loopnet"]` (→ "United States" for the geocode fallback, since LoopNet payloads carry no coordinates). Tests: `tests/test_location_scraper_resolve_source.py::test_us_cities_all_resolve` extended — 20 pass. Docs: `docs/location_scraper.md` supported-cities table + scheduled-cities list + US slug examples updated. **No deploy done this session** (user deploys). To activate: push to `main` (GitHub Actions `main_func-infinitspace-etl.yml` runs tests + deploys), then either let the weekly timer run, or trigger each city on-demand: `POST https://func-infinitspace-etl.azurewebsites.net/api/scrape?code=<host-key>` with body `{"City":"Redwood City"}` (one per city) — the new cities will 400/ValueError until the deploy lands. NB existing-building contact-link + Decimal-phantom persist fixes from 2026-06-11 already cover these cities; first-ever run is all-new buildings so contacts link on the insert path regardless.)
Previous: 2026-06-24 (**Nexudus invoice PDF download fixed (COWORKER_INVOICE_PRINT run-command flow)** — the finance dashboard needs the actual Nexudus coworker-invoice PDF document. A Nexudus PDF pipeline already existed (`nexudus_invoice_pdf_cache` timer 03:30 → `cache_missing_nexudus_pdfs` → blob → `silver.nexudus_coworker_invoices.pdf_blob_path` → gold worklist), but `NexudusClient.get_invoice_pdf` hit `GET /api/billing/coworkerinvoice/{id}/pdf` — **an endpoint that does not exist**, so every fetch failed and invoices were getting stamped with the `__unavailable__` sentinel. **Nexudus has no direct PDF endpoint**; per Nexudus support the PDF is rendered via a two-step *run command*: (1) `POST /api/billing/coworkerInvoices/runCommand` `{"Ids":[id],"Key":"COWORKER_INVOICE_PRINT","Parameters":[]}` → 200 with a temporary `RedirectURL`; (2) `GET https://spaces.nexudus.com{RedirectURL}` (on the **bare host, NOT `/api`**; the temp file expires in a few minutes, so download immediately, reusing the session Bearer header). One Id → PDF; 2+ Ids → zip, so we always pass one Id. **Code** [shared/nexudus/client.py](shared/nexudus/client.py): rewrote `get_invoice_pdf(invoice_source_id) -> Optional[bytes]` (signature unchanged → `cache_missing_nexudus_pdfs` untouched) on top of two new tenacity-retried helpers `run_invoice_print_command(ids) -> RedirectURL|None` and `download_temp_file(redirect_url) -> bytes` (rebuilds the URL with the query re-passed as params so the spaced `downloadFileName` gets encoded). Guards: `WasSuccessful=false`/no RedirectURL/404 → None; `.zip` RedirectURL or body not starting with `%PDF` → None (never stored). **Removed the `__unavailable__` sentinel** from `cache_missing_nexudus_pdfs` ([shared/nexudus/invoice_pdf_cache.py](shared/nexudus/invoice_pdf_cache.py)) — it leaked the literal string into the worklist `pdf_blob_path`, and the 2-day lookback already bounds retries (now matches Xero PDF caching: a failed fetch just stays NULL). **Backfill** [scripts/python_scripts/backfill_nexudus_pdfs.py](scripts/python_scripts/backfill_nexudus_pdfs.py) (`--dry-run`/`--limit`/`--all`/`--reset-unavailable`): default targets the finance-worklist subset (open, non-void/draft, unpaid); `--reset-unavailable` clears the old endpoint's bogus sentinels for retry. **Smoke tests** `scripts/python_scripts/test_nexudus_pdf.py` (one PDF → disk) + `test_nexudus_pdf_cache.py` (full round-trip). **Unit tests** `tests/test_nexudus_pdf.py` (13 pass, faked aiohttp session — request shape, RedirectURL parse, `%20` URL build, re-mint loop, WasSuccessful/zip/non-PDF guards, 404 no-retry, server-error raise). No schema change (`pdf_blob_path`/`pdf_cached_at` already exist). **Adversarial multi-agent review (10 confirmed findings) hardened the first cut**: (1) download URL is now `%20`-encoded via `quote(query, safe="=&%")` — the original params-re-pass emitted form-style `+` for the spaced `downloadFileName` (uniqueId is the real key, so harmless, but `%20` is the correct RFC-3986 encoding); (2) `download_temp_file` lost its long exponential retry (pointless on a few-minute temp URL) — `get_invoice_pdf` re-mints a fresh URL up to 2× instead; (3) zip detection parses the `downloadFileName` value (param-order/name independent); (4) the nightly query now also covers open finance-worklist invoices (365-day due_date) so the dashboard's invoices get a PDF nightly without relying on the backfill; (5) the timer fails the run when every fetch failed with zero cached (so the health report can catch a future runCommand-contract regression — previously failures were folded into `rows_skipped` on a green run); (6) both finance SPs `NULLIF(pdf_blob_path,'__unavailable__')` as belt-and-suspenders. **Status: code UNCOMMITTED + not deployed** — push to `main` (GitHub Actions) to ship; then run `backfill_nexudus_pdfs.py --reset-unavailable` once to populate existing finance-dashboard invoices and clear the stale `__unavailable__` rows. The two finance SPs only get the NULLIF guard when re-applied (the backfill is the operative cleanup; SP edit is defense-in-depth, and note the deployed `sp_refresh_finance_dashboard` has known drift from the repo). "Try it out": run `tests.test_nexudus_pdf` (offline) then `test_nexudus_pdf.py` against the live API with Nexudus creds.)
Previous: 2026-06-24 (**Competitor flexible-workspace classification** — clean the scraped `silver.competence_competitors` down to real flex-space operators for the AI-teamandy outreach map. **Why the category can't filter**: the APIFY scrape only ever tags 3 Google categories (Coworking space 6158 / Office space rental agency 5523 / Business center 3638 = 15,319 rows) because those were the search seeds, so the junk (real-estate brokers/agents, virtual-office, event venues, noise) wears the same labels — the **website** is the only discriminator, and the `city` column is APIFY garbage (use lat/lng). **Two-tier classifier, deduped by domain** (the cost lever: 15,319 rows → **8,433 unique operator domains**, so a chain is one LLM call, not one-per-site): Tier-1 free rules ([shared/competence/classification.py](shared/competence/classification.py) — auto-drop clearly-unrelated categories via word-boundary regex, optional `trust_coworking`, all else undecided); Tier-2 Anthropic ([shared/competence/classifier_service.py](shared/competence/classifier_service.py) `CompetitorClassifier` — Haiku `claude-haiku-4-5-20251001`, batched metadata classify → **escalate to a homepage fetch only when unsure**, sliding-window RateLimiter, `max_ai_units` cap, MERGE upsert). Verdict stored per `place_id` in **new `silver.competence_competitor_classification`** (decoupled from the competitor MERGE so the nightly sync can't clobber it) + clean serving view **`silver.competence_flex_competitors`** (deduped by place_id, `is_flex=1`, `is_deleted=0`, exposes lat/lng, omits city). DDL [scripts/sql_scripts/competence_classification.sql](scripts/sql_scripts/competence_classification.sql) (table + view, idempotent, **applied to the warehouse 2026-06-24**). **Runs as a STEP of the daily competence sync** ([functions/competence_sync.py](functions/competence_sync.py), right after the silver write, same run/RunTracker — so `silver.competence_flex_competitors` stays clean for future competitors automatically); incremental (only not-yet-classified place_ids) + per-run AI cap. Controlled by `COMPETENCE_CLASSIFY` (default on; auto-skips when `ANTHROPIC_API_KEY` unset) + `COMPETENCE_CLASSIFY_MAX_AI_UNITS`; classifier knobs `COMPETENCE_CLASSIFIER_MODEL`/`_BATCH`/`_RPM`. (Briefly a standalone gated timer; folded into the sync.) **One-off backfill** [scripts/python_scripts/backfill_competitor_classification.py](scripts/python_scripts/backfill_competitor_classification.py) (`--dry-run`/`--rules-only`/`--sample-per-category`/`--max-ai`/`--trust-coworking`/`--no-fetch`/`--model`). Tests `tests/test_competence_classification.py` (16 pass: rules, domain-dedup, prompt/parse). **Validated on a 60-unit sample 2026-06-24** (4 AI calls): per-category keep/drop — Office space rental agency 12/8 (~40% junk: realtors, Opus Virtual Offices, noise correctly dropped; Regus/Deskeo/Boutique Workplace kept), Business center 11/8/1-unsure, Coworking space 16/4 (~20% junk) → **NOT trusting the coworking category** (verify all). **Status: schema applied + full backfill done 2026-06-24 (8,433 operators classified, ~57% kept); classification folded into the sync; code UNCOMMITTED + undeployed.** Run order: deploy (push to `main`) with `ENABLE_COMPETENCE_FUNCTIONS=1` + `ANTHROPIC_API_KEY` set (then every sync self-cleans new competitors; `COMPETENCE_CLASSIFY=0` disables). If Haiku isn't enabled on the account, set `COMPETENCE_CLASSIFIER_MODEL=claude-sonnet-4-20250514`. Add `("competence","competence_classification","silver")` to `sync_health_report._expected_daily()` when enabled. Feeds the AI-teamandy pricing-tool map by **lat/lng** (next step). No change to the existing competence sync.)
Previous: 2026-06-19 (**AVA location plans** — new `ava.location_plans` serving table: one row per (location, **plan**), where "Plans" in the Nexudus UI = **Tariffs** in the API (endpoint `GET /billing/tariffs` — confirmed; the tariff bronze+silver pipeline already existed from Phase 2 2026-05-28). It is a **filtered denormalized view of `silver.nexudus_tariffs`** — the **silver layer is deliberately left unchanged** (no new columns, no row filtering); all filtering happens in the ava SP. **Exclusion rules** (drop a plan if ANY hold): `is_deleted=1`; `price<=0` (the "0 euros services" rule); `SystemTariffType IN (1,3,5)` (1=private office, 3=dedicated desk, 5=hot desk — served/priced elsewhere); or `location_source_id` doesn't resolve to a silver location. Everything else kept (connectivity/bandwidth, parking, business-address registration, mailbox, rack space, service packages, part-time access, …) → current data **~88 plans / 8 locations**. **`SystemTariffType` is not on silver**, so `ava.sp_refresh_location_plans` reads it from `bronze.nexudus_tariffs.raw_json` via `JSON_VALUE` (bronze is UNIQUE on `source_id`, one row per tariff); `system_tariff_type_label`: 8→`part_time_access`, 9→`mailbox_storage`, else→`service`. Also denormalizes `financial_account_name`. **ETL**: [functions/ava_refresh.py](functions/ava_refresh.py) `refresh_ava_availability` now rebuilds `ava.location_plans` right after `ava.product_availability` (new `_refresh_location_plans()` with its own `meta.sync_runs` row `('ava','location_plans','ava')`); it **skips with a warning if the table/proc are absent** (deploy-before-apply safe) and a plans failure is logged but never fails the product-availability run or the desk-price audit. Schema+SP source: [scripts/sql_scripts/ava_location_plans_schema.sql](scripts/sql_scripts/ava_location_plans_schema.sql) (DELETE+INSERT rebuild, mirrors `sp_refresh_product_availability`; ends with `EXEC` + verification SELECTs). Tests: `tests/test_ava_refresh.py` +2 (`_location_plans_objects_exist`), full ava suite 10 pass. **Schema applied + populated to prod 2026-06-19** via `scripts/python_scripts/apply_schema_script.py scripts/sql_scripts/ava_location_plans_schema.sql` → **87 plans across 8 locations** (the 88th, a global `Part-time Access` plan on a "beyond"-level BusinessId with no physical location, drops on the location join). Read-only viewer: `scripts/python_scripts/show_ava_location_plans.py`. **Code (`ava_refresh.py`) not yet deployed** — push to `main` (GitHub Actions) so the nightly run keeps the table fresh (until then the table is correct but static). No bronze/silver/transformer change.)
Previous: 2026-06-11 (Location Scraper — **scrape cadence switched monthly → weekly (Monday 01:00 UTC)** + **two persist-layer bugs confirmed against prod and FIXED**. **(1) Weekly cadence**: [functions/location_scraper.py](functions/location_scraper.py) timer `location_scraper_monthly` renamed `location_scraper_weekly` (NCRONTAB default `0 0 1 * * 1`, env `LOCATION_SCRAPER_WEEKLY_SCHEDULE`; the old `LOCATION_SCRAPER_MONTHLY_SCHEDULE` app setting is now ignored), parent orchestrator renamed `location_scraper_weekly_orch`, period key = ISO week `%G-W%V` (e.g. `2026-W25`), instance_id `location-scraper-weekly-{week}`, run_id `weekly-{city}-{week}` (slugified). Idempotency/skip-completed/wave logic unchanged — `ls_cities_needing_run` now naturally scopes to the week because run_ids are week-keyed; the parent orchestrator accepts both `period_key` and legacy `month_key` input. `MONTHLY_CITIES` renamed `SCRAPE_CITIES` (18 cities, unchanged). Tests: 83 pass, 7 pre-existing berlin_simulation failures unrelated. **(2) Confirmed bugs in [shared/location_scraper/activities/persist.py](shared/location_scraper/activities/persist.py)** (diagnostic `scripts/python_scripts/confirm_persist_design_issues.py`): **(a) contacts are only linked on the new-building path** — `_upsert_scraper_contact`/`_upsert_lusha_contacts` are never called for existing buildings, so update-path snapshots have **0/2,381** contact links in prod and re-scrapes can never add a broker/Lusha email to a known building (coverage holes e.g. münchen 17/241, frankfurt 116/309, madrid 308/618 buildings with a contact, while latest London raw has brokerEmail on 396/398 items); **(b) Decimal-vs-float phantom updates** — `price_monthly`/`price_per_m2` are DECIMAL in SQL (pyodbc → `Decimal`) compared with `!=` to Python floats, so unchanged buildings re-insert snapshots: **1,542 of 3,120** update snapshots are byte-identical to their predecessor (e.g. monthly-frankfurt-2026-06: 166/172 phantom). **Fixes shipped in persist.py**: new `_dec2()` quantizes both sides to `Decimal('0.01')` before comparing; `_READ_EXISTING` now also returns `latest_listing_id` per building, and the existing-building branch links scraper + Lusha contacts to the freshly inserted snapshot when something changed, else to the building's latest listing (idempotent `_LINK_CONTACT`/`_MERGE_CONTACT`); after an update snapshot the in-memory `existing_map` row is refreshed so a same-key listing later in the run doesn't insert a duplicate. New `tests/test_location_scraper_persist.py` (5 tests; `mock_sql_empty` in the idealista e2e now also patches `shared.location_scraper.activities.persist.get_sql_client` because persist binds it at import time). Full suite 184 pass / 7 pre-existing berlin_simulation failures. **Self-healing after deploy**: LoopNet broker emails relink automatically on the next weekly run (email comes from the payload); **Lusha-source historical holes do NOT self-heal** because `filter_new_agencies` skips any agency with a `has_contact=1` row in `bronze.location_scraper_lusha_diagnostics` even though its contacts were dropped — so a **one-off skip-list purge was executed against prod 2026-06-11**: `scripts/python_scripts/purge_lusha_diagnostics_skiplist.py` (with `--dry-run`/`--city`) deleted all 387 `has_contact=1` rows; per `scripts/python_scripts/quantify_lusha_diagnostics_purge.py` (read-only sizing, latest-globe-run join) ~**214 agencies** will actually go back to Lusha on the next weekly run (the filter's building-coverage check re-skips fully covered ones; worst case ~1,070 reveals at `LUSHA_MAX_REVEALS_PER_AGENCY=5`) — mostly IS24/Germany (munich 37, hamburg 35, berlin 34…); madrid only recovers 7 because most of its holes are `has_contact=0` no-domain/no-contact misses that were never skip-listed and are retried every run anyway. `scripts/python_scripts/confirm_persist_design_issues.py` re-run post-deploy should show update-snapshot contact links > 0 and phantom growth stopped.)
Previous: 2026-06-10 (Coworkers sync switched to the **full list endpoint** — `_sync_coworkers` in [functions/bronze_nexudus.py](functions/bronze_nexudus.py) now pulls the paginated `GET /spaces/coworkers` list with the standard `UpdatedSince` watermark (+ `force_full` param for backfills) instead of per-ID fetches of only the CoworkerIds appearing on changed invoices. The old path silently dropped every coworker never invoiced (leads, team members billed through their company, event guests): SQL held 1,259 coworkers vs **29,844** in Nexudus. One-off backfill via new `scripts/python_scripts/backfill_nexudus_coworkers.py` (`--dry-run` compares live TotalItems vs SQL counts; `--write` runs the production `_sync_coworkers(force_full=True)` → bronze → silver) — needed because the pre-existing watermark would have made the first list-based nightly run incremental-only, never reaching old unchanged records. `_sync_coworker_invoices` no longer returns coworker_ids (lines fetch unchanged). The weekly `nexudus_silver_reconcile` already used `spaces/coworkers` for its ID set, so soft-delete semantics now line up exactly with the sync. No schema change; transformer untouched (list payloads carry the same fields; `business_ids` may be NULL where the list omits the `Businesses` array — passthrough column, no joins on it). NB deploy = push to `main` (GitHub Actions).)
Previous: 2026-06-10 (Location Scraper — **LoopNet full-coverage fix (London sourcing was missing ~90% of qualifying buildings)**. Root cause, all empirically verified (`scripts/python_scripts/test_loopnet_*.py`): the memo23 actor never loads the start URL — it geocodes the city slug and queries LoopNet's internal mobile API with a FIXED ~0.3° bounding box hard-capped at **500 items**, ignoring every filter (URL query params, `BuildingSizeRangeMin`, `moreResults`); neighbourhood-slug fan-out is useless (all London sub-areas geocode to nearly the same central box → same 500). The 500-window is dominated by small spaces, so only **42 of 383** London buildings with >=1500 m² AVAILABLE space surfaced. LoopNet's `?min-space-size=<sqft>` URL filter (binds `SpaceAvailableRangeMin` — available space, NOT total building size) is the only full view of the qualifying market (London: 383 buildings/16 pages; New York: 455 — same param works on loopnet.com). **Fix = 2-step flow in `location_scraper_orch`**: new activity `ls_enumerate_loopnet_urls` ([shared/location_scraper/activities/enumerate_loopnet.py](shared/location_scraper/activities/enumerate_loopnet.py)) fetches the filtered search pages via **abotapi/loopnet-scraper** (URL mode, `fetchDetails=false`; it passes Akamai — plain HTTP, apify/web-scraper and puppeteer-scraper are all challenge-blocked, puppeteer-scraper additionally needs a console permission approval; pagination MUST use the `?page=N` QUERY param because abotapi drops the page PATH segment — browser-verified `?page=N` ≡ `/N/`), dedupes by listing id, then `ls_start_apify_run` feeds those listing-detail URLs to memo23 as `startUrls` (memo23 accepts slug-less `/Listing/{id}/` URLs and returns its rich payload **incl. brokerEmail**) — adapter/persist/globe unchanged, >=1500 m² floor kept as guardrail, fallback to the legacy broad search if enumeration returns 0. Config: UK domain switched to **loopnet.co.uk** + `office-space` path (old `.com/office-properties` UK route 404s), `min_space_size_sqft=16146` (=1500 m²) on UK+US blocks ([config.py](shared/location_scraper/config.py) also gains `LOOPNET_ENUM_ACTOR_ID`, `MIN_SPACE_SIZE_SQFT`), filtered URL built in [resolve.py](shared/location_scraper/activities/resolve.py), `SourceConfig.listing_urls` (back-compat default None), `LoopnetAdapter.build_input` accepts a URL list, env `LOOPNET_ENUM_MAX_PAGES` (default 25). Tests: 39 pass (resolve URLs updated, +enumeration suite `tests/test_location_scraper_loopnet_enumeration.py`, +build_input list case); 7 berlin_simulation failures pre-existing/unrelated. Validated live: enumeration 383/383 London listing URLs; memo23 probes return brokerEmail on slugged AND slug-less listing URLs with adapter normalize OK. Expected after deploy (push to `main`): London 42 → ~383 buildings, US cities likewise uncapped (NY 102 → ~455).)
Previous: 2026-06-10 (Three new sources: **Nexudus events + HubSpot marketing emails + Eventbrite events**, all bronze + silver. **(1) Nexudus events** — 3 new entities through the standard pipeline: `nexudus_to_bronze` gained `_sync_calendar_events` / `_sync_event_attendees` / `_sync_event_products` (GET `/content/calendarevents|eventattendees|eventproducts`, `UpdatedSince` watermark + SHA-256 hash-dedup like the other paginated entities), 3 new `BronzeWriter.write_*` methods, 3 pure transformers ([shared/nexudus/transformers/calendar_events.py](shared/nexudus/transformers/calendar_events.py), `event_attendees.py`, `event_products.py` — field mappings verified against live payloads via `scripts/python_scripts/inspect_nexudus_events.py`), 3 silver writers, and the queue fanout (`silver_nexudus.ENTITIES`, `silver_worker._ENTITY_MAP`) is now **13 entities**. **Linking**: `calendar_events.location_source_id` = BusinessId → locations (+ optional `resource_source_id`); `event_attendees` links event/coworker (NULL for external guests)/ticket (`event_product_source_id`)/invoice (`coworker_invoice_source_id`); `event_products` (= ticket types, with allocation/sales) link their event, and — because the EventProduct payload carries **no BusinessId** — the silver writer resolves `location_source_id` from the latest bronze calendar events (`{event_id → location_id}` map; bronze always completes before the silver fanout). All 3 added to the weekly `nexudus_silver_reconcile` (soft-delete, `content/*` endpoints). DDL `scripts/sql_scripts/nexudus_events_schema.sql` **applied to prod + fully backfilled 2026-06-10** via `scripts/python_scripts/test_events_sync.py --write`: 729 events / 1,553 attendees / 205 products, 0 errors; 100% attendee+product→event joins, 205/205 products got a location, 721/729 events join silver locations. **(2) HubSpot marketing emails** — new gated surface (`ENABLE_HUBSPOT_FUNCTIONS=1` + `HUBSPOT_ACCESS_TOKEN` private-app token with `content` scope, **not yet created**): `functions/hubspot_sync.py` (timer 05:45 UTC) mirrors the self-contained `bamboohr_sync` pattern — full fetch of `GET /marketing/v3/emails?includeStats=true` (full by design: stats keep changing; bronze hash-dedup keeps it cheap) → `bronze.hubspot_marketing_emails` (string source_id, latest-payload MERGE) → `silver.hubspot_marketing_emails` (name/subject/state/type, campaign link, from/reply-to, `body_plain_text` + full `content_json` + `web_version_url`, 13 KPI counters `stat_*` + 8 ratios `open_rate` etc. **+ raw `stats_json`** so an upstream stats-key rename can never lose data; counters/ratios are read case-insensitively with fallbacks) → embedded daily soft-delete reconcile (floor `HUBSPOT_RECONCILE_MIN_IDS=5`). The `--dry-run` of `scripts/python_scripts/test_hubspot_sync.py` prints the live stats key inventory to verify the mapping once a token exists. **(3) Eventbrite events** — new gated surface (`ENABLE_EVENTBRITE_FUNCTIONS=1` + `EVENTBRITE_PRIVATE_TOKEN`, **not yet created** — copy the private token from eventbrite.com/platform/api-keys, no OAuth flow needed; optional `EVENTBRITE_ORGANIZATION_ID` pin): `functions/eventbrite_sync.py` (timer 05:50 UTC) resolves orgs via `/users/me/organizations/` then pulls **all** events per org (`status=all`, `expand=venue,ticket_availability,organizer,format,category`, continuation pagination) → `bronze.eventbrite_events` → `silver.eventbrite_events` (UTC+local schedule, status, capacity, flattened venue incl. lat/lng + `venue_json`, ticket price range/sold-out/waitlist + `ticket_availability_json`, organizer, logo) → embedded daily reconcile (floor `EVENTBRITE_RECONCILE_MIN_IDS=1`). Both new sources registered in `function_app.py` behind their flags (default off, like competence) and added to `sync_health_report._expected_daily()` **only when enabled**. HubSpot/Eventbrite schemas **applied to prod 2026-06-10** (tables exist, empty until tokens are set). New helper `scripts/python_scripts/apply_schema_script.py` (runs GO-batched .sql against the warehouse). Tests: `tests/test_nexudus_event_transformers.py` (+11), `tests/test_hubspot_transformer.py` (+9), `tests/test_eventbrite_transformer.py` (+8) — 28 new tests pass, full suite 91 pass (4 pre-existing local-venv import errors in location-scraper tests: `pytest`/`apify_client` not installed locally). MERGE placeholder parity machine-checked for all 5 new silver writers. No new pip deps. **To activate**: deploy = push to `main` (GitHub Actions) — Nexudus events then flow nightly with zero config; for HubSpot/Eventbrite set the token + `ENABLE_*_FUNCTIONS=1` app settings on `func-infinitspace-etl`.)
Previous: 2026-06-08 (Competence competitor **country enrichment** — `silver.competence_competitors` competitors carried no usable country (`last_seen_country_code` is mostly empty in Firestore), so a **cleanup step between bronze and silver** now fills both `country` (NAME, new column) and `country_code` from each competitor's **per-country parent list** (`NL_AUTO` → Netherlands / NL). New pure resolver `resolve_competitor_country()` in [shared/firebase/transformers/competence.py](shared/firebase/transformers/competence.py). **Real-data shape (confirmed against prod):** all 30 `competence_new` parent lists carry a free-text country NAME and **no** `country_code`, and their ids are random Firestore doc ids (not `NL_AUTO`) — so the country NAME is what fills the code. Code precedence = competitor's own `last_seen_country_code` → parent list `country_code` → **list country name mapped to ISO2** (name→ISO2 alias map, the path that actually fires) → ISO2 from a `XX_AUTO` list-id prefix. Name precedence = parent list `country` name (authoritative) → canonical ISO name; `UK`→`GB`, and names are canonicalised ("USA" + "United States" → "United States" / `US`). `transform_competitor` gained optional `list_country_name`/`list_country_code` params; [shared/azure_clients/silver_writer_competence.py](shared/azure_clients/silver_writer_competence.py) `_sync_competitors` builds a `{list_source_id → (country, country_code)}` map from `silver.competence_lists` (synced first in the same run, so complete) and passes each parent's country in, and the competitor MERGE/`_competitor_params` now write `country` (44 placeholders, parity-checked). **Schema**: new `country NVARCHAR(200) NULL` column + index on `silver.competence_competitors` — source-of-truth `scripts/sql_scripts/competence_schema.sql` updated + idempotent migration `scripts/sql_scripts/competence_competitor_country_migration.sql` for existing DBs. **One-off backfill**: `scripts/python_scripts/backfill_competence_competitor_country.py` (`--dry-run`/`--all`/`--limit`) reuses the same resolver so backfilled rows match the sync exactly; uses the existing silver `country_code` as the competitor's "own" code, skips already-correct rows (idempotent). Only watermark-changed rows are re-enriched by the nightly sync, hence the backfill for history. Tests: `tests/test_competence_transformer.py` +8 (18 pass) covering inherit-from-list, own-code-wins, list-name-when-codes-agree, prefix fallback, name→ISO2, USA canonicalisation, UK→GB, unresolvable→NULL; modules import clean. **Applied to prod 2026-06-08:** migration run (added `country` column + index) and backfill executed — all 15,319 active competitor rows now carry `country` + `country_code`, 0 unresolved (NL 3689, US 3354, GB 2592, DE 1999, FR 1829, CA 549, ES 534, PL 350, PT 220, FI 203). Still TODO: deploy code = push to `main` (GitHub Actions) so the nightly sync keeps new/changed rows enriched. No Firestore/index change; competence stays gated behind `ENABLE_COMPETENCE_FUNCTIONS`.)
Previous: 2026-06-08 (Firebase `competence_new` sync — **new daily Firestore → bronze → silver pipeline for TeamAndy competitor data, gated behind `ENABLE_COMPETENCE_FUNCTIONS` (default off) + `FIREBASE_CREDENTIALS`**. New `functions/competence_sync.py` (two timers: **incremental** Mon-Sat 04:30 UTC + **weekly full reconcile** Sun 04:00 UTC) mirrors the self-contained `bamboohr_sync` pattern: reads Firestore collection `competence_new` (per-country parent lists + their `competitors` subcollection, with a legacy in-doc `competitors` array fallback) via new `shared/firebase/client.py` (firebase-admin; credentials from the `FIREBASE_CREDENTIALS` env var parsed as service-account JSON, or `FIREBASE_SERVICE_ACCOUNT_KEY_FILE` (a local path or a Google Drive download URL, fetched in-memory) — the same connection logic the AI-teamandy services use) and `shared/firebase/competence.py` (reader; the competitor bronze key is the composite `{list_id}::{competitor_doc_id}` so the same placeId appearing under multiple lists stays distinct). Writes `bronze.competence_lists` + `bronze.competence_competitors` (string `source_id`, latest-payload MERGE with SHA-256 hash-dedup — `shared/azure_clients/competence_bronze_writer.py`), then `silver.competence_lists` + `silver.competence_competitors` (pure transformers in `shared/firebase/transformers/competence.py`; writer `shared/azure_clients/silver_writer_competence.py` reuses `load_latest_bronze_rows`), then (weekly only, in `competence_full_reconcile`) a full-read soft-delete reconcile (`is_deleted`/`deleted_at`, `COMPETENCE_RECONCILE_MIN_IDS` safety floor). The daily `competence_sync` is **incremental** — it reads only competitors whose `updated_at` advanced since the last run via a Firestore collection-group watermark (needs a one-time COLLECTION_GROUP index on `competitors.updated_at`; falls back to a full read while the index builds), so it never re-imports all 15k; bronze SHA-256 hash-dedup + the silver watermark mean only new/changed rows are written either way. Each step tracked in `meta.sync_runs` under source_name `competence`. SQL DDL: `scripts/sql_scripts/competence_schema.sql` (run once; NVARCHAR(450) string keys). New dep `firebase-admin>=6.5.0,<7` (resolved to 6.9.0). Validation: `scripts/python_scripts/test_competence_sync.py` (`--dry-run`/`--write`) + `tests/test_competence_transformer.py` (10 tests pass; module imports + blueprint registration verified). Gated separately (not in the default ETL deployment) so the daily timer only runs once creds exist. `sync_health_report` now includes competence in its expected-daily check (`("competence","competence","silver")`) **only when `ENABLE_COMPETENCE_FUNCTIONS=1`**, so a disabled feature is never flagged "never started"; the weekly `competence_reconcile` is deliberately excluded from the daily check. NB deploy = push to `main` (GitHub Actions), then set `FIREBASE_CREDENTIALS` + `ENABLE_COMPETENCE_FUNCTIONS=1` on `func-infinitspace-etl`.)
Previous: 2026-06-04 (Location Scraper — **streaming raw-dataset refactor deployed (root-cause OOM fix) + June 2026 monthly run completed 18/18 cities**. Commit `7a50c64` (deployed 06-04 08:42Z) stops routing the full Apify dataset through the Durable orchestrator. Old path: `ls_fetch_dataset` returned the whole payload list to the orchestrator (serialised into the orchestration history **and re-materialised in orchestrator memory on every replay** — the Lusha enrich fan-out triggers many replays), then passed it again as input to BOTH `ls_persist_raw` and `ls_normalize`; on 2 GB Flex Consumption instances this OOM-killed the worker (`python exited with code 137`) for large cities. **New path**: [shared/location_scraper/clients/apify.py](shared/location_scraper/clients/apify.py) `iterate_dataset()` (generator, never materialises the list) + [shared/location_scraper/activities/raw_payload.py](shared/location_scraper/activities/raw_payload.py) `fetch_and_persist_raw()` streams Apify → `bronze.location_scraper_raw` page-by-page in batches of 200 (peak memory ≈ one batch), and `read_raw_items()` pages the raw back from SQL via OFFSET/FETCH. New activity **`ls_fetch_and_persist_raw`** replaces the `ls_fetch_dataset`→`ls_persist_raw` pair (orchestrator step 4 returns only `{item_count, rows_inserted}`); **`ls_normalize`** is now called with `{actor, city, run_id}` (no `items`) and reads raw back from SQL — so the full dataset **never transits Durable**. Legacy `ls_fetch_dataset`/`ls_persist_raw` remain off the hot path (back-compat/tests). Idempotent per `run_id` (deletes raw rows first). **Validated at the unchanged 2048 MB** (`instanceMemoryMB` NOT bumped — RAM was never the real fix; `LOCATION_SCRAPER_MAX_ITEMS` capping avoided so full datasets are retained): the 8 stuck cities all re-ran green — berlin **1234 buildings**, munich 822, hamburg 753, frankfurt 559, dusseldorf 475, stuttgart 217, austin 129, new york 102. The new york / LoopNet `42S22 Invalid column name 'available_surface_m2'` failures were collateral of the pre-fix surface-migration deploy skew (commit `21cf1c4`, live since 06-03 12:10Z) and cleared on the clean re-run. **Operational cleanup this session**: purged a 3-week-old stuck Durable orphan (`location_scraper_orch`, berlin, `Running` since 2026-05-12) via the durable HTTP mgmt API (`/runtime/webhooks/durabletask/instances/{id}/terminate` then purge); cleared stale `error_message` on 6 `completed` rows (5 LoopNet + warsaw's stale `137`) — `bronze.n8n_location_scraper_logs` for `monthly-%-2026-06` is now **18 completed / 0 errors**. **Follow-up (not done)**: now that activities are memory-light, `host.json` `maxConcurrentActivityFunctions=2` is overly conservative and serialises the Lusha enrich fan-out — it can safely be raised (e.g. 8) to speed up future monthly runs. NB deploy = push to `main` (GitHub Actions `main_func-infinitspace-etl.yml`).)
Previous: 2026-06-03 (Location Scraper — **`host.json` activity concurrency lowered 10 → 2 to stop worker OOM (`python exited with code 137`)**. Even with sequential waves of 3, `maxConcurrentActivityFunctions=10` let up to ~6 memory-heavy activities (`ls_fetch_dataset`/`ls_persist_raw` load the full Apify payload set) run at once → the Python worker was SIGKILL'd for OOM (German IS24 cities + austin failed with a *NULL* `error_message`: a hard OOM kill dies before it can log). `maxConcurrentActivityFunctions` is now **2**, so at most 2 big datasets are in memory at any time, regardless of wave composition. `maxConcurrentOrchestratorFunctions` stays 5 (orchestrators are lightweight; memory lives in activities). Trade-off: the monthly batch runs more serially (fine — it is a once-a-month job). If a *single* huge city still OOMs alone, the next lever is capping its items (`LOCATION_SCRAPER_MAX_ITEMS`) or bumping the plan instance size. NB deploy = push to `main` (GitHub Actions).)
Previous: 2026-06-03 (Location Scraper — **monthly run now batched into sequential waves of 3 cities (fixes worker OOM / exit 137) + SQL-based retry of only failed cities** [+ follow-up fix: a re-trigger now terminates a hung parent instead of skipping it]. Problem: the monthly timer fanned out all 18 cities at once; with `host.json` `maxConcurrentActivityFunctions=10`, several memory-heavy Apify datasets (`ls_fetch_dataset`/`ls_persist_raw` load the full payload set — e.g. madrid 633 buildings) loaded simultaneously and the Python worker was SIGKILL'd for OOM (`python exited with code 137`). Solution in [functions/location_scraper.py](functions/location_scraper.py): the timer no longer starts 18 orchestrations directly — it starts **one parent orchestrator** `location_scraper_monthly_orch` (instance_id `location-scraper-monthly-{YYYY-MM}`, one per month). The parent processes cities in **sequential waves** of `_wave_size()` (default 3, env `LOCATION_SCRAPER_WAVE_SIZE`): each wave fans out its cities as **sub-orchestrations** (`call_sub_orchestrator("location_scraper_orch", …)`, no explicit child instance_id → Durable auto-assigns; the month-scoped `run_id` drives SQL/gold de-dup) and `task_all` waits before the next wave — so at most `wave_size` datasets are in memory at once. A wave failure is caught (try/except around `task_all`) and logged (`is_replaying`-guarded), so one failed city doesn't block later waves. **Retry semantics**: new activity `ls_cities_needing_run` queries `bronze.n8n_location_scraper_logs` (new helper `log_run.completed_run_ids`) and the parent **skips cities already `completed` this month**, so a re-trigger only retries failed/missing cities (no wasted Apify credits). New activity `ls_init_run_log` writes the per-city RUNNING row from inside the parent (orchestrators can't do I/O directly). **Parent-level lock** (replaces the per-city lock from the previous entry): a (re-)trigger always supersedes the current parent — a `running`/`pending`/`suspended` parent is **terminated** (`client.terminate`) then purged, a `completed`/`failed` parent is purged, and a fresh parent is started; SQL idempotency handles the rest, so a manual portal **Test/Run** always re-runs (retrying only failed cities). NB: the earlier "skip if running" variant deadlocked — a hung parent silently blocked every Test/Run (`Monthly location scraper parent already in progress; skipping`); terminating fixes that. Also: `log_run._UPDATE_LOG` and `_UPSERT_RUNNING_LOG` now clear `error_message` on success/re-run, so `completed` rows no longer carry a stale error (previously a city that failed then succeeded on retry showed `completed` with the old error text). No schema change (uses existing `error_message` column). Tests: 40 pass (resolve/loopnet/IS24/run_quality); module imports clean. NB still needs `func azure functionapp publish func-infinitspace-etl --python` — i.e. merge to `main` (GitHub Actions `main_func-infinitspace-etl.yml` deploys on push to `main`). Per-city `POST /api/scrape` path unchanged.)
Previous: 2026-06-03 (Location Scraper — **monthly re-run no longer blocked by a failed run (idempotency lock relaxed) + auto-purge of failed Durable instances**. Problem: the `location_scraper_monthly` timer skipped any city whose Durable instance already existed *in any state*, including `Failed`. So once a monthly run crashed (e.g. the surface_unit deploy skew below), re-triggering the timer skipped those cities forever — the only fix was a manual Durable purge. Solution in [functions/location_scraper.py](functions/location_scraper.py): the per-city skip now only triggers when the existing instance is in a **blocking** state (`running`/`pending`/`completed`/`continuedasnew`/`suspended` — i.e. in-progress or already-succeeded). A `Failed`/`Terminated`/`Canceled` instance is **non-blocking**: the timer logs it, calls `client.purge_instance_history(instance_id)` to clear the stale history, then re-runs the city under the same `instance_id`. `runtime_status` is compared on its trailing name, case-insensitively (`OrchestrationRuntimeStatus.Failed` → `failed`). Net effect: a failed city auto-retries on the next monthly fire **or** on a manual portal **Test/Run**, with no manual purge ever needed. Monthly idempotency for *successful*/*in-progress* runs is unchanged (still one run per city per month). No SQL/schema change. NB still needs `func azure functionapp publish func-infinitspace-etl --python` to deploy.)
Previous: 2026-06-03 (Location Scraper — **surface display unit (sqft for UK/US) + gold email-only filter**. Problem: every layer stored surface in m² only, but LoopNet (UK/US) listings are quoted in **square feet** — both outreach emails and the dashboard must show the local unit. Solution: keep a canonical m² *and* carry the display value + unit. **New columns across 3 layers** (`available_surface_m2` → renamed `surface_m2` in bronze; globe v2 / gold already used `surface_m2`): `surface_m2` (canonical m², used for sort/compare + the LoopNet ≥1500 m² guardrail), `surface_display` (value in display unit: native sqft for loopnet, = m² for idealista/otodom/IS24), `surface_unit` (`'sqft'`|`'m2'`). Code: [models.py](shared/location_scraper/models.py) (`Listing` fields + `from_dict` **backward-compat shim** mapping legacy `available_surface_m2` queue messages); [adapters/loopnet.py](shared/location_scraper/adapters/loopnet.py) new `available_surface_sqft_from_payload` (native sqft; `available_surface_m2_from_payload` now derives m² from it) → `surface_unit='sqft'`; the 3 EU adapters set `surface_display=surface_m2`, `surface_unit='m2'`; [activities/persist.py](shared/location_scraper/activities/persist.py) (`_INSERT_LISTING`); [activities/materialize_globe.py](shared/location_scraper/activities/materialize_globe.py) (`_map_row` computes display/unit, `_INSERT_ROW`). **Gold now keeps only buildings with a contact email** — `gold.sp_refresh_location_scraper_map_markers` gained `WHERE marker_rank = 1 AND lusha_email_1 IS NOT NULL` (the representative row is ranked to prefer rows with an email, so this drops markers where no listing has any email; `lusha_email_*` slots carry both Lusha emails and LoopNet broker emails). Gold also gains `total/min/max_surface_display` + `surface_unit`. **SQL**: schema source-of-truth updated in `location_scraper_schema.sql`, `location_scraper_globe_materialized_v2.sql`, `location_scraper_gold_map_markers_price_breakdown.sql`; new migration `scripts/sql_scripts/location_scraper_surface_unit_migration.sql` does the bronze rename + adds the silver columns + **backfills history** (loopnet rows: `unit='sqft'`, `display = surface_m2 / 0.092903`; others: `unit='m2'`, `display = surface_m2`). **Apply order**: (1) surface_unit_migration.sql, (2) globe_materialized_v2.sql, (3) gold_map_markers_price_breakdown.sql, (4) `EXEC gold.sp_refresh_location_scraper_map_markers`. Tests updated (loopnet/IS24/idealista/berlin) — 74 pass; the 7 berlin_simulation failures remain pre-existing (city/region fixture mismatch, unrelated). NB still needs `func azure functionapp publish func-infinitspace-etl --python`. Idealista/Otodom/IS24 normalization logic otherwise unchanged.)
Previous: 2026-06-02 (Location Scraper — **LoopNet US cities added (New York, San Francisco, Palo Alto, Los Angeles, Austin, Seattle)**. The existing LoopNet adapter + `memo23/loopnet-scraper-ppe` actor (`0ZCQONxB3BdyOzrbD`) is reused — no adapter/orchestrator changes. New `COUNTRY_CONFIG["us"]` block in [shared/location_scraper/config.py](shared/location_scraper/config.py): `country_code="us"`, `actor="loopnet"`, **`property_path="office-space"` + `filter_suffix="for-lease"`** (US LoopNet uses a different URL shape than UK's `office-properties`/`for-rent`; verified against the actor docs + live loopnet.com URLs). City slugs are `{city}-{state}`: `new-york-ny`, `san-francisco-ca`, `palo-alto-ca`, `los-angeles-ca`, `austin-tx`, `seattle-wa`. The shared `loopnet` branch in [shared/location_scraper/activities/resolve.py](shared/location_scraper/activities/resolve.py) already builds `…/search/{property_path}/{slug}/{filter_suffix}/`, so US resolves to e.g. `https://www.loopnet.com/search/office-space/new-york-ny/for-lease/`. Currency derives from country (non-GB → USD) in `currency_for_country`. **Multi-word city names contain a space** (`new york`) — kept as-is for resolve matching (config keys + `COUNTRY_NAME_BY_SOURCE["loopnet"]` use the spaced form → "United States" for geocode fallback), but the monthly timer in [functions/location_scraper.py](functions/location_scraper.py) now **slugifies** the city (`new york`→`new-york`) when building `run_id`/Durable `instance_id` (spaces are unsafe there). All 6 US cities added to `MONTHLY_CITIES`. Lusha still skipped for loopnet (broker email comes from payload). Tests: added US cases to `tests/test_location_scraper_resolve_source.py` (29 pass). NB still needs `func azure functionapp publish func-infinitspace-etl --python` to deploy; per-city runs can be triggered now via `POST /api/scrape {"City":"New York"}`. Idealista/Otodom/IS24/LoopNet-UK logic unchanged.)
Previous: 2026-06-02 (Location Scraper — **monthly scrape failure fix (apify-client version mismatch)**. The first-ever firing of the `location_scraper_monthly` timer (01:00 UTC on 2026-06-01, cron `0 0 1 1 * *`) started all 11 city Apify runs successfully but every Durable orchestration was marked `failed` ~2s later. Root cause recovered from the Durable task-hub table `funcinfinitspaceetlInstances`: `Activity 'ls_start_apify_run' failed: TypeError: 'Run' object is not subscriptable`. `requirements.txt` had `apify-client>=1.7.0` (unpinned); the prod build pulled a newer apify-client where `Actor.start()` / `run().get()` return a typed `Run` **object** instead of a subscriptable dict, so `run["id"]` in [shared/location_scraper/clients/apify.py](shared/location_scraper/clients/apify.py) threw. The Apify actors themselves ran fine — only the orchestrator's dict access broke, which is why the runs were lost despite Apify succeeding. **Fixes:** (1) pinned `apify-client==2.5.0` in `requirements.txt`; (2) added `_run_field()` helper in `apify.py` that reads run fields from either a dict or a `Run` object (used by `start_run`, `get_run_status`, `run_sync`) so a future version bump can't re-break it; (3) errors are now persisted to SQL — new `error_message NVARCHAR(MAX)` column on `bronze.n8n_location_scraper_logs` (migration `scripts/sql_scripts/location_scraper_logs_error_message.sql`, already applied to prod DB) written by `mark_run_failed` in [shared/location_scraper/activities/log_run.py](shared/location_scraper/activities/log_run.py) with a graceful fallback if the column is absent. Previously failures only went to App Insights, and there was no June-1 telemetry, which made diagnosis require the Durable history table. NB still needs `func azure functionapp publish func-infinitspace-etl --python` to deploy; the monthly run can be re-triggered per city via `POST /api/scrape`.)
Previous: 2026-06-02 (Location Scraper — **LoopNet (UK / London) added as a new source**. New adapter [shared/location_scraper/adapters/loopnet.py](shared/location_scraper/adapters/loopnet.py) (registered in `adapters/registry.py`), new `COUNTRY_CONFIG["uk"]` block in [shared/location_scraper/config.py](shared/location_scraper/config.py) with `LOOPNET_ACTOR_ID = "0ZCQONxB3BdyOzrbD"` (memo23 **pay-per-event** actor `memo23/loopnet-scraper-ppe`; the $31/mo flat-rate twin `RuOxoBM1bnc5pQ3TJ` is deliberately NOT used), and a `loopnet` URL branch in [shared/location_scraper/activities/resolve.py](shared/location_scraper/activities/resolve.py) producing `…/search/office-properties/london-england--united-kingdom/for-rent/`. **City slug must include region + country** (`london-england--united-kingdom`) — the actor geocodes the search area from the URL; a bare `london` slug fails. Key behaviours: (1) **areas are square feet → m²** (×0.092903) via `available_surface_m2_from_payload` (parses `header.subtext` "X SF … Available", falls back to summed `spaces[].size`); (2) **≥1500 m² hard floor** enforced both in the adapter (`normalize` returns None) AND in globe materialization (`_map_row` returns None, filtered in `materialize_globe_run`) — LoopNet's URL size filter is unreliable (filters total building size, not available area); (3) LoopNet payloads carry **no coordinates** → filled by the geocode fallback; (4) **broker name/company/phone/email come straight from the payload**, so `Listing.email` is populated and `matching_name()` uses `company_name` for loopnet. **Free geocoding**: added [shared/location_scraper/free_geocoding.py](shared/location_scraper/free_geocoding.py) (`NominatimGeocodingCache`, OpenStreetMap, no API key, 1 req/s, same `get_or_geocode` interface as `GeocodingCache`). `activities/scrape.py` + `activities/materialize_globe.py` now use Google Maps when `GOOGLE_MAPS_API_KEY` is set, else the free Nominatim geocoder (selector helper `get_geocoding_cache()` in `geocoding.py`; `COUNTRY_NAME_BY_SOURCE["loopnet"]["london"]="United Kingdom"`). `london` added to `MONTHLY_CITIES`. Tests: `tests/test_location_scraper_loopnet_adapter.py` (new) + LoopNet cases in `tests/test_location_scraper_resolve_source.py`; fixed `test_normalize_idealista_missing_coords` to stub the geocoder (stay offline). **Lusha skipped for LoopNet**: `LUSHA_SKIP_SOURCES = {"loopnet"}` in `functions/location_scraper.py` makes the orchestrator bypass dedupe/enrich/consolidate for LoopNet (broker email observed at 100% coverage on ≥1500 m² London listings). The broker contact is still persisted to `bronze.n8n_location_scraper_contacts` (`source='scraper'`) by `ls_upsert_sql`, and broker email(s) are surfaced in the globe email slots via new `_loopnet_broker_contacts` in `materialize_globe.py`. Optional env: `NOMINATIM_URL`, `NOMINATIM_USER_AGENT`. NB: 7 pre-existing failures in `tests/test_location_scraper_berlin_simulation.py` are unrelated (test fixture sets `region` as a neighbourhood while the IS24 adapter treats `region` as the city). Idealista/Otodom/IS24 logic unchanged.)
Previous: 2026-06-02 (Finance dashboard worklist reliability — [functions/finance_dashboard_refresh.py](functions/finance_dashboard_refresh.py): the nightly 05:30 `refresh_finance_dashboard` intermittently under-builds `gold.finance_dashboard_invoice_worklist` — on 2026-06-02 it published only 55 of ~180 rows, silently hiding ~125 genuinely-overdue invoices (incl. `INV-2026.04-7199`, Climb Online, £1,428, 41 days overdue, London-Aldgate). Root cause confirmed via App Insights: the run completed cleanly (7.3s, success, no error) — `invoice_worklist 210 -> 55` — and a manual re-`EXEC gold.sp_refresh_finance_dashboard` later the same day produced 180 with **no data change**, proving the invoices were always present in silver and the rebuild simply under-read them in the early-morning window (right after the 05:15 invoice reconcile). NB: the on-demand worklist refresh's silver re-sync is a **no-op** by design — the incremental watermark (last successful silver `started_at` = 02:30) means there is nothing newer than the 02:00 bronze snapshot to pull — so re-syncing does NOT fix this; only re-running the rebuild does. Two fixes shipped, **no schema change**: (1) **guardrail** — the rebuild + its verification counts now run in one transaction; if the new worklist collapses below `FINANCE_DASHBOARD_MIN_WORKLIST_RATIO` (default 0.5) of the currently-published count, the transaction is rolled back (last good list stays live) and the run is failed so the 06:00 health-report email flags it; skipped when the live count < `FINANCE_DASHBOARD_GUARDRAIL_MIN_BASELINE` (default 20). (2) **second guarded rebuild** `refresh_finance_dashboard_recheck` at 10:00 UTC (`FINANCE_DASHBOARD_RECHECK_SCHEDULE`) re-runs once the morning data has settled, republishing the full list. Separately noted for follow-up: the deployed `gold.sp_refresh_finance_dashboard` has drifted from the repo (still carries a `credit_note = 0` filter and a `has_recurrent_account = 1 OR due_date_local <= today+2` clause the on-demand `sp_refresh_invoice_worklist` dropped) — minor (~4 + future-one-off rows) and NOT the cause of the collapse; redeploy when convenient.)
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
