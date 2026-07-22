-- =============================================================================
-- nexudus_coworkers_hubspot_deal_url_migration.sql
--
-- Adds silver.nexudus_coworkers.hubspot_deal_url (+ index).
--
-- Why:
--   The sales dashboard's Direct Enquiries report links each Nexudus contract
--   to its HubSpot deal 1-to-1 via a HubSpot deal URL stored in an INTERNAL
--   Nexudus custom field on the customer's company. That field surfaces on the
--   coworker API payload inside the CustomFields.Data array (a {Name, Value}
--   entry keyed by the field id, e.g. Name = '1414370977') and lands in
--   bronze.nexudus_coworkers.raw_json. Reading it live from raw_json means a
--   full JSON scan of ~30k rows (~3 min) — far too slow for a page load.
--
--   This flattens it into an indexed silver column. The transformer
--   (shared/nexudus/transformers/coworkers.py :: _extract_hubspot_deal_url)
--   populates it on every sync going forward; the dashboard then reads the
--   indexed column instantly.
--
-- Idempotent: the ADD only runs when the column is absent; the index create is
-- guarded by sys.indexes. Safe to re-run.
--
-- Apply with:
--   python scripts/python_scripts/apply_schema_script.py \
--       scripts/sql_scripts/nexudus_coworkers_hubspot_deal_url_migration.sql
--
-- Backfill note:
--   The field is brand new, so no existing coworker carries a value yet — the
--   normal nightly sync picks up each company as its payload changes (a colleague
--   pasting the URL IS a change), so no backfill is required for rollout. If you
--   ever need to retro-populate rows whose payload won't change again, reset the
--   silver-coworkers watermark so the next run re-maps every coworker:
--     DELETE FROM meta.sync_runs
--     WHERE source_name='nexudus' AND entity='coworkers' AND layer='silver'
--       AND status='success';
--   then let the nightly silver run (or trigger it) re-MERGE all coworkers.
-- =============================================================================

IF COL_LENGTH('silver.nexudus_coworkers', 'hubspot_deal_url') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworkers ADD hubspot_deal_url NVARCHAR(512) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworkers_hubspot_deal_url'
      AND object_id = OBJECT_ID('silver.nexudus_coworkers')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworkers_hubspot_deal_url
        ON silver.nexudus_coworkers (hubspot_deal_url);
END
GO

-- Verify
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'nexudus_coworkers'
  AND COLUMN_NAME = 'hubspot_deal_url';
