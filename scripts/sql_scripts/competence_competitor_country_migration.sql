-- =============================================================================
-- competence_competitor_country_migration.sql
--
-- Adds silver.competence_competitors.country (country NAME, e.g. "Netherlands").
--
-- Competitor docs carry almost no country of their own (last_seen_country_code
-- is mostly empty), so the nightly competence_sync now derives the country from
-- each competitor's per-country parent list (silver.competence_lists): both the
-- country NAME (new column) and the country_code (already existed) are filled
-- as a cleanup step between bronze and silver.
--
-- This script only adds the new `country` column + an index. country_code is
-- unchanged structurally. After running it, backfill the existing rows with:
--   scripts/python_scripts/backfill_competence_competitor_country.py
--
-- Idempotent: safe to run more than once.
-- =============================================================================

IF COL_LENGTH('silver.competence_competitors', 'country') IS NULL
BEGIN
    ALTER TABLE silver.competence_competitors
    ADD country NVARCHAR(200) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_competence_competitors_country_name'
      AND object_id = OBJECT_ID('silver.competence_competitors')
)
BEGIN
    CREATE INDEX ix_silver_competence_competitors_country_name
        ON silver.competence_competitors (country);
END
GO

-- Verify
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'competence_competitors'
  AND COLUMN_NAME IN ('country', 'country_code')
ORDER BY ORDINAL_POSITION;
