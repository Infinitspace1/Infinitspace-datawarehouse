-- =============================================================================
-- nexudus_coworkers_tariff_name_widen_migration.sql
--
-- Widens silver.nexudus_coworkers.tariff_name (and next_tariff_name) from
-- NVARCHAR(512) to NVARCHAR(MAX).
--
-- Why:
--   Since the 2026-06-10 switch to the full list endpoint GET /spaces/coworkers,
--   the coworker payload's TariffName / NextTariffName can carry an AGGREGATED
--   (comma-joined) value across all of a member's tariffs, not a single tariff
--   name. For members/companies with many tariffs this exceeds 512 chars, and
--   the transformer (shared/nexudus/transformers/coworkers.py) copies it through
--   with no length cap. A single oversized value fails the entire silver MERGE
--   batch with:
--     "String or binary data would be truncated ... column 'tariff_name'"
--   taking down the whole coworkers silver run (0 read / 0 written).
--
--   The sibling column coworker_contract_tariff_names is already NVARCHAR(MAX)
--   for exactly this aggregation reason; tariff_name / next_tariff_name were
--   sized for a single tariff and never widened. next_tariff_name has the
--   identical exposure, so it is widened too to prevent the same failure
--   recurring on the sibling column.
--
-- Neither column is indexed, so ALTER to NVARCHAR(MAX) is safe.
--
-- Idempotent: the ALTER only runs when the column is not already MAX
-- (CHARACTER_MAXIMUM_LENGTH = -1 denotes NVARCHAR(MAX)).
-- =============================================================================

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'nexudus_coworkers'
      AND COLUMN_NAME = 'tariff_name' AND CHARACTER_MAXIMUM_LENGTH <> -1
)
BEGIN
    ALTER TABLE silver.nexudus_coworkers ALTER COLUMN tariff_name NVARCHAR(MAX) NULL;
END
GO

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'nexudus_coworkers'
      AND COLUMN_NAME = 'next_tariff_name' AND CHARACTER_MAXIMUM_LENGTH <> -1
)
BEGIN
    ALTER TABLE silver.nexudus_coworkers ALTER COLUMN next_tariff_name NVARCHAR(MAX) NULL;
END
GO

-- Verify (both should report CHARACTER_MAXIMUM_LENGTH = -1)
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'nexudus_coworkers'
  AND COLUMN_NAME IN ('tariff_name', 'next_tariff_name')
ORDER BY COLUMN_NAME;
