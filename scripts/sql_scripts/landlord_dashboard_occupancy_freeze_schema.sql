-- =============================================================================
-- landlord_dashboard_occupancy_freeze_schema.sql
--
-- Phase 3 (2026-05-28): Frozen past occupancy for the landlord dashboard.
--
-- Why this exists:
--   Past occupancy can change if we recompute it from contracts (contracts
--   get edited retroactively, late cancellations, etc.). For the dashboard
--   to show a stable historical line we freeze the past month's number on
--   the 1st of each new month and never recompute it.
--
-- Architecture:
--   silver.landlord_frozen_monthly_occupancy  ← the frozen table (this file)
--                                                  • backfilled from Daniel's
--                                                    sheet (Jun-25 → May-26)
--                                                  • appended by the cron in
--                                                    functions/landlord_freeze_monthly_occupancy.py
--   gold.vw_landlord_occupancy_combined        ← unioned view: frozen wins,
--                                                  falls back to membership_book
--                                                  for any month not yet frozen
--
-- Flask reads gold.vw_landlord_occupancy_combined for past + future and
-- doesn't need to know whether each row is frozen or live.
--
-- Backfill source: Daniel's manual workstation-count sheet, one number per
-- (location, month). Capacity is taken from current silver.nexudus_products
-- at read time (acceptable approximation; the user explicitly accepted this
-- when they said "we cannot do any operations on the past").
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC sp_executesql N'CREATE SCHEMA silver';
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC sp_executesql N'CREATE SCHEMA gold';
GO


-- =============================================================================
-- 1. silver.landlord_frozen_monthly_occupancy
-- =============================================================================

IF OBJECT_ID('silver.landlord_frozen_monthly_occupancy', 'U') IS NULL
BEGIN
    CREATE TABLE silver.landlord_frozen_monthly_occupancy (
        id                          BIGINT              IDENTITY(1,1) PRIMARY KEY,
        location_source_id          BIGINT              NOT NULL,
        period                      CHAR(7)             NOT NULL,           -- 'YYYY-MM'
        CONSTRAINT uq_landlord_frozen_occ UNIQUE (location_source_id, period),

        occupied_workstations       INT                 NOT NULL,

        -- Provenance — so we can tell Daniel's backfill rows from cron-frozen
        -- rows if any reconciliation is needed later.
        source                      NVARCHAR(32)        NOT NULL DEFAULT N'cron',
        --   'daniel_backfill'  → Jun-25..May-26 from Daniel's manual sheet
        --   'cron'             → monthly freeze job (default for new rows)
        --   'manual_override'  → emergency manual correction
        frozen_at                   DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        notes                       NVARCHAR(512)       NULL
    );

    CREATE INDEX ix_landlord_frozen_occ_location_period
        ON silver.landlord_frozen_monthly_occupancy (location_source_id, period);
END
GO


-- =============================================================================
-- 2. Backfill from Daniel's sheet (Jun-25 → May-26, 8 locations)
-- =============================================================================
-- Idempotent: only inserts rows that don't already exist. Re-running the
-- file is safe; the cron's later inserts are untouched.

;WITH daniel_data AS (
    SELECT short_name, period, occupied_ws
    FROM (VALUES
        -- Gouden Bocht (Amsterdam Center — Herengracht 471)
        (N'Gouden Bocht',           N'2025-06', 17),
        (N'Gouden Bocht',           N'2025-07', 17),
        (N'Gouden Bocht',           N'2025-08', 17),
        (N'Gouden Bocht',           N'2025-09', 18),
        (N'Gouden Bocht',           N'2025-10', 27),
        (N'Gouden Bocht',           N'2025-11', 36),
        (N'Gouden Bocht',           N'2025-12', 42),
        (N'Gouden Bocht',           N'2026-01', 45),
        (N'Gouden Bocht',           N'2026-02', 45),
        (N'Gouden Bocht',           N'2026-03', 49),
        (N'Gouden Bocht',           N'2026-04', 53),
        (N'Gouden Bocht',           N'2026-05', 57),

        -- Zuidtoren (Amsterdam Hoofddorp — Taurusavenue 3)
        (N'Zuidtoren',              N'2025-06', 87),
        (N'Zuidtoren',              N'2025-07', 107),
        (N'Zuidtoren',              N'2025-08', 107),
        (N'Zuidtoren',              N'2025-09', 107),
        (N'Zuidtoren',              N'2025-10', 127),
        (N'Zuidtoren',              N'2025-11', 127),
        (N'Zuidtoren',              N'2025-12', 127),
        (N'Zuidtoren',              N'2026-01', 127),
        (N'Zuidtoren',              N'2026-02', 167),
        (N'Zuidtoren',              N'2026-03', 200),
        (N'Zuidtoren',              N'2026-04', 239),
        (N'Zuidtoren',              N'2026-05', 239),

        -- Republica (Amsterdam Noord — Papaverhof 59)
        (N'Republica',              N'2025-06', 61),
        (N'Republica',              N'2025-07', 62),
        (N'Republica',              N'2025-08', 62),
        (N'Republica',              N'2025-09', 67),
        (N'Republica',              N'2025-10', 67),
        (N'Republica',              N'2025-11', 67),
        (N'Republica',              N'2025-12', 67),
        (N'Republica',              N'2026-01', 105),
        (N'Republica',              N'2026-02', 116),
        (N'Republica',              N'2026-03', 119),
        (N'Republica',              N'2026-04', 123),
        (N'Republica',              N'2026-05', 124),

        -- Chausseestrasse (Berlin Mitte — Chausseestrasse 29)
        (N'Chausseestrasse',        N'2025-06', 0),
        (N'Chausseestrasse',        N'2025-07', 0),
        (N'Chausseestrasse',        N'2025-08', 4),
        (N'Chausseestrasse',        N'2025-09', 6),
        (N'Chausseestrasse',        N'2025-10', 19),
        (N'Chausseestrasse',        N'2025-11', 40),
        (N'Chausseestrasse',        N'2025-12', 43),
        (N'Chausseestrasse',        N'2026-01', 71),
        (N'Chausseestrasse',        N'2026-02', 71),
        (N'Chausseestrasse',        N'2026-03', 71),
        (N'Chausseestrasse',        N'2026-04', 200),
        (N'Chausseestrasse',        N'2026-05', 202),

        -- Quartier Heidestrasse (Berlin Mitte — Heidestraße 34)
        (N'Quartier Heidestrasse',  N'2025-06', 42),
        (N'Quartier Heidestrasse',  N'2025-07', 67),
        (N'Quartier Heidestrasse',  N'2025-08', 85),
        (N'Quartier Heidestrasse',  N'2025-09', 112),
        (N'Quartier Heidestrasse',  N'2025-10', 112),
        (N'Quartier Heidestrasse',  N'2025-11', 123),
        (N'Quartier Heidestrasse',  N'2025-12', 123),
        (N'Quartier Heidestrasse',  N'2026-01', 160),
        (N'Quartier Heidestrasse',  N'2026-02', 165),
        (N'Quartier Heidestrasse',  N'2026-03', 195),
        (N'Quartier Heidestrasse',  N'2026-04', 197),
        (N'Quartier Heidestrasse',  N'2026-05', 210),

        -- Aldgate Tower (London Aldgate — 2 Leman Street)
        (N'Aldgate Tower',          N'2025-06', 913),
        (N'Aldgate Tower',          N'2025-07', 946),
        (N'Aldgate Tower',          N'2025-08', 983),
        (N'Aldgate Tower',          N'2025-09', 1003),
        (N'Aldgate Tower',          N'2025-10', 1036),
        (N'Aldgate Tower',          N'2025-11', 1036),
        (N'Aldgate Tower',          N'2025-12', 1037),
        (N'Aldgate Tower',          N'2026-01', 1041),
        (N'Aldgate Tower',          N'2026-02', 1089),
        (N'Aldgate Tower',          N'2026-03', 1095),
        (N'Aldgate Tower',          N'2026-04', 1117),
        (N'Aldgate Tower',          N'2026-05', 1126),

        -- Fox Court (London Holborn — 14 Grays Inn Rd)
        (N'Fox Court',              N'2025-06', 0),
        (N'Fox Court',              N'2025-07', 0),
        (N'Fox Court',              N'2025-08', 735),
        (N'Fox Court',              N'2025-09', 819),
        (N'Fox Court',              N'2025-10', 849),
        (N'Fox Court',              N'2025-11', 920),
        (N'Fox Court',              N'2025-12', 921),
        (N'Fox Court',              N'2026-01', 1075),
        (N'Fox Court',              N'2026-02', 1079),
        (N'Fox Court',              N'2026-03', 1123),
        (N'Fox Court',              N'2026-04', 1149),
        (N'Fox Court',              N'2026-05', 1368),

        -- The Bower (London Old Street — 207 Old Street)
        (N'The Bower',              N'2025-06', 186),
        (N'The Bower',              N'2025-07', 186),
        (N'The Bower',              N'2025-08', 186),
        (N'The Bower',              N'2025-09', 186),
        (N'The Bower',              N'2025-10', 186),
        (N'The Bower',              N'2025-11', 186),
        (N'The Bower',              N'2025-12', 186),
        (N'The Bower',              N'2026-01', 186),
        (N'The Bower',              N'2026-02', 196),
        (N'The Bower',              N'2026-03', 211),
        (N'The Bower',              N'2026-04', 247),
        (N'The Bower',              N'2026-05', 373)
    ) v(short_name, period, occupied_ws)
),
loc_map AS (
    -- Map Daniel's nicknames to Nexudus location_source_id by LIKE pattern.
    -- Done as a SELECT so the file is self-validating: if any nickname fails
    -- to resolve, the INSERT silently skips it instead of erroring — we report
    -- the count at the bottom for sanity-check.
    SELECT N'Gouden Bocht'          AS short_name, source_id AS location_source_id FROM silver.nexudus_locations WHERE name LIKE N'%Herengracht%'    AND is_deleted = 0
    UNION ALL
    SELECT N'Zuidtoren',             source_id FROM silver.nexudus_locations WHERE name LIKE N'%Taurusavenue%'  AND is_deleted = 0
    UNION ALL
    SELECT N'Republica',             source_id FROM silver.nexudus_locations WHERE name LIKE N'%Papaverhof%'    AND is_deleted = 0
    UNION ALL
    SELECT N'Chausseestrasse',       source_id FROM silver.nexudus_locations WHERE name LIKE N'%Chausseestrasse%' AND is_deleted = 0
    UNION ALL
    SELECT N'Quartier Heidestrasse', source_id FROM silver.nexudus_locations WHERE name LIKE N'%Heidestra%34%'  AND is_deleted = 0
    UNION ALL
    SELECT N'Aldgate Tower',         source_id FROM silver.nexudus_locations WHERE name LIKE N'%Aldgate%2 Leman%' AND is_deleted = 0
    UNION ALL
    SELECT N'Fox Court',             source_id FROM silver.nexudus_locations WHERE name LIKE N'%14 Grays Inn%'  AND is_deleted = 0
    UNION ALL
    SELECT N'The Bower',             source_id FROM silver.nexudus_locations WHERE name LIKE N'%207 Old Street%' AND is_deleted = 0
)
INSERT INTO silver.landlord_frozen_monthly_occupancy
    (location_source_id, period, occupied_workstations, source, notes)
SELECT
    lm.location_source_id,
    dd.period,
    dd.occupied_ws,
    N'daniel_backfill',
    N'Backfilled 2026-05-28 from Daniel''s manual workstation sheet'
FROM daniel_data dd
INNER JOIN loc_map lm ON lm.short_name = dd.short_name
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.landlord_frozen_monthly_occupancy f
    WHERE f.location_source_id = lm.location_source_id
      AND f.period = dd.period
);
GO


-- =============================================================================
-- 3. gold.vw_landlord_occupancy_combined
--
-- Single source for the dashboard's occupancy line. For each (location, period):
--   • If a frozen row exists → return it (Daniel's data or a past cron freeze)
--   • Otherwise              → fall back to gold.vw_landlord_membership_book_monthly
--                              (live, contract-based, with membership-fee filter)
--
-- Result: Flask reads ONE view and doesn't care which path the value came from.
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_occupancy_combined
AS
SELECT
    f.period,
    f.location_source_id,
    loc.name                            AS location_name,
    f.occupied_workstations,
    f.source                            AS data_source        -- 'daniel_backfill' / 'cron' / 'manual_override'
FROM silver.landlord_frozen_monthly_occupancy f
LEFT JOIN silver.nexudus_locations loc
    ON loc.source_id = f.location_source_id
UNION ALL
SELECT
    mb.period,
    mb.location_source_id,
    mb.location_name,
    mb.occupied_workstations,
    N'computed'                         AS data_source
FROM gold.vw_landlord_membership_book_monthly mb
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.landlord_frozen_monthly_occupancy f
    WHERE f.location_source_id = mb.location_source_id
      AND f.period             = mb.period
);
GO


-- =============================================================================
-- Verification
-- =============================================================================
-- Expect 96 backfilled rows (8 locations × 12 months) on first run, 0 on re-runs.
SELECT
    COUNT(*)                                   AS frozen_row_count,
    SUM(CASE WHEN source = 'daniel_backfill' THEN 1 ELSE 0 END) AS from_daniel,
    SUM(CASE WHEN source = 'cron' THEN 1 ELSE 0 END)            AS from_cron,
    MIN(period)                                AS earliest_period,
    MAX(period)                                AS latest_period
FROM silver.landlord_frozen_monthly_occupancy;

-- Cross-check that every Daniel nickname resolved to exactly one location.
-- Expect 8 rows. If any returns 0 or >1, the LIKE pattern needs adjustment.
SELECT n.short_name, COUNT(loc.source_id) AS resolved_locations
FROM (VALUES
    (N'Gouden Bocht'), (N'Zuidtoren'), (N'Republica'), (N'Chausseestrasse'),
    (N'Quartier Heidestrasse'), (N'Aldgate Tower'), (N'Fox Court'), (N'The Bower')
) n(short_name)
LEFT JOIN silver.nexudus_locations loc
    ON loc.is_deleted = 0
    AND (
        (n.short_name = N'Gouden Bocht'          AND loc.name LIKE N'%Herengracht%')
     OR (n.short_name = N'Zuidtoren'             AND loc.name LIKE N'%Taurusavenue%')
     OR (n.short_name = N'Republica'             AND loc.name LIKE N'%Papaverhof%')
     OR (n.short_name = N'Chausseestrasse'       AND loc.name LIKE N'%Chausseestrasse%')
     OR (n.short_name = N'Quartier Heidestrasse' AND loc.name LIKE N'%Heidestra%34%')
     OR (n.short_name = N'Aldgate Tower'         AND loc.name LIKE N'%Aldgate%2 Leman%')
     OR (n.short_name = N'Fox Court'             AND loc.name LIKE N'%14 Grays Inn%')
     OR (n.short_name = N'The Bower'             AND loc.name LIKE N'%207 Old Street%')
    )
GROUP BY n.short_name
ORDER BY n.short_name;


-- Phase 3 sanity
SELECT COUNT(*) AS frozen_rows FROM silver.landlord_frozen_monthly_occupancy;
-- expect 96

-- Past occupancy now reads from frozen view
SELECT period, location_source_id, occupied_workstations, data_source
FROM gold.vw_landlord_occupancy_combined
WHERE period BETWEEN '2025-06' AND '2026-05'
ORDER BY location_source_id, period;
-- expect data_source = 'daniel_backfill' for the 96 backfilled rows