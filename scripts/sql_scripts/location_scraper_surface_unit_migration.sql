-- Location Scraper — surface display unit migration.
--
-- Goal: carry the surface in BOTH the canonical m² (for sorting / cross-source
-- comparison / the LoopNet >=1500 m² guardrail) AND the unit a local broker
-- expects to see (square feet for UK/US LoopNet listings, m² everywhere else),
-- so the dashboard and outreach emails show the right unit without converting.
--
-- New columns (bronze + silver globe v2):
--   surface_m2       -- canonical m² (bronze: renamed from available_surface_m2)
--   surface_display  -- value in the display unit (sqft for loopnet, else m²)
--   surface_unit     -- 'sqft' | 'm2'
--
-- 1 square foot = 0.092903 m²  =>  sqft = m² / 0.092903
--
-- Apply order:
--   1. this script                                              (bronze + silver schema + history backfill)
--   2. location_scraper_globe_materialized_v2.sql               (idempotent; adds the same silver columns on fresh DBs)
--   3. location_scraper_gold_map_markers_price_breakdown.sql    (adds gold display columns + updated refresh proc)
--   4. EXEC gold.sp_refresh_location_scraper_map_markers;       (rebuilds gold from the now-backfilled silver)
--
-- Idempotent: safe to re-run.

-------------------------------------------------------------------------------
-- 1. bronze.n8n_location_scraper_listings
-------------------------------------------------------------------------------
IF OBJECT_ID(N'bronze.n8n_location_scraper_listings', N'U') IS NOT NULL
BEGIN
    -- Rename available_surface_m2 -> surface_m2 (preserves existing data).
    IF COL_LENGTH(N'bronze.n8n_location_scraper_listings', N'available_surface_m2') IS NOT NULL
       AND COL_LENGTH(N'bronze.n8n_location_scraper_listings', N'surface_m2') IS NULL
        EXEC sp_rename
            N'bronze.n8n_location_scraper_listings.available_surface_m2',
            N'surface_m2',
            N'COLUMN';

    IF COL_LENGTH(N'bronze.n8n_location_scraper_listings', N'surface_display') IS NULL
        ALTER TABLE bronze.n8n_location_scraper_listings ADD surface_display DECIMAL(12,2) NULL;
    IF COL_LENGTH(N'bronze.n8n_location_scraper_listings', N'surface_unit') IS NULL
        ALTER TABLE bronze.n8n_location_scraper_listings ADD surface_unit NVARCHAR(10) NULL;
END
GO

-- Backfill history: LoopNet (UK/US) listings display square feet, everything
-- else stays metric. Source lives on the building, joined via building_id.
UPDATE l
SET
    surface_unit = CASE WHEN b.source = 'loopnet' THEN 'sqft' ELSE 'm2' END,
    surface_display = CASE
        WHEN b.source = 'loopnet' THEN ROUND(l.surface_m2 / 0.092903, 2)
        ELSE l.surface_m2
    END
FROM bronze.n8n_location_scraper_listings AS l
JOIN bronze.n8n_location_scraper_buildings AS b
    ON b.id = l.building_id
WHERE l.surface_unit IS NULL;
GO

-------------------------------------------------------------------------------
-- 2. silver.location_scraper_globe_v2
-------------------------------------------------------------------------------
IF OBJECT_ID(N'silver.location_scraper_globe_v2', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'surface_display') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD surface_display DECIMAL(18,2) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'surface_unit') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD surface_unit NVARCHAR(10) NULL;
END
GO

-- Backfill history (silver carries `source` directly).
UPDATE silver.location_scraper_globe_v2
SET
    surface_unit = CASE WHEN source = 'loopnet' THEN 'sqft' ELSE 'm2' END,
    surface_display = CASE
        WHEN source = 'loopnet' THEN ROUND(surface_m2 / 0.092903, 2)
        ELSE surface_m2
    END
WHERE surface_unit IS NULL;
GO

-------------------------------------------------------------------------------
-- 3. gold.location_scraper_map_markers
--    Columns + refresh proc live in location_scraper_gold_map_markers_price_breakdown.sql.
--    After applying that script, rebuild gold from the backfilled silver:
--
--        EXEC gold.sp_refresh_location_scraper_map_markers;
--
--    (The updated proc also drops markers with no contact email.)
-------------------------------------------------------------------------------
