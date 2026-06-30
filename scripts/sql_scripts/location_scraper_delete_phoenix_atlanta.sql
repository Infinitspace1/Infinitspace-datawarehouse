-- ---------------------------------------------------------------------------
-- One-off: delete ALL Location-Scraper data for Phoenix and Atlanta (US / LoopNet)
-- ---------------------------------------------------------------------------
-- These two US cities were removed from the weekly scrape config. This wipes
-- their historical rows from every Location-Scraper table in the warehouse DB.
--
-- Run against the data-warehouse DB (AZURE_SQL_DATABASE). It is wrapped in a
-- transaction: inspect the @@ROWCOUNT messages, then COMMIT (or ROLLBACK).
-- Keyed off both the scrape-city columns AND the run_ids that belong to those
-- cities, so it catches the weekly `weekly-<city>-<week>` runs and any
-- on-demand runs (which only record the city in the log row).
-- ---------------------------------------------------------------------------
SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRANSACTION;

DECLARE @cities TABLE (city NVARCHAR(100) PRIMARY KEY);
INSERT INTO @cities (city) VALUES ('phoenix'), ('atlanta');

-- 1. Every run_id belonging to these cities, gathered from all run-scoped
--    tables (each writes the scrape city), plus the deterministic weekly key.
DECLARE @run_ids TABLE (run_id NVARCHAR(100) PRIMARY KEY);
INSERT INTO @run_ids (run_id)
SELECT DISTINCT run_id FROM (
    SELECT run_id FROM bronze.n8n_location_scraper_logs        WHERE city     IN (SELECT city FROM @cities)
    UNION SELECT run_id FROM bronze.location_scraper_raw          WHERE city     IN (SELECT city FROM @cities)
    UNION SELECT run_id FROM bronze.location_scraper_run_quality  WHERE city     IN (SELECT city FROM @cities)
    UNION SELECT run_id FROM bronze.location_scraper_lusha_diagnostics WHERE city IN (SELECT city FROM @cities)
    UNION SELECT run_id FROM silver.location_scraper_globe_v2     WHERE run_city IN (SELECT city FROM @cities)
    UNION SELECT run_id FROM bronze.n8n_location_scraper_logs
        WHERE run_id LIKE 'weekly-phoenix-%' OR run_id LIKE 'weekly-atlanta-%'
) x;

-- 2. The listings (and their buildings + contacts) for those runs. Listings
--    only carry run_id, so this is the only reliable link to the buildings,
--    whose `city` column may hold a suburb name returned by LoopNet.
DECLARE @listing_ids  TABLE (id UNIQUEIDENTIFIER PRIMARY KEY);
DECLARE @building_ids TABLE (id UNIQUEIDENTIFIER PRIMARY KEY);
DECLARE @contact_ids  TABLE (id UNIQUEIDENTIFIER PRIMARY KEY);

INSERT INTO @listing_ids (id)
SELECT id FROM bronze.n8n_location_scraper_listings
WHERE run_id IN (SELECT run_id FROM @run_ids);

INSERT INTO @building_ids (id)
SELECT DISTINCT building_id FROM bronze.n8n_location_scraper_listings
WHERE run_id IN (SELECT run_id FROM @run_ids);

-- Also any building tagged directly with the scrape city (defensive).
INSERT INTO @building_ids (id)
SELECT id FROM bronze.n8n_location_scraper_buildings
WHERE city IN (SELECT city FROM @cities)
  AND id NOT IN (SELECT id FROM @building_ids);

-- Contacts linked to those listings (captured before the junction is cleared).
INSERT INTO @contact_ids (id)
SELECT DISTINCT contact_id FROM bronze.n8n_location_scraper_listing_contacts
WHERE listing_id IN (SELECT id FROM @listing_ids);

-- 3. Delete bottom-up to respect FKs.
DELETE FROM bronze.n8n_location_scraper_listing_contacts
WHERE listing_id IN (SELECT id FROM @listing_ids);
PRINT CONCAT('listing_contacts deleted: ', @@ROWCOUNT);

DELETE FROM bronze.n8n_location_scraper_listings
WHERE id IN (SELECT id FROM @listing_ids);
PRINT CONCAT('listings deleted: ', @@ROWCOUNT);

-- Buildings only when no listing references them anymore (keeps any building
-- still shared by another city's listings).
DELETE b
FROM bronze.n8n_location_scraper_buildings b
WHERE b.id IN (SELECT id FROM @building_ids)
  AND NOT EXISTS (
      SELECT 1 FROM bronze.n8n_location_scraper_listings l WHERE l.building_id = b.id
  );
PRINT CONCAT('buildings deleted: ', @@ROWCOUNT);

-- Contacts orphaned by the above (a broker email is UNIQUE and may be shared
-- across cities, so only drop ones no longer linked to any listing).
DELETE c
FROM bronze.n8n_location_scraper_contacts c
WHERE c.id IN (SELECT id FROM @contact_ids)
  AND NOT EXISTS (
      SELECT 1 FROM bronze.n8n_location_scraper_listing_contacts lc WHERE lc.contact_id = c.id
  );
PRINT CONCAT('contacts deleted: ', @@ROWCOUNT);

-- 4. Run-scoped bronze tables (by city, with run_id as a safety net).
DELETE FROM bronze.location_scraper_raw
WHERE city IN (SELECT city FROM @cities) OR run_id IN (SELECT run_id FROM @run_ids);
PRINT CONCAT('location_scraper_raw deleted: ', @@ROWCOUNT);

DELETE FROM bronze.location_scraper_run_quality
WHERE city IN (SELECT city FROM @cities) OR run_id IN (SELECT run_id FROM @run_ids);
PRINT CONCAT('location_scraper_run_quality deleted: ', @@ROWCOUNT);

DELETE FROM bronze.location_scraper_lusha_diagnostics
WHERE city IN (SELECT city FROM @cities) OR run_id IN (SELECT run_id FROM @run_ids);
PRINT CONCAT('location_scraper_lusha_diagnostics deleted: ', @@ROWCOUNT);

-- 5. Silver materialized globe + its quality table.
DELETE FROM silver.location_scraper_globe_v2
WHERE run_city IN (SELECT city FROM @cities) OR run_id IN (SELECT run_id FROM @run_ids);
PRINT CONCAT('location_scraper_globe_v2 deleted: ', @@ROWCOUNT);

DELETE FROM silver.location_scraper_globe_quality
WHERE run_city IN (SELECT city FROM @cities) OR run_id IN (SELECT run_id FROM @run_ids);
PRINT CONCAT('location_scraper_globe_quality deleted: ', @@ROWCOUNT);

-- 6. Run log rows (last — @run_ids is already materialized above).
DELETE FROM bronze.n8n_location_scraper_logs
WHERE city IN (SELECT city FROM @cities) OR run_id IN (SELECT run_id FROM @run_ids);
PRINT CONCAT('n8n_location_scraper_logs deleted: ', @@ROWCOUNT);

-- Inspect the PRINT counts above, then COMMIT (or ROLLBACK to undo).
COMMIT TRANSACTION;
-- ROLLBACK TRANSACTION;

-- 7. Rebuild the gold map markers (full DELETE+INSERT from silver globe, so
--    Phoenix/Atlanta drop out automatically). Run after the COMMIT.
EXEC gold.sp_refresh_location_scraper_map_markers;
