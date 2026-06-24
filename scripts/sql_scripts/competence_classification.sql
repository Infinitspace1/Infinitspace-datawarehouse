-- =============================================================================
-- competence_classification.sql
--
-- Coworking / flexible-workspace classification for the scraped competitor list.
-- The APIFY scrape lands many non-flex businesses in silver.competence_competitors;
-- this adds a verdict per physical location (Google place_id) so the map + outreach
-- consume only real flexible-workspace operators.
--
-- Verdict is kept in its OWN table keyed by place_id (one row per physical site, so
-- chains that repeat per list collapse to a single verdict) rather than a column on
-- silver.competence_competitors. That decouples it from the nightly competitor MERGE
-- (which would otherwise overwrite an AI verdict) and keeps classification incremental.
--
-- Filled by the two-tier classifier (free category rules + an LLM pass on the ambiguous
-- middle): functions/competence_classification.py for the nightly run, and
-- scripts/python_scripts/backfill_competitor_classification.py for the one-off backfill.
--
-- Idempotent: safe to run more than once.
-- =============================================================================

IF OBJECT_ID('silver.competence_competitor_classification', 'U') IS NULL
BEGIN
    CREATE TABLE silver.competence_competitor_classification (
        place_id       NVARCHAR(450)  NOT NULL,            -- Google Maps placeId (one verdict per physical site)
        is_flex        BIT            NULL,                -- 1 = flexible-workspace operator, 0 = not, NULL = unresolved
        confidence     DECIMAL(4,3)   NULL,                -- 0..1
        method         NVARCHAR(40)   NULL,                -- rule:category | rule:title | ai:meta | ai:web
        model          NVARCHAR(80)   NULL,                -- LLM model id when method = ai:*
        category_name  NVARCHAR(200)  NULL,                -- the category we classified from (audit / drift)
        input_hash     CHAR(64)       NULL,                -- SHA-256 of (title, category, domain); re-run only on change
        reasoning      NVARCHAR(500)  NULL,                -- short note (rule matched / model rationale)
        classified_at  DATETIME2      NOT NULL
            CONSTRAINT df_silver_competence_classification_at DEFAULT GETUTCDATE(),
        CONSTRAINT pk_silver_competence_competitor_classification PRIMARY KEY (place_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_competence_classification_is_flex'
      AND object_id = OBJECT_ID('silver.competence_competitor_classification')
)
BEGIN
    CREATE INDEX ix_silver_competence_classification_is_flex
        ON silver.competence_competitor_classification (is_flex);
END
GO

-- Clean serving view: real flexible-workspace operators only, one row per physical site.
-- Dedupes the competitor table on place_id (a place can appear under several country
-- lists), honours the soft-delete contract, and exposes lat/lng (the reliable location
-- signal — `city` from the scrape is unreliable, so it is intentionally NOT surfaced here).
CREATE OR ALTER VIEW silver.competence_flex_competitors AS
WITH ranked AS (
    SELECT
        c.place_id, c.source_id, c.list_source_id, c.title, c.category_name,
        c.address, c.street, c.postal_code, c.country, c.country_code,
        c.phone, c.website, c.google_maps_url, c.latitude, c.longitude,
        c.last_seen_at, c.last_synced_at,
        ROW_NUMBER() OVER (
            PARTITION BY c.place_id
            ORDER BY c.last_synced_at DESC, c.source_id
        ) AS rn
    FROM silver.competence_competitors c
    WHERE c.is_deleted = 0 AND c.place_id IS NOT NULL
)
SELECT
    r.place_id, r.title, r.category_name, r.address, r.street, r.postal_code,
    r.country, r.country_code, r.phone, r.website, r.google_maps_url,
    r.latitude, r.longitude, r.last_seen_at,
    k.confidence AS flex_confidence, k.method AS flex_method, k.classified_at AS flex_classified_at
FROM ranked r
JOIN silver.competence_competitor_classification k ON k.place_id = r.place_id
WHERE r.rn = 1 AND k.is_flex = 1;
GO

-- Verify
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'competence_competitor_classification'
ORDER BY ORDINAL_POSITION;



SELECT category_name, COUNT(*) AS n
FROM silver.competence_competitors
WHERE is_deleted = 0
GROUP BY category_name
ORDER BY n DESC;