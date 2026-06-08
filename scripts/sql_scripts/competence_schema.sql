-- scripts/sql_scripts/competence_schema.sql
-- Firebase competence_new (TeamAndy lead-gen) bronze + silver tables.
--
-- Source: Firestore collection `competence_new` (project wearebeyond-bd776).
--   - Each parent doc is a per-country competitor list ({ISO2}_AUTO, e.g. NL_AUTO).
--   - Competitor records live in a `competitors` subcollection (schema v2),
--     with a legacy fallback to an in-doc `competitors` array (schema v1).
--
-- Synced daily by functions/competence_sync.py (source_name = 'competence').
-- Run this script once against the warehouse DB to create the tables.
-- The bronze/silver schemas already exist (created by earlier layer scripts).
--
-- Design notes:
--   - source_id is the Firestore document id (a string), so it is NVARCHAR,
--     not BIGINT like the Nexudus bronze tables. NVARCHAR(450) keeps it within
--     SQL Server's 900-byte index-key limit while comfortably fitting real
--     Firestore ids (sanitized Google placeIds / k_<sha1> fallbacks).
--   - Bronze is latest-payload-wins (UNIQUE source_id, overwrite MERGE), like
--     bronze.bamboohr_employees. payload_hash drives change detection so
--     synced_at only advances when a record actually changes.
--   - Silver carries is_deleted/deleted_at maintained by the reconcile step in
--     competence_sync.py. Downstream reads MUST filter WHERE is_deleted = 0.

-- ── Bronze ──────────────────────────────────────────────────────────────────

CREATE TABLE bronze.competence_lists (
    id            BIGINT           IDENTITY(1,1) NOT NULL,
    sync_run_id   UNIQUEIDENTIFIER NOT NULL,
    source_id     NVARCHAR(450)    NOT NULL,       -- Firestore doc id (e.g. NL_AUTO)
    country_code  NVARCHAR(8)      NULL,           -- denorm for filtering
    raw_json      NVARCHAR(MAX)    NOT NULL,
    payload_hash  CHAR(64)         NULL,           -- SHA-256 of raw_json for change detection
    synced_at     DATETIME2        NOT NULL CONSTRAINT df_bronze_competence_lists_synced_at DEFAULT GETUTCDATE(),
    CONSTRAINT pk_bronze_competence_lists PRIMARY KEY (id),
    CONSTRAINT uq_bronze_competence_lists_source_id UNIQUE (source_id)
);

CREATE INDEX ix_bronze_competence_lists_sync_run  ON bronze.competence_lists (sync_run_id);
CREATE INDEX ix_bronze_competence_lists_synced_at ON bronze.competence_lists (synced_at);
GO

CREATE TABLE bronze.competence_competitors (
    id              BIGINT           IDENTITY(1,1) NOT NULL,
    sync_run_id     UNIQUEIDENTIFIER NOT NULL,
    source_id       NVARCHAR(450)    NOT NULL,     -- competitor doc id (sanitized placeId or k_<sha1>)
    list_source_id  NVARCHAR(450)    NULL,         -- parent competence_new doc id (denorm)
    place_id        NVARCHAR(450)    NULL,         -- Google Maps placeId (denorm for joins)
    raw_json        NVARCHAR(MAX)    NOT NULL,
    payload_hash    CHAR(64)         NULL,         -- SHA-256 of raw_json for change detection
    synced_at       DATETIME2        NOT NULL CONSTRAINT df_bronze_competence_competitors_synced_at DEFAULT GETUTCDATE(),
    CONSTRAINT pk_bronze_competence_competitors PRIMARY KEY (id),
    CONSTRAINT uq_bronze_competence_competitors_source_id UNIQUE (source_id)
);

CREATE INDEX ix_bronze_competence_competitors_list      ON bronze.competence_competitors (list_source_id);
CREATE INDEX ix_bronze_competence_competitors_sync_run  ON bronze.competence_competitors (sync_run_id);
CREATE INDEX ix_bronze_competence_competitors_synced_at ON bronze.competence_competitors (synced_at);
GO

-- ── Silver ──────────────────────────────────────────────────────────────────

CREATE TABLE silver.competence_lists (
    source_id              NVARCHAR(450)    NOT NULL,   -- Firestore doc id
    uid                    NVARCHAR(450)    NULL,
    competitor_list_name   NVARCHAR(400)    NULL,
    country                NVARCHAR(200)    NULL,
    country_code           NVARCHAR(8)      NULL,
    auto_managed           BIT              NULL,
    status                 NVARCHAR(50)     NULL,       -- running / completed / completed_with_errors / failed
    competitor_count       INT              NULL,       -- cached count on the parent doc
    schema_version         INT              NULL,       -- 1 = in-doc array, 2 = subcollection
    last_error             NVARCHAR(MAX)    NULL,
    created_at             DATETIME2        NULL,
    updated_at             DATETIME2        NULL,
    last_run_at            DATETIME2        NULL,
    bronze_id              BIGINT           NULL,
    sync_run_id            UNIQUEIDENTIFIER NULL,
    is_deleted             BIT              NOT NULL CONSTRAINT df_silver_competence_lists_is_deleted DEFAULT 0,
    deleted_at             DATETIME2        NULL,
    last_synced_at         DATETIME2        NOT NULL CONSTRAINT df_silver_competence_lists_synced_at DEFAULT GETUTCDATE(),
    CONSTRAINT pk_silver_competence_lists PRIMARY KEY (source_id)
);

CREATE INDEX ix_silver_competence_lists_country ON silver.competence_lists (country_code);
GO

CREATE TABLE silver.competence_competitors (
    source_id          NVARCHAR(450)    NOT NULL,   -- competitor doc id
    list_source_id     NVARCHAR(450)    NULL,       -- -> silver.competence_lists.source_id
    place_id           NVARCHAR(450)    NULL,       -- Google Maps placeId
    title              NVARCHAR(500)    NULL,       -- business name
    category_name      NVARCHAR(200)    NULL,       -- coworking space / business center / ...
    address            NVARCHAR(1000)   NULL,
    street             NVARCHAR(500)    NULL,
    city               NVARCHAR(200)    NULL,
    postal_code        NVARCHAR(50)     NULL,
    country            NVARCHAR(200)    NULL,       -- country NAME, derived from the parent list (NL_AUTO -> Netherlands)
    country_code       NVARCHAR(8)      NULL,       -- own last_seen_country_code, else inherited from the parent list
    phone              NVARCHAR(100)    NULL,
    website            NVARCHAR(1000)   NULL,
    google_maps_url    NVARCHAR(1000)   NULL,
    latitude           FLOAT            NULL,
    longitude          FLOAT            NULL,
    last_seen_at       DATETIME2        NULL,       -- last scrape that saw this competitor
    last_seen_in_city  NVARCHAR(200)    NULL,
    created_at         DATETIME2        NULL,
    updated_at         DATETIME2        NULL,
    bronze_id          BIGINT           NULL,
    sync_run_id        UNIQUEIDENTIFIER NULL,
    is_deleted         BIT              NOT NULL CONSTRAINT df_silver_competence_competitors_is_deleted DEFAULT 0,
    deleted_at         DATETIME2        NULL,
    last_synced_at     DATETIME2        NOT NULL CONSTRAINT df_silver_competence_competitors_synced_at DEFAULT GETUTCDATE(),
    CONSTRAINT pk_silver_competence_competitors PRIMARY KEY (source_id)
);

CREATE INDEX ix_silver_competence_competitors_list         ON silver.competence_competitors (list_source_id);
CREATE INDEX ix_silver_competence_competitors_place        ON silver.competence_competitors (place_id);
CREATE INDEX ix_silver_competence_competitors_city         ON silver.competence_competitors (city);
CREATE INDEX ix_silver_competence_competitors_country      ON silver.competence_competitors (country_code);
CREATE INDEX ix_silver_competence_competitors_country_name ON silver.competence_competitors (country);
GO
