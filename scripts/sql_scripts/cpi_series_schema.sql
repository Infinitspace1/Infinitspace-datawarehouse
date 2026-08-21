-- =============================================================================
-- cpi_series_schema.sql
--
-- 2026-08-20: National consumer price indices for the UK, the Netherlands and
-- Germany, so the Budget Tracking Tool's anniversaries tab can offer a CPI%
-- uplift column next to its existing 3% and 5% columns.
--
-- Source APIs (all public, unauthenticated, no key):
--   UK  ONS       https://www.ons.gov.uk/.../timeseries/{d7g7|d7bt}/mm23/data
--   NL  CBS       https://datasets.cbs.nl/odata/v1/CBS/86141NED/Observations
--   DE  Eurostat  https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_minr
--
-- Synced by functions/cpi_sync.py under source_name 'cpi', entity 'series'.
-- Ships inside ENABLE_ETL_FUNCTIONS - no secret, so no dedicated flag (a
-- dedicated flag would default OFF and never run).
--
-- Design notes:
--   * source_id = '{provider}:{geo}:{period}', e.g. 'ons:UK:2026-07'. Both the
--     bronze and silver grain is ONE OBSERVATION, not one API response, because
--     statistics offices REVISE already-published months. Keying at observation
--     level makes a revision a hash change on exactly the month that moved, and
--     a MERGE rather than a duplicate row.
--   * Both index_level AND annual_rate_pct are stored. Every escalation clause
--     in these three countries is a RATIO of two index levels at named
--     reference months; storing only the 12-month rate makes the contractual
--     calculation impossible to reproduce, and the rate carries one decimal
--     where the level carries two.
--   * `status` exists because CBS states verbatim of its provisional figures:
--     "Deze cijfers ... zijn niet geschikt om te gebruiken voor indexering."
--     ONS does not revise its published rate, so UK rows are definitive on
--     first publication. The asymmetry is per-provider, not global.
--   * GERMANY IS THE HARMONISED INDEX (HICP), NOT THE DOMESTIC VPI. Destatis
--     GENESIS requires a registered account (its data endpoint answers 401
--     anonymously), and the team chose Eurostat over creating one. index_code
--     records 'HICP' for DE and 'CPI' for UK/NL so nothing downstream can
--     confuse the two - a German agreement names the VPI, which this is not.
--   * NATIONAL geography only. No monthly city-level CPI exists for London or
--     Amsterdam (ONS declares its CPIH geography 'uk-only'; CBS 86141NED has no
--     regional dimension at all). Berlin has one but it is PDF/XLSX behind
--     hashed URLs. Escalation clauses name national indices anyway.
--   * DECIMAL, never FLOAT: the warehouse writes with pyodbc and the Budget
--     Tracking Tool reads with python-tds, and float coercion differs.
--   * No is_deleted / reconcile: a published month is never withdrawn.
--
-- Apply with:
--   .\venv\Scripts\python.exe scripts\python_scripts\apply_schema_script.py scripts/sql_scripts/cpi_series_schema.sql
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC sp_executesql N'CREATE SCHEMA bronze';
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC sp_executesql N'CREATE SCHEMA silver';
GO


-- =============================================================================
-- 1. bronze.cpi_series -- raw observation payloads, hash-deduped
-- =============================================================================

IF OBJECT_ID('bronze.cpi_series', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.cpi_series (
        id              BIGINT              IDENTITY(1,1) NOT NULL,
        CONSTRAINT pk_bronze_cpi_series PRIMARY KEY (id),

        sync_run_id     UNIQUEIDENTIFIER    NOT NULL,
        source_id       NVARCHAR(64)        NOT NULL,   -- '{provider}:{geo}:{period}'
        CONSTRAINT uq_bronze_cpi_series_source_id UNIQUE (source_id),

        -- Denormalised filter columns, so bronze can be inspected without
        -- parsing raw_json.
        provider        NVARCHAR(32)        NULL,       -- 'ons' | 'cbs' | 'eurostat'
        geo             NVARCHAR(8)         NULL,       -- 'UK' | 'NL' | 'DE'
        period          CHAR(7)             NULL,       -- 'YYYY-MM'

        raw_json        NVARCHAR(MAX)       NOT NULL,
        payload_hash    CHAR(64)            NULL,       -- SHA-256, revision detection

        synced_at       DATETIME2           NOT NULL
            CONSTRAINT df_bronze_cpi_series_synced_at DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'ix_bronze_cpi_series_synced_at'
                 AND object_id = OBJECT_ID('bronze.cpi_series'))
    CREATE INDEX ix_bronze_cpi_series_synced_at ON bronze.cpi_series (synced_at);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'ix_bronze_cpi_series_sync_run_id'
                 AND object_id = OBJECT_ID('bronze.cpi_series'))
    CREATE INDEX ix_bronze_cpi_series_sync_run_id ON bronze.cpi_series (sync_run_id);
GO


-- =============================================================================
-- 2. silver.cpi_series -- one flat row per (provider, geo, period)
-- =============================================================================

IF OBJECT_ID('silver.cpi_series', 'U') IS NULL
BEGIN
    CREATE TABLE silver.cpi_series (
        source_id        NVARCHAR(64)   NOT NULL,   -- '{provider}:{geo}:{period}'
        CONSTRAINT pk_silver_cpi_series PRIMARY KEY (source_id),

        bronze_id        BIGINT         NULL,
        sync_run_id      UNIQUEIDENTIFIER NULL,

        provider         NVARCHAR(32)   NOT NULL,   -- 'ons' | 'cbs' | 'eurostat'
        geo              NVARCHAR(8)    NOT NULL,   -- 'UK' | 'NL' | 'DE'
        index_code       NVARCHAR(16)   NOT NULL,   -- 'CPI' (UK, NL) | 'HICP' (DE)
        index_name       NVARCHAR(256)  NULL,       -- human label incl. the provider
        base_year        NVARCHAR(8)    NULL,       -- '2015' (UK) | '2025' (NL, DE)

        period           CHAR(7)        NOT NULL,   -- 'YYYY-MM'
        index_level      DECIMAL(12, 4) NULL,       -- the number a ratio clause needs
        annual_rate_pct  DECIMAL(9, 4)  NULL,       -- 12-month rate, percent

        status           NVARCHAR(16)   NOT NULL
            CONSTRAINT df_silver_cpi_series_status DEFAULT N'definitive',
        CONSTRAINT ck_silver_cpi_series_status
            CHECK (status IN (N'definitive', N'provisional')),

        source_url       NVARCHAR(512)  NULL,
        published_at     NVARCHAR(64)   NULL,       -- provider's own release stamp, as given

        first_seen_at    DATETIME2      NOT NULL
            CONSTRAINT df_silver_cpi_series_first_seen DEFAULT GETUTCDATE(),
        last_synced_at   DATETIME2      NOT NULL
            CONSTRAINT df_silver_cpi_series_synced_at DEFAULT GETUTCDATE()
    );
END
GO

-- The consumer always filters (geo, period) or (geo) ordered by period desc.
IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'ix_silver_cpi_series_geo_period'
                 AND object_id = OBJECT_ID('silver.cpi_series'))
    CREATE INDEX ix_silver_cpi_series_geo_period ON silver.cpi_series (geo, period DESC);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'ix_silver_cpi_series_period'
                 AND object_id = OBJECT_ID('silver.cpi_series'))
    CREATE INDEX ix_silver_cpi_series_period ON silver.cpi_series (period DESC);
GO
