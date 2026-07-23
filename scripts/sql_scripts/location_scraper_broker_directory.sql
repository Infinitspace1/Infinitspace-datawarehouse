-- Broker directory: persistent broker name -> email memory for LoopNet.
--
-- Why: the memo23 LoopNet actor has twice (2026-06-27, ~2026-07-13) shipped
-- payload changes that dropped broker contact fields. This table remembers
-- every (broker name, email) pair ever observed so listings arriving with a
-- broker NAME but no EMAIL can be back-filled from history during globe
-- materialization (shared/location_scraper/broker_directory.py).
--
-- Populated by:
--   * one-off backfill: scripts/python_scripts/backfill_broker_directory.py
--   * ongoing: every LoopNet globe materialization upserts the pairs it sees
--
-- Idempotent. Apply via:
--   .\venv\Scripts\python.exe scripts\python_scripts\apply_schema_script.py scripts/sql_scripts/location_scraper_broker_directory.sql

IF OBJECT_ID(N'silver.location_scraper_broker_directory', N'U') IS NULL
BEGIN
    CREATE TABLE silver.location_scraper_broker_directory (
        id              INT IDENTITY(1,1) NOT NULL,
        name_normalized NVARCHAR(300) NOT NULL,  -- lowercased, whitespace-collapsed
        name_display    NVARCHAR(300) NULL,      -- original casing, for display
        email           NVARCHAR(320) NOT NULL,  -- lowercased
        company         NVARCHAR(300) NULL,      -- last non-null brokerage seen
        phone           NVARCHAR(100) NULL,      -- last non-null phone seen
        source          NVARCHAR(50)  NOT NULL CONSTRAINT DF_ls_broker_dir_source DEFAULT 'loopnet',
        seen_count      INT           NOT NULL CONSTRAINT DF_ls_broker_dir_seen DEFAULT 1,
        first_seen_at   DATETIME2     NOT NULL CONSTRAINT DF_ls_broker_dir_first DEFAULT GETUTCDATE(),
        last_seen_at    DATETIME2     NOT NULL CONSTRAINT DF_ls_broker_dir_last DEFAULT GETUTCDATE(),
        CONSTRAINT PK_ls_broker_directory PRIMARY KEY (id),
        CONSTRAINT UQ_ls_broker_directory UNIQUE (name_normalized, email)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ls_broker_directory_name'
      AND object_id = OBJECT_ID(N'silver.location_scraper_broker_directory')
)
BEGIN
    CREATE INDEX IX_ls_broker_directory_name
        ON silver.location_scraper_broker_directory (name_normalized);
END
GO

-- Verification
SELECT COUNT(*) AS broker_directory_rows FROM silver.location_scraper_broker_directory;
GO
