-- Location Scraper: log table hardening and stale-run cleanup
--
-- Purpose:
--   1) Ensure bronze.n8n_location_scraper_logs.updated_at has a default.
--   2) Backfill missing updated_at values.
--   3) Mark old abandoned running runs as failed.
--
-- Usage:
--   Set @stale_hours as needed, then run this script.

DECLARE @stale_hours INT = 2;

IF OBJECT_ID(N'bronze.n8n_location_scraper_logs', N'U') IS NULL
BEGIN
    THROW 51000, 'bronze.n8n_location_scraper_logs does not exist. Run location_scraper_schema.sql first.', 1;
END
GO

IF COL_LENGTH(N'bronze.n8n_location_scraper_logs', N'updated_at') IS NULL
BEGIN
    ALTER TABLE bronze.n8n_location_scraper_logs
        ADD updated_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.default_constraints AS dc
    JOIN sys.columns AS c
        ON c.default_object_id = dc.object_id
    WHERE dc.parent_object_id = OBJECT_ID(N'bronze.n8n_location_scraper_logs', N'U')
      AND c.name = N'updated_at'
)
BEGIN
    ALTER TABLE bronze.n8n_location_scraper_logs
        ADD CONSTRAINT DF_n8n_location_scraper_logs_updated_at
            DEFAULT GETUTCDATE() FOR updated_at;
END
GO

UPDATE bronze.n8n_location_scraper_logs
SET updated_at = ISNULL(created_at, GETUTCDATE())
WHERE updated_at IS NULL;
GO

DECLARE @stale_hours_cleanup INT = 2;

UPDATE bronze.n8n_location_scraper_logs
SET
    status = N'failed',
    updated_at = GETUTCDATE()
WHERE status = N'running'
  AND created_at < DATEADD(hour, -@stale_hours_cleanup, GETUTCDATE());

SELECT
    @@ROWCOUNT AS stale_runs_marked_failed;
GO
