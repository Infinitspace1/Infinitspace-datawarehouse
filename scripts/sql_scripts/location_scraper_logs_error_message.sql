-- Add error_message column to bronze.n8n_location_scraper_logs
-- so orchestrator failures are visible in SQL (previously only logged to
-- App Insights, which made the 2026-06-01 monthly-run failure hard to diagnose).
-- Idempotent: safe to re-run.

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('bronze.n8n_location_scraper_logs')
      AND name = 'error_message'
)
BEGIN
    ALTER TABLE bronze.n8n_location_scraper_logs
        ADD error_message NVARCHAR(MAX) NULL;
END;
GO
