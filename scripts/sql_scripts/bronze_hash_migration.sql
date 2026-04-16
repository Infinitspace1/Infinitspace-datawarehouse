-- ============================================================
-- bronze_hash_migration.sql
-- Adds payload_hash column to all Nexudus bronze tables for
-- change detection. CHAR(64) stores SHA-256 hex digest.
-- NULL for existing rows; populated on next sync run.
-- ============================================================

ALTER TABLE bronze.nexudus_locations ADD payload_hash CHAR(64) NULL;
GO

ALTER TABLE bronze.nexudus_products ADD payload_hash CHAR(64) NULL;
GO

ALTER TABLE bronze.nexudus_contracts ADD payload_hash CHAR(64) NULL;
GO

ALTER TABLE bronze.nexudus_resources ADD payload_hash CHAR(64) NULL;
GO

ALTER TABLE bronze.nexudus_extra_services ADD payload_hash CHAR(64) NULL;
GO

ALTER TABLE bronze.nexudus_coworker_invoices ADD payload_hash CHAR(64) NULL;
GO

ALTER TABLE bronze.nexudus_coworker_invoice_lines ADD payload_hash CHAR(64) NULL;
GO

ALTER TABLE bronze.nexudus_coworkers ADD payload_hash CHAR(64) NULL;
GO
