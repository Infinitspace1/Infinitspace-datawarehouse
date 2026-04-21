-- =============================================================================
-- silver_soft_delete_migration.sql
--
-- Adds soft-delete columns (is_deleted, deleted_at) to all silver tables that
-- are populated from upsert-only (MERGE) writers. These columns let the
-- reconciliation jobs (daily for invoices / invoice_lines, weekly for the rest)
-- flag records that have been removed at the source without hard-deleting.
--
-- Downstream consumers (gold tables, views, reports) must filter
-- `WHERE is_deleted = 0` to exclude stale records.
--
-- Idempotent — safe to run multiple times. Columns are only added if missing.
-- =============================================================================

-- ── silver.nexudus_coworker_invoices ─────────────────────────────
IF COL_LENGTH('silver.nexudus_coworker_invoices', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworker_invoices
    ADD is_deleted BIT NOT NULL CONSTRAINT df_silver_nexudus_coworker_invoices_is_deleted DEFAULT 0;
END
GO

IF COL_LENGTH('silver.nexudus_coworker_invoices', 'deleted_at') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworker_invoices
    ADD deleted_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworker_invoices_is_deleted'
      AND object_id = OBJECT_ID('silver.nexudus_coworker_invoices')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworker_invoices_is_deleted
        ON silver.nexudus_coworker_invoices (is_deleted)
        WHERE is_deleted = 1;
END
GO


-- ── silver.nexudus_coworker_invoice_lines ────────────────────────
IF COL_LENGTH('silver.nexudus_coworker_invoice_lines', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworker_invoice_lines
    ADD is_deleted BIT NOT NULL CONSTRAINT df_silver_nexudus_coworker_invoice_lines_is_deleted DEFAULT 0;
END
GO

IF COL_LENGTH('silver.nexudus_coworker_invoice_lines', 'deleted_at') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworker_invoice_lines
    ADD deleted_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworker_invoice_lines_is_deleted'
      AND object_id = OBJECT_ID('silver.nexudus_coworker_invoice_lines')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworker_invoice_lines_is_deleted
        ON silver.nexudus_coworker_invoice_lines (is_deleted)
        WHERE is_deleted = 1;
END
GO


-- ── silver.nexudus_locations ─────────────────────────────────────
IF COL_LENGTH('silver.nexudus_locations', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_locations
    ADD is_deleted BIT NOT NULL CONSTRAINT df_silver_nexudus_locations_is_deleted DEFAULT 0;
END
GO

IF COL_LENGTH('silver.nexudus_locations', 'deleted_at') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_locations
    ADD deleted_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_locations_is_deleted'
      AND object_id = OBJECT_ID('silver.nexudus_locations')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_locations_is_deleted
        ON silver.nexudus_locations (is_deleted)
        WHERE is_deleted = 1;
END
GO


-- ── silver.nexudus_products ──────────────────────────────────────
IF COL_LENGTH('silver.nexudus_products', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_products
    ADD is_deleted BIT NOT NULL CONSTRAINT df_silver_nexudus_products_is_deleted DEFAULT 0;
END
GO

IF COL_LENGTH('silver.nexudus_products', 'deleted_at') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_products
    ADD deleted_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_products_is_deleted'
      AND object_id = OBJECT_ID('silver.nexudus_products')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_products_is_deleted
        ON silver.nexudus_products (is_deleted)
        WHERE is_deleted = 1;
END
GO


-- ── silver.nexudus_contracts ─────────────────────────────────────
IF COL_LENGTH('silver.nexudus_contracts', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_contracts
    ADD is_deleted BIT NOT NULL CONSTRAINT df_silver_nexudus_contracts_is_deleted DEFAULT 0;
END
GO

IF COL_LENGTH('silver.nexudus_contracts', 'deleted_at') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_contracts
    ADD deleted_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_contracts_is_deleted'
      AND object_id = OBJECT_ID('silver.nexudus_contracts')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_contracts_is_deleted
        ON silver.nexudus_contracts (is_deleted)
        WHERE is_deleted = 1;
END
GO


-- ── silver.nexudus_extra_services ────────────────────────────────
IF COL_LENGTH('silver.nexudus_extra_services', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_extra_services
    ADD is_deleted BIT NOT NULL CONSTRAINT df_silver_nexudus_extra_services_is_deleted DEFAULT 0;
END
GO

IF COL_LENGTH('silver.nexudus_extra_services', 'deleted_at') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_extra_services
    ADD deleted_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_extra_services_is_deleted'
      AND object_id = OBJECT_ID('silver.nexudus_extra_services')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_extra_services_is_deleted
        ON silver.nexudus_extra_services (is_deleted)
        WHERE is_deleted = 1;
END
GO


-- ── silver.nexudus_resources ─────────────────────────────────────
IF COL_LENGTH('silver.nexudus_resources', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_resources
    ADD is_deleted BIT NOT NULL CONSTRAINT df_silver_nexudus_resources_is_deleted DEFAULT 0;
END
GO

IF COL_LENGTH('silver.nexudus_resources', 'deleted_at') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_resources
    ADD deleted_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_resources_is_deleted'
      AND object_id = OBJECT_ID('silver.nexudus_resources')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_resources_is_deleted
        ON silver.nexudus_resources (is_deleted)
        WHERE is_deleted = 1;
END
GO


-- ── silver.nexudus_coworkers ─────────────────────────────────────
IF COL_LENGTH('silver.nexudus_coworkers', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworkers
    ADD is_deleted BIT NOT NULL CONSTRAINT df_silver_nexudus_coworkers_is_deleted DEFAULT 0;
END
GO

IF COL_LENGTH('silver.nexudus_coworkers', 'deleted_at') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworkers
    ADD deleted_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworkers_is_deleted'
      AND object_id = OBJECT_ID('silver.nexudus_coworkers')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworkers_is_deleted
        ON silver.nexudus_coworkers (is_deleted)
        WHERE is_deleted = 1;
END
GO


-- ── silver.bamboohr_employees ────────────────────────────────────
IF COL_LENGTH('silver.bamboohr_employees', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE silver.bamboohr_employees
    ADD is_deleted BIT NOT NULL CONSTRAINT df_silver_bamboohr_employees_is_deleted DEFAULT 0;
END
GO

IF COL_LENGTH('silver.bamboohr_employees', 'deleted_at') IS NULL
BEGIN
    ALTER TABLE silver.bamboohr_employees
    ADD deleted_at DATETIME2 NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_bamboohr_employees_is_deleted'
      AND object_id = OBJECT_ID('silver.bamboohr_employees')
)
BEGIN
    CREATE INDEX ix_silver_bamboohr_employees_is_deleted
        ON silver.bamboohr_employees (is_deleted)
        WHERE is_deleted = 1;
END
GO


-- ── Verification ─────────────────────────────────────────────────
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver'
  AND COLUMN_NAME IN ('is_deleted', 'deleted_at')
  AND TABLE_NAME IN (
      'nexudus_coworker_invoices',
      'nexudus_coworker_invoice_lines',
      'nexudus_locations',
      'nexudus_products',
      'nexudus_contracts',
      'nexudus_extra_services',
      'nexudus_resources',
      'nexudus_coworkers',
      'bamboohr_employees'
  )
ORDER BY TABLE_NAME, COLUMN_NAME;
