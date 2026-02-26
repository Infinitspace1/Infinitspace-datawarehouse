-- ============================================================
-- bronze_upsert_constraints.sql
--
-- Purpose:
--   Add UNIQUE constraints on bronze source_id keys once
--   append-only duplicates have been cleaned up.
--
-- Safety:
--   Each table is checked for duplicate source_id values first.
--   If duplicates exist, the constraint is skipped with a clear message.
-- ============================================================

PRINT 'Applying bronze upsert constraints (guarded mode)...';
GO

-- ------------------------------------------------------------
-- bronze.nexudus_locations
-- ------------------------------------------------------------
IF EXISTS (
    SELECT source_id
    FROM bronze.nexudus_locations
    GROUP BY source_id
    HAVING COUNT(*) > 1
)
BEGIN
    PRINT 'SKIP: bronze.nexudus_locations has duplicate source_id rows. Deduplicate before adding uq_bronze_nexudus_locations_source_id.';
END
ELSE IF NOT EXISTS (
    SELECT 1
    FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID('bronze.nexudus_locations')
      AND name = 'uq_bronze_nexudus_locations_source_id'
)
BEGIN
    ALTER TABLE bronze.nexudus_locations
    ADD CONSTRAINT uq_bronze_nexudus_locations_source_id UNIQUE (source_id);
    PRINT 'OK: Added uq_bronze_nexudus_locations_source_id.';
END
ELSE
BEGIN
    PRINT 'OK: uq_bronze_nexudus_locations_source_id already exists.';
END
GO

-- ------------------------------------------------------------
-- bronze.nexudus_products
-- ------------------------------------------------------------
IF EXISTS (
    SELECT source_id
    FROM bronze.nexudus_products
    GROUP BY source_id
    HAVING COUNT(*) > 1
)
BEGIN
    PRINT 'SKIP: bronze.nexudus_products has duplicate source_id rows. Deduplicate before adding uq_bronze_nexudus_products_source_id.';
END
ELSE IF NOT EXISTS (
    SELECT 1
    FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID('bronze.nexudus_products')
      AND name = 'uq_bronze_nexudus_products_source_id'
)
BEGIN
    ALTER TABLE bronze.nexudus_products
    ADD CONSTRAINT uq_bronze_nexudus_products_source_id UNIQUE (source_id);
    PRINT 'OK: Added uq_bronze_nexudus_products_source_id.';
END
ELSE
BEGIN
    PRINT 'OK: uq_bronze_nexudus_products_source_id already exists.';
END
GO

-- ------------------------------------------------------------
-- bronze.nexudus_contracts
-- ------------------------------------------------------------
IF EXISTS (
    SELECT source_id
    FROM bronze.nexudus_contracts
    GROUP BY source_id
    HAVING COUNT(*) > 1
)
BEGIN
    PRINT 'SKIP: bronze.nexudus_contracts has duplicate source_id rows. Deduplicate before adding uq_bronze_nexudus_contracts_source_id.';
END
ELSE IF NOT EXISTS (
    SELECT 1
    FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID('bronze.nexudus_contracts')
      AND name = 'uq_bronze_nexudus_contracts_source_id'
)
BEGIN
    ALTER TABLE bronze.nexudus_contracts
    ADD CONSTRAINT uq_bronze_nexudus_contracts_source_id UNIQUE (source_id);
    PRINT 'OK: Added uq_bronze_nexudus_contracts_source_id.';
END
ELSE
BEGIN
    PRINT 'OK: uq_bronze_nexudus_contracts_source_id already exists.';
END
GO

-- ------------------------------------------------------------
-- bronze.nexudus_resources
-- ------------------------------------------------------------
IF EXISTS (
    SELECT source_id
    FROM bronze.nexudus_resources
    GROUP BY source_id
    HAVING COUNT(*) > 1
)
BEGIN
    PRINT 'SKIP: bronze.nexudus_resources has duplicate source_id rows. Deduplicate before adding uq_bronze_nexudus_resources_source_id.';
END
ELSE IF NOT EXISTS (
    SELECT 1
    FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID('bronze.nexudus_resources')
      AND name = 'uq_bronze_nexudus_resources_source_id'
)
BEGIN
    ALTER TABLE bronze.nexudus_resources
    ADD CONSTRAINT uq_bronze_nexudus_resources_source_id UNIQUE (source_id);
    PRINT 'OK: Added uq_bronze_nexudus_resources_source_id.';
END
ELSE
BEGIN
    PRINT 'OK: uq_bronze_nexudus_resources_source_id already exists.';
END
GO

-- ------------------------------------------------------------
-- bronze.nexudus_extra_services
-- ------------------------------------------------------------
IF EXISTS (
    SELECT source_id
    FROM bronze.nexudus_extra_services
    GROUP BY source_id
    HAVING COUNT(*) > 1
)
BEGIN
    PRINT 'SKIP: bronze.nexudus_extra_services has duplicate source_id rows. Deduplicate before adding uq_bronze_nexudus_extra_services_source_id.';
END
ELSE IF NOT EXISTS (
    SELECT 1
    FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID('bronze.nexudus_extra_services')
      AND name = 'uq_bronze_nexudus_extra_services_source_id'
)
BEGIN
    ALTER TABLE bronze.nexudus_extra_services
    ADD CONSTRAINT uq_bronze_nexudus_extra_services_source_id UNIQUE (source_id);
    PRINT 'OK: Added uq_bronze_nexudus_extra_services_source_id.';
END
ELSE
BEGIN
    PRINT 'OK: uq_bronze_nexudus_extra_services_source_id already exists.';
END
GO

PRINT 'bronze_upsert_constraints.sql complete.';
GO
