-- ============================================================
-- silver_nexudus_resources_alignment.sql
--
-- Non-destructive alignment for environments where
-- silver.nexudus_resources exists but is missing newer columns.
--
-- Safe to run multiple times.
-- ============================================================

IF OBJECT_ID('silver.nexudus_resources', 'U') IS NULL
BEGIN
    RAISERROR(
        'silver.nexudus_resources does not exist. Run silver_nexudus_resources_schema.sql first.',
        16,
        1
    );
    RETURN;
END
GO

IF COL_LENGTH('silver.nexudus_resources', 'group_id') IS NULL
    ALTER TABLE silver.nexudus_resources ADD group_id BIGINT NULL;
GO

IF COL_LENGTH('silver.nexudus_resources', 'group_name') IS NULL
    ALTER TABLE silver.nexudus_resources ADD group_name NVARCHAR(256) NULL;
GO

IF COL_LENGTH('silver.nexudus_resources', 'is_visible') IS NULL
    ALTER TABLE silver.nexudus_resources ADD is_visible BIT NULL;
GO

IF COL_LENGTH('silver.nexudus_resources', 'allocation') IS NULL
    ALTER TABLE silver.nexudus_resources ADD allocation INT NULL;
GO

IF COL_LENGTH('silver.nexudus_resources', 'online') IS NULL
    ALTER TABLE silver.nexudus_resources ADD online BIT NULL;
GO

IF COL_LENGTH('silver.nexudus_resources', 'visible_to_others') IS NULL
    ALTER TABLE silver.nexudus_resources ADD visible_to_others BIT NULL;
GO

IF COL_LENGTH('silver.nexudus_resources', 'available') IS NULL
    ALTER TABLE silver.nexudus_resources ADD available BIT NULL;
GO

IF COL_LENGTH('silver.nexudus_resources', 'accessible') IS NULL
    ALTER TABLE silver.nexudus_resources ADD accessible BIT NULL;
GO

IF COL_LENGTH('silver.nexudus_resources', 'visible') IS NOT NULL
BEGIN
    EXEC sp_executesql N'
        UPDATE silver.nexudus_resources
        SET is_visible = COALESCE(is_visible, visible)
        WHERE is_visible IS NULL;
    ';
END
GO

IF COL_LENGTH('silver.nexudus_resources', 'capacity') IS NOT NULL
BEGIN
    EXEC sp_executesql N'
        UPDATE silver.nexudus_resources
        SET allocation = COALESCE(allocation, TRY_CONVERT(INT, capacity))
        WHERE allocation IS NULL;
    ';
END
GO

UPDATE silver.nexudus_resources
SET is_visible = COALESCE(is_visible, 0),
    online = COALESCE(online, 0),
    visible_to_others = COALESCE(visible_to_others, 0),
    available = COALESCE(available, 0),
    accessible = COALESCE(accessible, 0)
WHERE is_visible IS NULL
   OR online IS NULL
   OR visible_to_others IS NULL
   OR available IS NULL
   OR accessible IS NULL;
GO

IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'is_visible'
      AND is_nullable = 1
)
    ALTER TABLE silver.nexudus_resources ALTER COLUMN is_visible BIT NOT NULL;
GO

IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'online'
      AND is_nullable = 1
)
    ALTER TABLE silver.nexudus_resources ALTER COLUMN online BIT NOT NULL;
GO

IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'visible_to_others'
      AND is_nullable = 1
)
    ALTER TABLE silver.nexudus_resources ALTER COLUMN visible_to_others BIT NOT NULL;
GO

IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'available'
      AND is_nullable = 1
)
    ALTER TABLE silver.nexudus_resources ALTER COLUMN available BIT NOT NULL;
GO

IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'accessible'
      AND is_nullable = 1
)
    ALTER TABLE silver.nexudus_resources ALTER COLUMN accessible BIT NOT NULL;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.default_constraints
    WHERE parent_object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'df_silver_nexudus_resources_is_visible'
)
    ALTER TABLE silver.nexudus_resources
    ADD CONSTRAINT df_silver_nexudus_resources_is_visible DEFAULT 0 FOR is_visible;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.default_constraints
    WHERE parent_object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'df_silver_nexudus_resources_online'
)
    ALTER TABLE silver.nexudus_resources
    ADD CONSTRAINT df_silver_nexudus_resources_online DEFAULT 0 FOR online;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.default_constraints
    WHERE parent_object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'df_silver_nexudus_resources_visible_to_others'
)
    ALTER TABLE silver.nexudus_resources
    ADD CONSTRAINT df_silver_nexudus_resources_visible_to_others DEFAULT 0 FOR visible_to_others;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.default_constraints
    WHERE parent_object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'df_silver_nexudus_resources_available'
)
    ALTER TABLE silver.nexudus_resources
    ADD CONSTRAINT df_silver_nexudus_resources_available DEFAULT 0 FOR available;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.default_constraints
    WHERE parent_object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'df_silver_nexudus_resources_accessible'
)
    ALTER TABLE silver.nexudus_resources
    ADD CONSTRAINT df_silver_nexudus_resources_accessible DEFAULT 0 FOR accessible;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('silver.nexudus_resources')
      AND name = 'ix_silver_nexudus_resources_visible'
)
    CREATE INDEX ix_silver_nexudus_resources_visible
        ON silver.nexudus_resources (is_visible);
GO
