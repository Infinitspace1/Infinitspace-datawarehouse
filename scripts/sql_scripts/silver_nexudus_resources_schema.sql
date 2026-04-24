-- ============================================================
-- silver_nexudus_resources_schema.sql
--
-- One table: silver.nexudus_resources
-- Source: bronze.nexudus_resources (raw Resource JSON)
--
-- Upsert key: source_id (Nexudus Resource Id)
-- Relationship: location_source_id -> silver.nexudus_locations.source_id
--               source_id          -> silver.nexudus_products.resource_id (soft)
-- ============================================================

DROP TABLE IF EXISTS silver.nexudus_resources;
GO

CREATE TABLE silver.nexudus_resources (
    id                  BIGINT           IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id           BIGINT           NOT NULL,
    CONSTRAINT uq_silver_nexudus_resources_source_id UNIQUE (source_id),

    -- Traceability
    bronze_id           BIGINT           NULL,
    sync_run_id         UNIQUEIDENTIFIER NULL,

    -- Location
    location_source_id  BIGINT           NOT NULL,

    -- Identity
    nexudus_uuid        NVARCHAR(64)     NULL,
    name                NVARCHAR(512)    NOT NULL,
    description         NVARCHAR(MAX)    NULL,

    -- Classification
    resource_type_id    BIGINT           NULL,
    resource_type_name  NVARCHAR(256)    NULL,
    group_id            BIGINT           NULL,
    group_name          NVARCHAR(256)    NULL,

    -- Availability / visibility
    is_visible          BIT              NOT NULL DEFAULT 0,
    online              BIT              NOT NULL DEFAULT 0,
    visible_to_others   BIT              NOT NULL DEFAULT 0,
    available           BIT              NOT NULL DEFAULT 0,

    -- Physical
    allocation          INT              NULL,
    size                FLOAT            NULL,
    floor_number        INT              NULL,
    accessible          BIT              NOT NULL DEFAULT 0,

    -- Nexudus timestamps
    created_on          DATETIME2        NULL,
    updated_on          DATETIME2        NULL,

    -- Pipeline timestamps
    first_seen_at       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at      DATETIME2        NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_resources_location
    ON silver.nexudus_resources (location_source_id);

CREATE INDEX ix_silver_nexudus_resources_type
    ON silver.nexudus_resources (resource_type_name);

CREATE INDEX ix_silver_nexudus_resources_visible
    ON silver.nexudus_resources (is_visible);
GO
