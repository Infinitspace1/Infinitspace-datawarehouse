-- ============================================================
-- silver_nexudus_products.sql  (revised — single table)
--
-- Drop extension tables if they exist, rebuild as one table.
-- ============================================================

DROP TABLE IF EXISTS silver.nexudus_product_offices;
DROP TABLE IF EXISTS silver.nexudus_product_desks;
DROP TABLE IF EXISTS silver.nexudus_product_rooms;
DROP TABLE IF EXISTS silver.nexudus_products;
GO

CREATE TABLE silver.nexudus_products (
    id                          BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id                   BIGINT          NOT NULL,
    CONSTRAINT uq_silver_nexudus_products_source_id UNIQUE (source_id),

    -- Traceability
    bronze_id                   BIGINT          NULL,
    sync_run_id                 UNIQUEIDENTIFIER NULL,

    -- Classification
    item_type                   TINYINT         NOT NULL,   -- 1=Office 2=Dedicated 3=Hot 4=Other 5=Room
    product_type_label          NVARCHAR(32)    NOT NULL,

    -- Location
    location_source_id          BIGINT          NOT NULL,
    location_name               NVARCHAR(512)   NULL,
    floor_plan_id               BIGINT          NULL,
    floor_plan_name             NVARCHAR(256)   NULL,

    -- Identity
    name                        NVARCHAR(256)   NOT NULL,
    area_code                   NVARCHAR(128)   NULL,

    -- Pricing
    price                       DECIMAL(12,2)   NULL,
    currency_code               NVARCHAR(8)     NULL,

    -- Availability
    is_available                BIT             NOT NULL DEFAULT 1,
    available_from              DATETIME2       NULL,
    available_to                DATETIME2       NULL,

    -- Current occupant
    coworker_id                 BIGINT          NULL,
    coworker_name               NVARCHAR(512)   NULL,
    coworker_company            NVARCHAR(512)   NULL,
    coworker_email              NVARCHAR(512)   NULL,
    contract_ids_raw            NVARCHAR(1024)  NULL,

    -- Physical
    size_sqm                    FLOAT           NULL,       -- from floor plan geometry
    custom_size_sqm             FLOAT           NULL,       -- from CustomFields (type 1 only, more accurate)
    capacity                    INT             NULL,
    size_is_linked_to_area      BIT             NULL,

    -- Room/resource fields (types 4+5, null for others)
    resource_id                 BIGINT          NULL,
    resource_name               NVARCHAR(512)   NULL,
    resource_type_name          NVARCHAR(256)   NULL,
    resource_allocation         INT             NULL,
    resource_shifts             NVARCHAR(MAX)   NULL,

    -- Amenities (types 4+5, null for others)
    amenity_air_conditioning    BIT             NULL,
    amenity_heating             BIT             NULL,
    amenity_internet            BIT             NULL,
    amenity_large_display       BIT             NULL,
    amenity_natural_light       BIT             NULL,
    amenity_whiteboard          BIT             NULL,
    amenity_soundproof          BIT             NULL,
    amenity_quiet_zone          BIT             NULL,
    amenity_tea_coffee          BIT             NULL,
    amenity_security_lock       BIT             NULL,
    amenity_cctv                BIT             NULL,
    amenity_catering            BIT             NULL,
    amenity_conference_phone    BIT             NULL,
    amenity_projector           BIT             NULL,
    amenity_standing_desk       BIT             NULL,
    amenity_drinks              BIT             NULL,
    amenity_privacy_screen      BIT             NULL,
    amenity_voice_recorder      BIT             NULL,
    amenity_standard_phone      BIT             NULL,
    amenity_wireless_charger    BIT             NULL,

    -- Nexudus timestamps
    created_on                  DATETIME2       NULL,
    updated_on                  DATETIME2       NULL,

    -- Pipeline timestamps
    first_seen_at               DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_products_location  ON silver.nexudus_products (location_source_id);
CREATE INDEX ix_silver_nexudus_products_item_type ON silver.nexudus_products (item_type);
CREATE INDEX ix_silver_nexudus_products_available ON silver.nexudus_products (is_available);
CREATE INDEX ix_silver_nexudus_products_resource  ON silver.nexudus_products (resource_id);
GO