-- ============================================================
-- silver_nexudus_extra_services_schema.sql
--
-- One table: silver.nexudus_extra_services
-- Source: bronze.nexudus_extra_services (raw ExtraService JSON)
--
-- Upsert key: source_id (Nexudus ExtraService Id)
-- Relationship: location_source_id → silver.nexudus_locations.source_id
--               resource_type_names → soft link to products (types 4+5)
--                   via silver.nexudus_products.resource_type_name
--
-- Extra services define pricing tiers for bookable resources at a location.
-- Products of type 4 (Other) or 5 (Meeting Room) are booked against an
-- extra service — the link is: product.resource_type_name IN extra_service.resource_type_names
-- ============================================================

DROP TABLE IF EXISTS silver.nexudus_extra_services;
GO

CREATE TABLE silver.nexudus_extra_services (
    id                          BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id                   BIGINT          NOT NULL,
    CONSTRAINT uq_silver_nexudus_extra_services_source_id UNIQUE (source_id),
    unique_id                   NVARCHAR(64)    NULL,               -- Nexudus UniqueId (GUID)

    -- Traceability
    bronze_id                   BIGINT          NULL,
    sync_run_id                 UNIQUEIDENTIFIER NULL,

    -- Location (soft FK → silver.nexudus_locations.source_id)
    location_source_id          BIGINT          NOT NULL,           -- BusinessId

    -- Identity
    name                        NVARCHAR(512)   NOT NULL,
    description                 NVARCHAR(MAX)   NULL,

    -- Pricing
    price                       DECIMAL(12,2)   NOT NULL,
    currency_code               NVARCHAR(8)     NULL,
    charge_period               TINYINT         NULL,               -- 1=Per booking, 4=Per month, 5=Per day
    credit_price                DECIMAL(12,2)   NULL,               -- price in credits (if credit-based)
    fixed_cost_price            DECIMAL(12,2)   NULL,               -- flat fee regardless of duration
    fixed_cost_length_minutes   INT             NULL,               -- duration the fixed cost covers
    maximum_price               DECIMAL(12,2)   NULL,               -- price cap for dynamic/long bookings
    min_length_minutes          INT             NULL,               -- minimum booking duration
    max_length_minutes          INT             NULL,               -- maximum booking duration

    -- Flags
    is_default_price            BIT             NOT NULL DEFAULT 1,
    is_printing_credit          BIT             NOT NULL DEFAULT 0,
    only_for_contacts           BIT             NOT NULL DEFAULT 0,
    only_for_members            BIT             NOT NULL DEFAULT 0,
    apply_charge_to_visitors    BIT             NOT NULL DEFAULT 0,
    use_per_night_pricing       BIT             NOT NULL DEFAULT 0,

    -- Dynamic pricing
    last_minute_adjustment_type TINYINT         NULL,               -- 0=None, 3=Percentage

    -- Availability window (if applicable)
    apply_from                  DATETIME2       NULL,
    apply_to                    DATETIME2       NULL,

    -- Resource type link (soft link → silver.nexudus_products.resource_type_name)
    -- Comma-separated resource type names this service applies to.
    -- NULL means it applies to all resource types at the location.
    resource_type_names         NVARCHAR(MAX)   NULL,               -- e.g. "4 person meeting room (AT),5 person meeting room (AT)"

    -- Financial
    tax_rate_id                 BIGINT          NULL,
    reduced_tax_rate_id         BIGINT          NULL,
    exempt_tax_rate_id          BIGINT          NULL,
    financial_account_id        BIGINT          NULL,

    -- Audit
    updated_by                  NVARCHAR(512)   NULL,

    -- Nexudus timestamps
    created_on                  DATETIME2       NULL,
    updated_on                  DATETIME2       NULL,

    -- Pipeline timestamps
    first_seen_at               DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_extra_services_location   ON silver.nexudus_extra_services (location_source_id);
CREATE INDEX ix_silver_nexudus_extra_services_price      ON silver.nexudus_extra_services (price, currency_code);
CREATE INDEX ix_silver_nexudus_extra_services_default    ON silver.nexudus_extra_services (is_default_price);
GO
