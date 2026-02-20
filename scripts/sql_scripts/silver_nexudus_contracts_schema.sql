-- ============================================================
-- silver_nexudus_contracts_schema.sql
--
-- One table: silver.nexudus_contracts
-- Source: bronze.nexudus_contracts (raw CoworkerContract JSON)
--
-- Upsert key: source_id (Nexudus CoworkerContract Id)
-- Relationship: location_source_id → silver.nexudus_locations.source_id
--               floor_plan_desk_ids → silver.nexudus_products.source_id (comma-separated)
-- ============================================================

DROP TABLE IF EXISTS silver.nexudus_contracts;
GO

CREATE TABLE silver.nexudus_contracts (
    id                          BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id                   BIGINT          NOT NULL,
    CONSTRAINT uq_silver_nexudus_contracts_source_id UNIQUE (source_id),
    unique_id                   NVARCHAR(64)    NULL,               -- Nexudus UniqueId (GUID)

    -- Traceability
    bronze_id                   BIGINT          NULL,
    sync_run_id                 UNIQUEIDENTIFIER NULL,

    -- Status
    active                      BIT             NOT NULL DEFAULT 1,
    cancelled                   BIT             NOT NULL DEFAULT 0,
    main_contract               BIT             NOT NULL DEFAULT 1,
    in_paused_period            BIT             NOT NULL DEFAULT 0,

    -- Coworker
    coworker_id                 BIGINT          NULL,
    coworker_name               NVARCHAR(512)   NULL,
    coworker_email              NVARCHAR(512)   NULL,
    coworker_company            NVARCHAR(512)   NULL,
    coworker_billing_name       NVARCHAR(512)   NULL,
    coworker_type               TINYINT         NULL,               -- 1=Individual, 2=Company
    coworker_active             BIT             NULL,

    -- Issuing location (soft FK → silver.nexudus_locations.source_id)
    location_source_id          BIGINT          NULL,               -- IssuedById
    location_name               NVARCHAR(512)   NULL,               -- IssuedByName

    -- Tariff / plan
    tariff_id                   BIGINT          NULL,
    tariff_name                 NVARCHAR(512)   NULL,
    tariff_price                DECIMAL(12,2)   NULL,
    currency_code               NVARCHAR(8)     NULL,
    next_tariff_id              BIGINT          NULL,
    next_tariff_name            NVARCHAR(512)   NULL,

    -- Linked products (comma-separated FloorPlanDesk Ids)
    -- Soft link → silver.nexudus_products.source_id
    floor_plan_desk_ids         NVARCHAR(1024)  NULL,               -- e.g. "1415402817" or "1415402776,1415468319"
    floor_plan_desk_names       NVARCHAR(MAX)   NULL,

    -- Pricing
    price                       DECIMAL(12,2)   NULL,               -- can be negative (discounts)
    price_with_products         DECIMAL(12,2)   NULL,
    unit_price                  DECIMAL(12,2)   NULL,
    quantity                    INT             NULL,
    billing_day                 TINYINT         NULL,               -- day of month billing runs

    -- Billing flags
    apply_pro_rating            BIT             NULL,
    pro_rate_cancellation       BIT             NULL,
    include_signup_fee          BIT             NULL,
    cancellation_limit_days     INT             NULL,

    -- Key dates
    start_date                  DATETIME2       NULL,
    contract_term               DATETIME2       NULL,               -- end / expiry date
    renewal_date                DATETIME2       NULL,
    cancellation_date           DATETIME2       NULL,
    invoiced_period             DATETIME2       NULL,               -- last invoice period end

    -- Duration
    term_duration_months        INT             NULL,               -- TermDurationInMonths

    -- Audit
    notes                       NVARCHAR(MAX)   NULL,
    updated_by                  NVARCHAR(512)   NULL,

    -- Nexudus timestamps
    created_on                  DATETIME2       NULL,
    updated_on                  DATETIME2       NULL,

    -- Pipeline timestamps
    first_seen_at               DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_contracts_coworker  ON silver.nexudus_contracts (coworker_id);
CREATE INDEX ix_silver_nexudus_contracts_location  ON silver.nexudus_contracts (location_source_id);
CREATE INDEX ix_silver_nexudus_contracts_active    ON silver.nexudus_contracts (active, cancelled);
CREATE INDEX ix_silver_nexudus_contracts_tariff    ON silver.nexudus_contracts (tariff_id);
GO

--invoices, lineitems has each finance account with details membership fees, ... 
-- help desk MCP for operation team chatbot
-- discussion board 
-- xero 
--