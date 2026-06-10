-- ============================================================
-- nexudus_events_schema.sql  (2026-06-10)
--
-- Nexudus events: bronze + silver tables for
--   - calendar events   (GET /api/content/calendarevents)
--   - event attendees   (GET /api/content/eventattendees)
--   - event products    (GET /api/content/eventproducts)  = ticket types
--
-- Run once against the warehouse DB.
--
-- Relationships:
--   silver.nexudus_calendar_events.location_source_id
--       -> silver.nexudus_locations.source_id            (BusinessId)
--   silver.nexudus_calendar_events.resource_source_id
--       -> silver.nexudus_resources.source_id            (optional)
--   silver.nexudus_event_attendees.calendar_event_source_id
--       -> silver.nexudus_calendar_events.source_id
--   silver.nexudus_event_attendees.coworker_source_id
--       -> silver.nexudus_coworkers.source_id            (NULL for external guests)
--   silver.nexudus_event_attendees.event_product_source_id
--       -> silver.nexudus_event_products.source_id
--   silver.nexudus_event_attendees.coworker_invoice_source_id
--       -> silver.nexudus_coworker_invoices.source_id    (NULL until invoiced)
--   silver.nexudus_event_products.calendar_event_source_id
--       -> silver.nexudus_calendar_events.source_id
--   silver.nexudus_event_products.location_source_id
--       -> silver.nexudus_locations.source_id
--       (derived from the parent event — the raw payload has no BusinessId)
--
-- Soft delete: is_deleted/deleted_at maintained weekly by
-- nexudus_silver_reconcile. Downstream reads MUST filter is_deleted = 0.
-- ============================================================

-- ── Bronze ──────────────────────────────────────────────────

CREATE TABLE bronze.nexudus_calendar_events (
    id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id     UNIQUEIDENTIFIER NOT NULL,
    source_id       BIGINT          NOT NULL,       -- CalendarEvent Id
    location_id     BIGINT          NULL,           -- BusinessId (denorm)
    raw_json        NVARCHAR(MAX)   NOT NULL,
    payload_hash    CHAR(64)        NULL,           -- SHA-256 of raw_json for change detection
    synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_calendar_events_source_id ON bronze.nexudus_calendar_events (source_id);
CREATE INDEX ix_bronze_nexudus_calendar_events_location  ON bronze.nexudus_calendar_events (location_id);
CREATE INDEX ix_bronze_nexudus_calendar_events_sync_run  ON bronze.nexudus_calendar_events (sync_run_id);
CREATE INDEX ix_bronze_nexudus_calendar_events_synced_at ON bronze.nexudus_calendar_events (synced_at);
GO

CREATE TABLE bronze.nexudus_event_attendees (
    id                  BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id         UNIQUEIDENTIFIER NOT NULL,
    source_id           BIGINT          NOT NULL,   -- EventAttendee Id
    location_id         BIGINT          NULL,       -- BusinessId (denorm)
    calendar_event_id   BIGINT          NULL,       -- CalendarEventId (denorm)
    coworker_id         BIGINT          NULL,       -- CoworkerId (denorm, NULL for external guests)
    raw_json            NVARCHAR(MAX)   NOT NULL,
    payload_hash        CHAR(64)        NULL,       -- SHA-256 of raw_json for change detection
    synced_at           DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_event_attendees_source_id ON bronze.nexudus_event_attendees (source_id);
CREATE INDEX ix_bronze_nexudus_event_attendees_event     ON bronze.nexudus_event_attendees (calendar_event_id);
CREATE INDEX ix_bronze_nexudus_event_attendees_coworker  ON bronze.nexudus_event_attendees (coworker_id);
CREATE INDEX ix_bronze_nexudus_event_attendees_sync_run  ON bronze.nexudus_event_attendees (sync_run_id);
CREATE INDEX ix_bronze_nexudus_event_attendees_synced_at ON bronze.nexudus_event_attendees (synced_at);
GO

CREATE TABLE bronze.nexudus_event_products (
    id                  BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id         UNIQUEIDENTIFIER NOT NULL,
    source_id           BIGINT          NOT NULL,   -- EventProduct Id
    calendar_event_id   BIGINT          NULL,       -- CalendarEventId (denorm; payload has no BusinessId)
    raw_json            NVARCHAR(MAX)   NOT NULL,
    payload_hash        CHAR(64)        NULL,       -- SHA-256 of raw_json for change detection
    synced_at           DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_event_products_source_id ON bronze.nexudus_event_products (source_id);
CREATE INDEX ix_bronze_nexudus_event_products_event     ON bronze.nexudus_event_products (calendar_event_id);
CREATE INDEX ix_bronze_nexudus_event_products_sync_run  ON bronze.nexudus_event_products (sync_run_id);
CREATE INDEX ix_bronze_nexudus_event_products_synced_at ON bronze.nexudus_event_products (synced_at);
GO

-- ── Silver ──────────────────────────────────────────────────

CREATE TABLE silver.nexudus_calendar_events (
    id                      BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id               BIGINT          NOT NULL,
    CONSTRAINT uq_silver_nexudus_calendar_events_source_id UNIQUE (source_id),
    unique_id               NVARCHAR(64)    NULL,               -- Nexudus UniqueId (GUID)

    -- Traceability
    bronze_id               BIGINT          NULL,
    sync_run_id             UNIQUEIDENTIFIER NULL,

    -- Location (soft FK -> silver.nexudus_locations.source_id)
    location_source_id      BIGINT          NOT NULL,           -- BusinessId

    -- Identity
    name                    NVARCHAR(512)   NOT NULL,
    slug                    NVARCHAR(512)   NULL,
    short_description       NVARCHAR(MAX)   NULL,
    long_description        NVARCHAR(MAX)   NULL,

    -- Venue / links
    venue_name              NVARCHAR(512)   NULL,               -- "Location" field (free text)
    venue_address           NVARCHAR(1000)  NULL,
    web_address             NVARCHAR(1000)  NULL,
    tickets_page            NVARCHAR(1000)  NULL,
    facebook_page           NVARCHAR(1000)  NULL,
    host_full_name          NVARCHAR(512)   NULL,

    -- Optional booked resource (soft FK -> silver.nexudus_resources.source_id)
    resource_source_id      BIGINT          NULL,

    -- Schedule
    start_date              DATETIME2       NULL,
    end_date                DATETIME2       NULL,
    publish_date            DATETIME2       NULL,

    -- Audience / visibility flags
    only_for_contacts       BIT             NOT NULL DEFAULT 0,
    only_for_members        BIT             NOT NULL DEFAULT 0,
    allow_comments          BIT             NOT NULL DEFAULT 0,
    enable_wait_list        BIT             NOT NULL DEFAULT 0,
    show_event_attendees    BIT             NOT NULL DEFAULT 0,
    show_in_home_page       BIT             NOT NULL DEFAULT 0,
    show_in_home_banner     BIT             NOT NULL DEFAULT 0,

    -- Recurrence
    repeat_event            BIT             NOT NULL DEFAULT 0,
    repeats                 INT             NULL,
    repeat_every            INT             NULL,
    repeat_until            DATETIME2       NULL,
    repeat_series_unique_id NVARCHAR(64)    NULL,

    -- Registration form
    has_event_form          BIT             NOT NULL DEFAULT 0,
    form_page_id            BIGINT          NULL,
    form_page_name          NVARCHAR(512)   NULL,

    -- Tickets / media
    ticket_notes            NVARCHAR(MAX)   NULL,
    large_logo_file_name    NVARCHAR(512)   NULL,
    small_logo_file_name    NVARCHAR(512)   NULL,

    -- Audit
    updated_by              NVARCHAR(512)   NULL,
    created_on              DATETIME2       NULL,
    updated_on              DATETIME2       NULL,

    -- Soft delete (maintained by nexudus_silver_reconcile, weekly)
    is_deleted              BIT             NOT NULL DEFAULT 0,
    deleted_at              DATETIME2       NULL,

    -- Pipeline timestamps
    first_seen_at           DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_calendar_events_location ON silver.nexudus_calendar_events (location_source_id);
CREATE INDEX ix_silver_nexudus_calendar_events_start    ON silver.nexudus_calendar_events (start_date);
CREATE INDEX ix_silver_nexudus_calendar_events_deleted  ON silver.nexudus_calendar_events (is_deleted);
GO

CREATE TABLE silver.nexudus_event_attendees (
    id                          BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id                   BIGINT          NOT NULL,
    CONSTRAINT uq_silver_nexudus_event_attendees_source_id UNIQUE (source_id),
    unique_id                   NVARCHAR(64)    NULL,           -- Nexudus UniqueId (GUID)

    -- Traceability
    bronze_id                   BIGINT          NULL,
    sync_run_id                 UNIQUEIDENTIFIER NULL,

    -- Event (soft FK -> silver.nexudus_calendar_events.source_id)
    calendar_event_source_id    BIGINT          NOT NULL,
    calendar_event_name         NVARCHAR(512)   NULL,           -- denorm convenience

    -- Location (soft FK -> silver.nexudus_locations.source_id)
    location_source_id          BIGINT          NULL,           -- BusinessId

    -- Attendee identity (coworker_source_id NULL for external guests)
    coworker_source_id          BIGINT          NULL,           -- -> silver.nexudus_coworkers.source_id
    coworker_full_name          NVARCHAR(512)   NULL,
    full_name                   NVARCHAR(512)   NULL,
    email                       NVARCHAR(512)   NULL,
    attendee_code               NVARCHAR(64)    NULL,           -- check-in code

    -- Check-in
    checked_in                  BIT             NOT NULL DEFAULT 0,
    checked_in_date             DATETIME2       NULL,

    -- Ticket (soft FK -> silver.nexudus_event_products.source_id)
    event_product_source_id     BIGINT          NULL,
    event_product_name          NVARCHAR(512)   NULL,
    event_product_price         DECIMAL(12,2)   NULL,
    event_product_currency_code NVARCHAR(8)     NULL,

    -- Billing (soft FK -> silver.nexudus_coworker_invoices.source_id)
    invoiced                    BIT             NOT NULL DEFAULT 0,
    coworker_invoice_source_id  BIGINT          NULL,
    coworker_invoice_number     NVARCHAR(128)   NULL,
    coworker_invoice_paid       BIT             NOT NULL DEFAULT 0,
    due_date                    DATETIME2       NULL,
    purchase_order              NVARCHAR(256)   NULL,

    -- Audit
    updated_by                  NVARCHAR(512)   NULL,
    created_on                  DATETIME2       NULL,
    updated_on                  DATETIME2       NULL,

    -- Soft delete (maintained by nexudus_silver_reconcile, weekly)
    is_deleted                  BIT             NOT NULL DEFAULT 0,
    deleted_at                  DATETIME2       NULL,

    -- Pipeline timestamps
    first_seen_at               DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_event_attendees_event    ON silver.nexudus_event_attendees (calendar_event_source_id);
CREATE INDEX ix_silver_nexudus_event_attendees_location ON silver.nexudus_event_attendees (location_source_id);
CREATE INDEX ix_silver_nexudus_event_attendees_coworker ON silver.nexudus_event_attendees (coworker_source_id);
CREATE INDEX ix_silver_nexudus_event_attendees_product  ON silver.nexudus_event_attendees (event_product_source_id);
CREATE INDEX ix_silver_nexudus_event_attendees_email    ON silver.nexudus_event_attendees (email);
CREATE INDEX ix_silver_nexudus_event_attendees_deleted  ON silver.nexudus_event_attendees (is_deleted);
GO

CREATE TABLE silver.nexudus_event_products (
    id                          BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id                   BIGINT          NOT NULL,
    CONSTRAINT uq_silver_nexudus_event_products_source_id UNIQUE (source_id),
    unique_id                   NVARCHAR(64)    NULL,           -- Nexudus UniqueId (GUID)

    -- Traceability
    bronze_id                   BIGINT          NULL,
    sync_run_id                 UNIQUEIDENTIFIER NULL,

    -- Event (soft FK -> silver.nexudus_calendar_events.source_id)
    calendar_event_source_id    BIGINT          NOT NULL,

    -- Location, inherited from the parent event
    -- (soft FK -> silver.nexudus_locations.source_id)
    location_source_id          BIGINT          NULL,

    -- Identity
    name                        NVARCHAR(512)   NOT NULL,
    description                 NVARCHAR(MAX)   NULL,

    -- Pricing
    price                       DECIMAL(12,2)   NOT NULL,
    currency_code               NVARCHAR(8)     NULL,

    -- Capacity / sales
    allocation                  INT             NULL,           -- total tickets available
    sales                       INT             NULL,           -- tickets sold so far
    max_tickets_per_attendee    INT             NULL,

    -- Sale window
    start_date                  DATETIME2       NULL,
    end_date                    DATETIME2       NULL,

    -- Flags
    only_for_contacts           BIT             NOT NULL DEFAULT 0,
    only_for_members            BIT             NOT NULL DEFAULT 0,
    visible                     BIT             NOT NULL DEFAULT 0,
    display_order               INT             NULL,

    -- Tickets
    ticket_notes                NVARCHAR(MAX)   NULL,

    -- Financial
    tax_rate_id                 BIGINT          NULL,
    financial_account_id        BIGINT          NULL,

    -- Audit
    updated_by                  NVARCHAR(512)   NULL,
    created_on                  DATETIME2       NULL,
    updated_on                  DATETIME2       NULL,

    -- Soft delete (maintained by nexudus_silver_reconcile, weekly)
    is_deleted                  BIT             NOT NULL DEFAULT 0,
    deleted_at                  DATETIME2       NULL,

    -- Pipeline timestamps
    first_seen_at               DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_event_products_event    ON silver.nexudus_event_products (calendar_event_source_id);
CREATE INDEX ix_silver_nexudus_event_products_location ON silver.nexudus_event_products (location_source_id);
CREATE INDEX ix_silver_nexudus_event_products_deleted  ON silver.nexudus_event_products (is_deleted);
GO
