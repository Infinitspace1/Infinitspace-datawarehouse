-- ============================================================
-- eventbrite_events_schema.sql  (2026-06-10)
--
-- Eventbrite events: bronze + silver tables.
-- Source: Eventbrite API v3
--   GET /v3/organizations/{id}/events/?status=all
--       &expand=venue,ticket_availability,organizer,format,category
--
-- Synced daily by functions/eventbrite_sync.py (source_name = 'eventbrite'),
-- gated behind ENABLE_EVENTBRITE_FUNCTIONS + EVENTBRITE_PRIVATE_TOKEN.
-- Run this script once against the warehouse DB.
--
-- Design notes:
--   - source_id is the Eventbrite event id (numeric, but delivered as a
--     string by the v3 API) -> NVARCHAR(64).
--   - Bronze is latest-payload-wins (UNIQUE source_id, overwrite MERGE)
--     with payload_hash change detection, like bronze.competence_*.
--   - Silver is fully FLAT — no JSON columns (policy). Venue + ticket
--     availability are flattened to columns; the complete raw payload
--     always remains in bronze.eventbrite_events.raw_json.
--   - is_deleted/deleted_at maintained by the reconcile step embedded in
--     eventbrite_sync. Downstream reads MUST filter WHERE is_deleted = 0.
-- ============================================================

-- ── Bronze ──────────────────────────────────────────────────

CREATE TABLE bronze.eventbrite_events (
    id               BIGINT           IDENTITY(1,1) NOT NULL,
    sync_run_id      UNIQUEIDENTIFIER NOT NULL,
    source_id        NVARCHAR(64)     NOT NULL,      -- Eventbrite event id
    organization_id  NVARCHAR(64)     NULL,          -- denorm for filtering
    event_status     NVARCHAR(64)     NULL,          -- denorm (draft/live/ended/canceled/...)
    raw_json         NVARCHAR(MAX)    NOT NULL,
    payload_hash     CHAR(64)         NULL,          -- SHA-256 of raw_json for change detection
    synced_at        DATETIME2        NOT NULL CONSTRAINT df_bronze_eventbrite_events_synced_at DEFAULT GETUTCDATE(),
    CONSTRAINT pk_bronze_eventbrite_events PRIMARY KEY (id),
    CONSTRAINT uq_bronze_eventbrite_events_source_id UNIQUE (source_id)
);

CREATE INDEX ix_bronze_eventbrite_events_org       ON bronze.eventbrite_events (organization_id);
CREATE INDEX ix_bronze_eventbrite_events_sync_run  ON bronze.eventbrite_events (sync_run_id);
CREATE INDEX ix_bronze_eventbrite_events_synced_at ON bronze.eventbrite_events (synced_at);
GO

-- ── Silver ──────────────────────────────────────────────────

CREATE TABLE silver.eventbrite_events (
    source_id                       NVARCHAR(64)     NOT NULL,  -- Eventbrite event id
    bronze_id                       BIGINT           NULL,
    sync_run_id                     UNIQUEIDENTIFIER NULL,

    -- Organization / organizer
    organization_id                 NVARCHAR(64)     NULL,
    organizer_id                    NVARCHAR(64)     NULL,
    organizer_name                  NVARCHAR(512)    NULL,

    -- Identity
    name                            NVARCHAR(512)    NOT NULL,
    summary                         NVARCHAR(MAX)    NULL,
    description_text                NVARCHAR(MAX)    NULL,
    description_html                NVARCHAR(MAX)    NULL,
    url                             NVARCHAR(1000)   NULL,
    status                          NVARCHAR(64)     NULL,      -- draft/live/started/ended/completed/canceled
    currency                        NVARCHAR(8)      NULL,

    -- Schedule
    start_utc                       DATETIME2        NULL,
    start_local                     DATETIME2        NULL,
    end_utc                         DATETIME2        NULL,
    end_local                       DATETIME2        NULL,
    timezone                        NVARCHAR(64)     NULL,

    -- Lifecycle
    created                         DATETIME2        NULL,
    changed                         DATETIME2        NULL,
    published                       DATETIME2        NULL,

    -- Flags
    online_event                    BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_online DEFAULT 0,
    listed                          BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_listed DEFAULT 0,
    shareable                       BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_shareable DEFAULT 0,
    is_free                         BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_free DEFAULT 0,
    is_series                       BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_series DEFAULT 0,
    is_series_parent                BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_series_parent DEFAULT 0,
    hide_start_date                 BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_hide_start DEFAULT 0,
    hide_end_date                   BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_hide_end DEFAULT 0,

    -- Capacity
    capacity                        INT              NULL,
    capacity_is_custom              BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_cap_custom DEFAULT 0,

    -- Series link
    series_id                       NVARCHAR(64)     NULL,

    -- Classification
    format_id                       NVARCHAR(64)     NULL,
    format_name                     NVARCHAR(256)    NULL,
    category_id                     NVARCHAR(64)     NULL,
    category_name                   NVARCHAR(256)    NULL,
    subcategory_id                  NVARCHAR(64)     NULL,

    -- Venue (from expand=venue, fully flattened)
    venue_id                        NVARCHAR(64)     NULL,
    venue_resource_uri              NVARCHAR(1000)   NULL,
    venue_name                      NVARCHAR(512)    NULL,
    venue_address                   NVARCHAR(1000)   NULL,      -- localized display, e.g. "207 Old Street, London, EC1V 9NR"
    venue_address_1                 NVARCHAR(500)    NULL,
    venue_address_2                 NVARCHAR(500)    NULL,
    venue_city                      NVARCHAR(200)    NULL,
    venue_region                    NVARCHAR(200)    NULL,
    venue_postal_code               NVARCHAR(50)     NULL,
    venue_country                   NVARCHAR(8)      NULL,      -- ISO2
    venue_address_latitude          FLOAT            NULL,
    venue_address_longitude         FLOAT            NULL,
    venue_localized_area            NVARCHAR(500)    NULL,
    venue_multi_line_address        NVARCHAR(1000)   NULL,
    venue_latitude                  FLOAT            NULL,
    venue_longitude                 FLOAT            NULL,
    venue_capacity                  INT              NULL,
    venue_age_restriction           NVARCHAR(64)     NULL,

    -- Tickets (from expand=ticket_availability, fully flattened)
    has_available_tickets           BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_avail DEFAULT 0,
    is_sold_out                     BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_soldout DEFAULT 0,
    waitlist_available              BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_waitlist DEFAULT 0,
    minimum_ticket_price            DECIMAL(12,2)    NULL,
    minimum_ticket_price_display    NVARCHAR(64)     NULL,
    minimum_ticket_price_currency   NVARCHAR(8)      NULL,
    minimum_ticket_price_minor      INT              NULL,
    maximum_ticket_price            DECIMAL(12,2)    NULL,
    maximum_ticket_price_display    NVARCHAR(64)     NULL,
    maximum_ticket_price_currency   NVARCHAR(8)      NULL,
    maximum_ticket_price_minor      INT              NULL,
    ticket_currency                 NVARCHAR(8)      NULL,
    sales_start_utc                 DATETIME2        NULL,
    sales_start_local               DATETIME2        NULL,
    sales_start_timezone            NVARCHAR(64)     NULL,

    -- Media
    logo_url                        NVARCHAR(1000)   NULL,

    -- Soft delete (maintained by the reconcile step in eventbrite_sync)
    is_deleted                      BIT              NOT NULL CONSTRAINT df_silver_eventbrite_events_is_deleted DEFAULT 0,
    deleted_at                      DATETIME2        NULL,

    -- Pipeline timestamps
    first_seen_at                   DATETIME2        NOT NULL CONSTRAINT df_silver_eventbrite_events_first_seen DEFAULT GETUTCDATE(),
    last_synced_at                  DATETIME2        NOT NULL CONSTRAINT df_silver_eventbrite_events_synced_at DEFAULT GETUTCDATE(),

    CONSTRAINT pk_silver_eventbrite_events PRIMARY KEY (source_id)
);

CREATE INDEX ix_silver_eventbrite_events_org      ON silver.eventbrite_events (organization_id);
CREATE INDEX ix_silver_eventbrite_events_status   ON silver.eventbrite_events (status);
CREATE INDEX ix_silver_eventbrite_events_start    ON silver.eventbrite_events (start_utc);
CREATE INDEX ix_silver_eventbrite_events_city     ON silver.eventbrite_events (venue_city);
CREATE INDEX ix_silver_eventbrite_events_deleted  ON silver.eventbrite_events (is_deleted);
GO
