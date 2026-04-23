-- =============================================================================
-- ava.product_availability — Denormalized availability table for Ava chatbot
-- =============================================================================
-- Purpose : Single flat table queried by Ava to answer questions about
--           inventory availability, pricing and capacity across all locations.
-- Refreshed: Daily at 03:00 UTC via ava.sp_refresh_product_availability
--            (called from Azure Function functions/ava_refresh.py)
--
-- Item categories stored in this table:
--   hot_desk       — item_type 3, always available, capacity 1
--   dedicated_desk — item_type 2, always available, capacity 1
--   private_office — item_type 1, availability derived from contracts
--   meeting_room   — sourced from extra_services + resources, per booking
--   day_pass       — sourced from extra_services (resource_type_names LIKE 'hot desk%')
-- =============================================================================

-- Create schema (safe: no-op if already exists)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ava')
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA ava';
END
GO

-- Drop and recreate (schema evolution handled by SP refresh, not table alter)
IF OBJECT_ID('ava.product_availability', 'U') IS NOT NULL
    DROP TABLE ava.product_availability;
GO

CREATE TABLE ava.product_availability (
    id                      BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- -------------------------------------------------------------------------
    -- Location context (denormalised for Ava — avoids joins at query time)
    -- -------------------------------------------------------------------------
    location_source_id      BIGINT          NOT NULL,       -- silver.nexudus_locations.source_id
    location_name           NVARCHAR(512)   NOT NULL,
    city                    NVARCHAR(255)   NULL,
    country_name            NVARCHAR(128)   NULL,

    -- -------------------------------------------------------------------------
    -- Item classification
    -- -------------------------------------------------------------------------
    item_category           NVARCHAR(32)    NOT NULL,
        -- 'hot_desk' | 'dedicated_desk' | 'private_office' | 'meeting_room' | 'day_pass'

    -- -------------------------------------------------------------------------
    -- Source traceability (one or more populated depending on category)
    -- -------------------------------------------------------------------------
    product_source_id       BIGINT          NULL,   -- silver.nexudus_products.source_id
                                                    -- populated for: hot_desk, dedicated_desk, private_office
    resource_source_id      BIGINT          NULL,   -- silver.nexudus_resources.source_id
                                                    -- populated for: meeting_room (when join succeeds)
    extra_service_source_id BIGINT          NULL,   -- silver.nexudus_extra_services.source_id
                                                    -- populated for: meeting_room (min-price entry), day_pass

    -- -------------------------------------------------------------------------
    -- Descriptive
    -- -------------------------------------------------------------------------
    item_name               NVARCHAR(512)   NOT NULL,   -- product/resource/service name

    -- -------------------------------------------------------------------------
    -- Physical attributes
    -- -------------------------------------------------------------------------
    capacity                INT             NULL,
        -- 1                        for hot_desk, dedicated_desk, day_pass
        -- products.capacity        for private_office
        -- resources.allocation     for meeting_room (NULL if resource join misses)

    -- -------------------------------------------------------------------------
    -- Pricing
    -- -------------------------------------------------------------------------
    price                   DECIMAL(12,2)   NULL,
        -- Standard / member price. For meeting_room: MIN(price) across tiers.
    external_price          DECIMAL(12,2)   NULL,
        -- Non-member price. Populated for meeting_room only (MAX(price) across tiers).
        -- NULL for all other categories.
    currency_code           NVARCHAR(8)     NULL,
    charge_period           NVARCHAR(32)    NULL,
        -- 'per_month'    for hot_desk, dedicated_desk, private_office
        -- 'per_booking'  for meeting_room
        -- 'per_day'      for day_pass

    -- -------------------------------------------------------------------------
    -- Availability  (rich — designed for Ava to answer any availability question)
    -- Only private_office rows have non-trivial values here.
    -- All other categories always have: is_available=1, everything else NULL.
    -- -------------------------------------------------------------------------
    is_available            BIT             NOT NULL DEFAULT 1,
        -- 1 = available right now
        -- 0 = currently occupied (private_office only)

    available_from          DATE            NULL,
        -- private_office only. The FIRST date a new customer can actually move in.
        --
        -- If occupied + consecutive chain immediately follows → chain_occupied_until
        --   (the chain ends there; NULL = chain is indefinite, no opening visible)
        -- If occupied + real gap exists before next contract  → occupied_until
        --   (office frees up at occupied_until, then gets taken again from chain_start)
        -- If occupied + no future contracts                   → occupied_until
        -- If currently available                              → NULL (available RIGHT NOW)

    occupied_until          DATE            NULL,
        -- private_office only. End of the CURRENT active contract.
        -- COALESCE(cancellation_date, contract_term) of the active contract.
        -- NULL = rolling monthly contract with no fixed end.

    next_occupied_from      DATE            NULL,
        -- private_office only.
        -- For currently FREE offices: the start date of the next upcoming contract.
        -- Combined with chain_occupied_until, tells Ava the full upcoming window.
        -- NULL = no upcoming contracts.

    chain_occupied_until    DATE            NULL,
        -- private_office only.
        -- The end date of the consecutive chain of future contracts starting at
        -- next_occupied_from (for free offices) or starting at/before occupied_until
        -- (for occupied offices with immediately following contracts).
        -- NULL = the chain has no known end (last contract in chain is rolling/indefinite).
        --
        -- Example: Free office → reserved Mar 31 → Contract A ends Jun 30
        --          → Contract B starts Jun 30 ends Mar 30, 2027
        --          → chain_occupied_until = 2027-03-30
        --          Ava can say: "Available now, occupied from Mar 31 through Mar 2027"

    availability_notes      NVARCHAR(512)   NULL,
        -- Human-readable summary for Ava, covering all scenarios:
        --
        -- Free offices:
        --   'Available'
        --   'Available now – reserved from 2026-03-31 through 2027-03-30'
        --   'Available now – reserved from 2026-03-31 (long-term occupancy follows)'
        --
        -- Occupied offices:
        --   'Occupied – active monthly contract, no fixed end date'
        --   'Occupied (monthly renewal); re-occupied from 2026-05-15'
        --   'Occupied until 2026-04-30'
        --   'Occupied until 2026-04-30; re-occupied through 2026-09-30'
        --   'Occupied until 2026-04-30; immediately re-occupied with no known end date'
        --   'Occupied until 2026-04-30; briefly available, then re-occupied from 2026-05-15 through 2026-09-30'
        --   'Occupied until 2026-04-30; briefly available, then re-occupied from 2026-05-15'
        --
        -- Non-private-office:
        --   'Always available'

    -- -------------------------------------------------------------------------
    -- Freshness
    -- -------------------------------------------------------------------------
    last_refreshed_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Indexes for Ava query patterns
CREATE INDEX ix_ava_product_avail_location
    ON ava.product_availability (location_source_id);

CREATE INDEX ix_ava_product_avail_category
    ON ava.product_availability (item_category);

CREATE INDEX ix_ava_product_avail_available
    ON ava.product_availability (is_available);

CREATE INDEX ix_ava_product_avail_location_category
    ON ava.product_availability (location_source_id, item_category);
GO
