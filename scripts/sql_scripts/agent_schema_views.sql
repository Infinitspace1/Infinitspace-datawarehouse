-- =============================================================================
-- agent schema — the published, agent-readable surface of the warehouse
-- =============================================================================
-- Purpose : One schema that defines exactly what the Copilot Studio "SQL agent"
--           can see. The security model is a SINGLE grant:
--
--               GRANT SELECT ON SCHEMA::agent TO [sql-agent-reader];
--
--           Everything in this schema is readable by the agent. Nothing outside
--           it is. Ownership chaining does the rest — every schema in this DB is
--           owned by dbo with zero explicit object owners (verified 2026-08-20),
--           so a view here can read silver.*/ava.*/bronze.* while the agent holds
--           no permission at all on those base tables.
--
-- ADDING A VIEW LATER (the whole point of this design):
--   1. CREATE VIEW agent.vw_<domain>_<subject> AS ...
--   2. INSERT one row into agent.view_catalog describing it
--   No new GRANT. No Copilot Studio change. No redeploy.
--
-- HOUSE RULES for anything added here:
--   * Only the columns the agent needs — silver tables run 26-75 columns wide,
--     which is far more than an LLM handles well.
--   * Denormalized. Join to locations here so the agent never has to.
--   * WHERE is_deleted = 0 baked in, always.
--   * Currency or unit column alongside every amount.
--   * Booleans collapsed into a readable list (see the amenities pattern below)
--     rather than 21 separate bit columns.
--   * NO personal data unless a use case genuinely requires it. See the
--     "DELIBERATELY EXCLUDED" note at the foot of this file.
--
-- Apply with:
--   .\venv\Scripts\python.exe scripts\python_scripts\apply_schema_script.py ^
--       scripts/sql_scripts/agent_schema_views.sql
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'agent')
    EXEC sp_executesql N'CREATE SCHEMA agent';
GO


-- -----------------------------------------------------------------------------
-- Catalog — how the agent discovers what it can query.
-- Standing instruction to the agent: when you don't know where something lives,
-- SELECT * FROM agent.vw_catalog first.
-- -----------------------------------------------------------------------------
IF OBJECT_ID('agent.view_catalog', 'U') IS NULL
BEGIN
    CREATE TABLE agent.view_catalog (
        view_name    sysname        NOT NULL PRIMARY KEY,
        domain       nvarchar(50)   NOT NULL,   -- 'ava' | 'nexudus' | 'hubspot'
        description  nvarchar(1000) NOT NULL,   -- written FOR the LLM, not for humans
        key_columns  nvarchar(1000) NULL,
        refresh_note nvarchar(200)  NULL,
        row_estimate int            NULL,
        is_active    bit            NOT NULL CONSTRAINT df_view_catalog_active DEFAULT 1,
        updated_at   datetime2      NOT NULL CONSTRAINT df_view_catalog_updated DEFAULT SYSUTCDATETIME()
    );
END
GO

CREATE OR ALTER VIEW agent.vw_catalog
AS
SELECT view_name, domain, description, key_columns, refresh_note, row_estimate
FROM agent.view_catalog
WHERE is_active = 1;
GO


-- =============================================================================
-- AVA — thin wrappers over the existing ava.vw_agent_* views.
-- The logic (city derivation, price-on-request flags, MIN/MAX tier aggregation)
-- stays where it was built and tested; this schema is the published surface and
-- the access-control boundary, not a second copy of the logic.
-- =============================================================================

CREATE OR ALTER VIEW agent.vw_ava_location_summary
AS SELECT * FROM ava.vw_agent_location_summary;
GO

CREATE OR ALTER VIEW agent.vw_ava_office_availability
AS SELECT * FROM ava.vw_agent_office_availability;
GO

CREATE OR ALTER VIEW agent.vw_ava_faq
AS SELECT * FROM ava.vw_agent_faq;
GO

CREATE OR ALTER VIEW agent.vw_ava_plans
AS SELECT * FROM ava.vw_agent_plans;
GO


-- =============================================================================
-- NEXUDUS
-- =============================================================================

-- Every location we operate, with contact details and coordinates.
--
-- NOT exposed, because Nexudus has never had them filled in (0/11 as of
-- 2026-08-20): TownCity, Address, PostalCode, ShortIntro. Offering empty
-- columns just invites the agent to answer "I don't know" about an address.
-- If someone fills those fields in Nexudus, add the columns back here.
--
-- city is therefore DERIVED from the "City - District - Street" naming
-- convention, preferring the real column if it is ever populated. Locations
-- that don't follow the convention (e.g. "The Stack") return NULL — filter on
-- country_name for those, and see the data-quality note at the foot of this file.
CREATE OR ALTER VIEW agent.vw_nexudus_locations
AS
SELECT
    l.source_id                 AS location_id,
    l.name                      AS location_name,
    COALESCE(
        l.city,
        CASE WHEN CHARINDEX(' - ', l.name) > 0
             THEN LTRIM(RTRIM(LEFT(l.name, CHARINDEX(' - ', l.name) - 1)))
        END
    )                           AS city,
    l.country_name,
    l.latitude,
    l.longitude,
    l.phone,
    l.email                     AS contact_email,
    l.web_address,
    l.currency_code
FROM silver.nexudus_locations AS l
WHERE l.is_deleted = 0;
GO


-- Individual bookable workspaces (offices, desks) with amenities.
-- Occupant identity columns are deliberately NOT exposed — see foot of file.
CREATE OR ALTER VIEW agent.vw_nexudus_workspaces
AS
SELECT
    p.source_id                 AS workspace_id,
    p.product_type_label        AS workspace_type,
    p.location_source_id        AS location_id,
    p.location_name,
    p.name                      AS workspace_name,
    p.floor_plan_name           AS floor,
    p.capacity                  AS seats,
    p.size_sqm,
    CASE WHEN p.price = 0 THEN NULL ELSE p.price END AS price_per_month,
    CAST(CASE WHEN p.price IS NULL OR p.price = 0 THEN 1 ELSE 0 END AS bit) AS price_on_request,
    p.currency_code,
    p.is_available              AS is_bookable_now,
    p.available_from,
    NULLIF(CONCAT_WS(', ',
        CASE WHEN p.amenity_air_conditioning = 1 THEN 'air conditioning' END,
        CASE WHEN p.amenity_heating          = 1 THEN 'heating' END,
        CASE WHEN p.amenity_internet         = 1 THEN 'internet' END,
        CASE WHEN p.amenity_large_display    = 1 THEN 'large display' END,
        CASE WHEN p.amenity_natural_light    = 1 THEN 'natural light' END,
        CASE WHEN p.amenity_whiteboard       = 1 THEN 'whiteboard' END,
        CASE WHEN p.amenity_soundproof       = 1 THEN 'soundproof' END,
        CASE WHEN p.amenity_quiet_zone       = 1 THEN 'quiet zone' END,
        CASE WHEN p.amenity_tea_coffee       = 1 THEN 'tea and coffee' END,
        CASE WHEN p.amenity_security_lock    = 1 THEN 'security lock' END,
        CASE WHEN p.amenity_cctv             = 1 THEN 'CCTV' END,
        CASE WHEN p.amenity_catering         = 1 THEN 'catering' END,
        CASE WHEN p.amenity_conference_phone = 1 THEN 'conference phone' END,
        CASE WHEN p.amenity_projector        = 1 THEN 'projector' END,
        CASE WHEN p.amenity_standing_desk    = 1 THEN 'standing desk' END,
        CASE WHEN p.amenity_privacy_screen   = 1 THEN 'privacy screen' END,
        CASE WHEN p.amenity_wireless_charger = 1 THEN 'wireless charger' END
    ), '')                      AS amenities
FROM silver.nexudus_products AS p
WHERE p.is_deleted = 0;
GO


-- Bookable rooms (meeting rooms, phone booths, studios) with booking rules.
CREATE OR ALTER VIEW agent.vw_nexudus_rooms
AS
SELECT
    r.source_id                 AS room_id,
    r.location_source_id        AS location_id,
    r.location_name,
    r.name                      AS room_name,
    r.resource_type_name        AS room_type,
    r.allocation                AS seats,
    r.description,
    r.min_booking_minutes,
    r.max_booking_minutes,
    r.book_in_advance_minutes,
    r.requires_confirmation,
    r.only_for_members,
    NULLIF(CONCAT_WS(', ',
        CASE WHEN r.amenity_air_conditioning      = 1 THEN 'air conditioning' END,
        CASE WHEN r.amenity_catering              = 1 THEN 'catering' END,
        CASE WHEN r.amenity_conference_phone      = 1 THEN 'conference phone' END,
        CASE WHEN r.amenity_desktop_monitor       = 1 THEN 'desktop monitor' END,
        CASE WHEN r.amenity_display_screen        = 1 THEN 'display screen' END,
        CASE WHEN r.amenity_dual_display_screen   = 1 THEN 'dual display' END,
        CASE WHEN r.amenity_flip_chart            = 1 THEN 'flip chart' END,
        CASE WHEN r.amenity_internet              = 1 THEN 'internet' END,
        CASE WHEN r.amenity_natural_light         = 1 THEN 'natural light' END,
        CASE WHEN r.amenity_pa_system             = 1 THEN 'PA system' END,
        CASE WHEN r.amenity_projector             = 1 THEN 'projector' END,
        CASE WHEN r.amenity_quiet_zone            = 1 THEN 'quiet zone' END,
        CASE WHEN r.amenity_soundproof            = 1 THEN 'soundproof' END,
        CASE WHEN r.amenity_standing_desk         = 1 THEN 'standing desk' END,
        CASE WHEN r.amenity_tea_and_coffee        = 1 THEN 'tea and coffee' END,
        CASE WHEN r.amenity_video_conferencing    = 1 THEN 'video conferencing' END,
        CASE WHEN r.amenity_whiteboard            = 1 THEN 'whiteboard' END,
        CASE WHEN r.amenity_wireless_presentation = 1 THEN 'wireless presentation' END
    ), '')                      AS amenities
FROM silver.nexudus_resources AS r
WHERE r.is_deleted = 0
  AND r.is_archived = 0
  AND r.is_visible = 1;
GO


-- Membership plans (Nexudus "tariffs"). Location joined for readability.
CREATE OR ALTER VIEW agent.vw_nexudus_plans
AS
SELECT
    t.source_id                 AS plan_id,
    t.name                      AS plan_name,
    t.description,
    t.location_source_id        AS location_id,
    l.name                      AS location_name,
    l.city,
    l.country_name,
    t.price,
    t.currency_code,
    t.signup_fee,
    t.deposit,
    t.included_credit_amount,
    t.time_credit_minutes,
    t.term_duration_months,
    t.notice_period_days,
    t.is_team_plan
FROM silver.nexudus_tariffs AS t
LEFT JOIN silver.nexudus_locations AS l
       ON l.source_id = t.location_source_id
      AND l.is_deleted = 0
WHERE t.is_deleted = 0
  AND t.active = 1;
GO


-- Chargeable extras: room hire rate cards, printing, catering, parking...
CREATE OR ALTER VIEW agent.vw_nexudus_extra_services
AS
SELECT
    e.source_id                 AS service_id,
    e.name                      AS service_name,
    e.description,
    e.location_source_id        AS location_id,
    l.name                      AS location_name,
    l.city,
    e.price,
    e.currency_code,
    e.charge_period,
    e.min_length_minutes,
    e.max_length_minutes,
    e.resource_type_names       AS applies_to_room_types,
    e.only_for_members
FROM silver.nexudus_extra_services AS e
LEFT JOIN silver.nexudus_locations AS l
       ON l.source_id = e.location_source_id
      AND l.is_deleted = 0
WHERE e.is_deleted = 0;
GO


-- Community events. Attendee lists are NOT exposed — see foot of file.
CREATE OR ALTER VIEW agent.vw_nexudus_events
AS
SELECT
    ev.source_id                AS event_id,
    ev.name                     AS event_name,
    ev.short_description,
    ev.location_source_id       AS location_id,
    l.name                      AS location_name,
    l.city,
    ev.start_date,
    ev.end_date,
    ev.venue_name,
    ev.venue_address,
    ev.web_address,
    ev.tickets_page,
    ev.only_for_members,
    ev.host_full_name           AS host
FROM silver.nexudus_calendar_events AS ev
LEFT JOIN silver.nexudus_locations AS l
       ON l.source_id = ev.location_source_id
      AND l.is_deleted = 0
WHERE ev.is_deleted = 0;
GO


-- =============================================================================
-- HUBSPOT
-- =============================================================================

-- Marketing email campaigns and their performance. Aggregate counters only —
-- no recipient identities exist in this table at all.
CREATE OR ALTER VIEW agent.vw_hubspot_marketing_emails
AS
SELECT
    h.source_id                 AS email_id,
    h.name                      AS email_name,
    h.subject,
    h.subject_preview_text      AS preview_text,
    h.state,
    h.email_type,
    h.campaign_name,
    h.from_name,
    h.published_at,
    h.stat_sent                 AS sent,
    h.stat_delivered            AS delivered,
    h.stat_opens                AS opens,
    h.stat_clicks               AS clicks,
    h.stat_bounces              AS bounces,
    h.stat_unsubscribed         AS unsubscribed,
    h.stat_spam_reports         AS spam_reports,
    h.open_rate,
    h.click_rate,
    h.click_through_rate,
    h.delivered_rate,
    h.bounce_rate,
    h.unsubscribed_rate,
    h.opens_mobile,
    h.opens_computer,
    h.clicks_mobile,
    h.clicks_computer
FROM silver.hubspot_marketing_emails AS h
WHERE h.is_deleted = 0;
GO


-- =============================================================================
-- Seed / refresh the catalog
-- =============================================================================
MERGE agent.view_catalog AS tgt
USING (VALUES
 ('vw_ava_location_summary','ava',
  'One row per beyond location (8) with every headline price: hot desk and dedicated desk monthly rates, day pass member/non-member, meeting room price ranges, private office availability count and price range. Start here for almost any pricing or "what do you have" question.',
  'city, country_name, currency_code, hot_desk_price_per_month, private_office_available_now','rebuilt daily 03:00 UTC',8),

 ('vw_ava_office_availability','ava',
  'Every private office including occupied ones. Filter is_bookable_now = 1 before offering one. Use availability_notes verbatim for move-in timing; never compute dates yourself. price_on_request = 1 means price on request, never free.',
  'city, office_name, desks, price_per_month, price_on_request, is_bookable_now, availability_notes','rebuilt daily 03:00 UTC',451),

 ('vw_ava_faq','ava',
  'Curated reference answers, company-wide (scope = all_locations) and per location (scope = location). Broader than FAQs: also holds contact and enquiry emails, brochure links, photos, transportation, parking, print and meeting-room credits, membership inclusions, sustainability and USPs.',
  'scope, category, question, answer, city','maintained manually',106),

 ('vw_ava_plans','ava',
  'Ava-curated ancillary plans per location: parking, mailbox, business address registration, dedicated broadband, IP/SSID/VLAN, part-time access. Excludes desks and offices, which are priced in the other ava views.',
  'plan_name, plan_type, price, currency_code, city','rebuilt daily 03:00 UTC',87),

 ('vw_nexudus_locations','nexudus',
  'Every location we operate, with full postal address, coordinates, phone, contact email and website. Use for "where are you", address and contact-detail questions.',
  'location_id, location_name, city, country_name, address, phone, contact_email','synced daily 02:00 UTC',12),

 ('vw_nexudus_workspaces','nexudus',
  'Individual bookable workspaces (private offices and desks) with floor, seat count, floor area, monthly price and a readable amenities list. Raw Nexudus inventory — for customer-facing availability questions prefer vw_ava_office_availability, which adds occupancy timing.',
  'workspace_type, location_name, workspace_name, seats, size_sqm, price_per_month, is_bookable_now, amenities','synced daily 02:00 UTC',1117),

 ('vw_nexudus_rooms','nexudus',
  'Bookable rooms — meeting rooms, phone booths, studios — with seat count, booking rules (minimum and maximum duration, advance notice, confirmation required, members only) and a readable amenities list. Visible, non-archived rooms only.',
  'location_name, room_name, room_type, seats, min_booking_minutes, only_for_members, amenities','synced daily 02:00 UTC',98),

 ('vw_nexudus_plans','nexudus',
  'All active membership plans (Nexudus tariffs) with price, signup fee, deposit, included credit, contract term and notice period. Raw plan catalogue — broader than vw_ava_plans, which is the curated customer-facing subset.',
  'plan_name, location_name, city, price, currency_code, term_duration_months, notice_period_days',' synced daily 02:00 UTC',196),

 ('vw_nexudus_extra_services','nexudus',
  'Chargeable extras and rate cards: meeting-room hourly rates, printing, catering, parking, day passes. applies_to_room_types links a rate card to the room type it prices.',
  'service_name, location_name, price, currency_code, charge_period, applies_to_room_types','synced daily 02:00 UTC',143),

 ('vw_nexudus_events','nexudus',
  'Community events with schedule, venue, description, ticket page and whether they are members-only. Attendee lists are deliberately not available.',
  'event_name, location_name, city, start_date, end_date, only_for_members, host','synced daily 02:00 UTC',845),

 ('vw_hubspot_marketing_emails','hubspot',
  'Marketing email campaigns with subject, campaign, sender and full performance stats: sent, delivered, opens, clicks, bounces, unsubscribes plus derived rates and a mobile/desktop split. Aggregate counters only — no recipient identities.',
  'email_name, subject, campaign_name, published_at, sent, opens, open_rate, click_rate','synced daily 05:45 UTC',917)
) AS src (view_name, domain, description, key_columns, refresh_note, row_estimate)
ON tgt.view_name = src.view_name
WHEN MATCHED THEN UPDATE SET
    domain = src.domain, description = src.description, key_columns = src.key_columns,
    refresh_note = src.refresh_note, row_estimate = src.row_estimate,
    is_active = 1, updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT
    (view_name, domain, description, key_columns, refresh_note, row_estimate)
    VALUES (src.view_name, src.domain, src.description, src.key_columns,
            src.refresh_note, src.row_estimate);
GO


-- =============================================================================
-- DELIBERATELY EXCLUDED — do not add these without an explicit decision
-- =============================================================================
-- Because the grant is schema-wide, the privacy boundary is decided by WHICH
-- VIEWS EXIST HERE, not by anything in the agent's prompt. Creating a view over
-- any of the below immediately makes that data agent-readable.
--
--   silver.nexudus_coworkers            member names, emails, phone, billing details
--   silver.nexudus_coworker_invoices    who owes what, billing addresses
--   silver.nexudus_coworker_invoice_lines
--   silver.nexudus_event_attendees      who attended which event
--   silver.nexudus_products.coworker_name / coworker_company / coworker_email
--       ^ which company occupies which office. Excluded from vw_nexudus_workspaces
--         even though the rest of that table is exposed.
--   ava.messages, ava.website_conversations, ava.hubspot_conversations
--       ^ live customer chat transcripts. Never expose.
--   teamandy.*                          CRM leads and contact persons
--   silver.bamboohr_employees           staff records
--
-- Safe to add if wanted: gold.* dashboards (check each for customer names first),
-- silver.xero_profit_loss_accounts (financials — internal-only agents only),
-- silver.eventbrite_events, silver.competence_competitors (public competitor data).
-- =============================================================================

-- =============================================================================
-- KNOWN SOURCE DATA-QUALITY ISSUES (Nexudus data entry, not code)
-- Verified 2026-08-20. Both cause the agent to give wrong answers.
--
-- 1. TownCity is EMPTY for all 14 locations in Nexudus, so silver.city is NULL
--    everywhere. Every view here derives city by parsing the
--    "City - District - Street" name. "The Stack" doesn't follow that pattern,
--    so it has no city and will be missed by a city filter.
--    FIX: fill TownCity in Nexudus. The COALESCE in each view picks it up
--         automatically on the next sync — no code change needed.
--
-- 2. CountryName is WRONG for three locations. Nexudus itself returns
--    "Netherlands" for:
--         The Stack
--         Munich - Mitte, Kaufingerstrasse   (should be Germany)
--         London - Vauxhall, Old Paradise St (should be United Kingdom)
--    These were created without a country and defaulted to the account's.
--    Until fixed, an agent filtering country_name = 'Germany' misses Munich,
--    and one filtering 'Netherlands' wrongly returns Munich and Vauxhall.
--    FIX: set the country on those three locations in Nexudus.
--
-- Address, PostalCode and ShortIntro are likewise empty for all locations and
-- are deliberately not exposed (see vw_nexudus_locations).
-- =============================================================================


-- =============================================================================
-- INTERNAL-ONLY VIEWS
-- Added 2026-08-20 after confirming this agent is internal, Copilot-only.
-- These carry personal data (member names, emails, phones, billing contacts).
-- If the agent is ever exposed to customers, or connected to a parent agent
-- that is, DROP these two views first — the schema-wide grant means their mere
-- existence is what makes the data reachable.
-- =============================================================================

CREATE OR ALTER VIEW agent.vw_nexudus_members
AS
SELECT
    m.source_id                 AS member_id,
    m.full_name,
    m.email,
    m.company_name,
    m.team_name,
    m.coworker_type             AS member_type,
    m.location_source_id        AS location_id,
    m.location_name,
    m.mobile_phone,
    m.tariff_name               AS current_plan,
    m.next_tariff_name          AS next_plan,
    m.active                    AS is_active,
    m.archived                  AS is_archived,
    m.registration_date,
    m.start_date,
    m.renewal_date,
    m.cancellation_date
FROM silver.nexudus_coworkers AS m
WHERE m.is_deleted = 0;
GO


CREATE OR ALTER VIEW agent.vw_nexudus_invoices
AS
SELECT
    i.source_id                 AS invoice_id,
    i.invoice_number,
    i.coworker_id               AS member_id,
    i.coworker_name             AS member_name,
    i.coworker_company_name     AS company,
    i.location_source_id        AS location_id,
    i.location_name,
    i.description,
    i.currency_code,
    i.due_date,
    i.invoice_from_date,
    i.invoice_to_date,
    i.total_amount,
    i.paid_amount,
    i.due_amount,
    i.tax_amount,
    i.paid                      AS is_paid,
    i.is_due                    AS is_overdue,
    i.void                      AS is_void,
    i.draft                     AS is_draft,
    i.credit_note               AS is_credit_note,
    i.invoice_status,
    i.payment_state,
    i.sent_on,
    i.paid_on
FROM silver.nexudus_coworker_invoices AS i
WHERE i.is_deleted = 0;
GO


MERGE agent.view_catalog AS tgt
USING (VALUES
 ('vw_nexudus_members','nexudus',
  'INTERNAL ONLY - contains personal data. Every member and contact record: name, email, phone, company, team, home location, current and next plan and key dates. IMPORTANT: is_active does NOT identify members - it is 1 on 30419 of 30720 rows, including leads and past contacts. Use current_plan IS NOT NULL to find people actually on a membership plan (about 6250). Most rows are leads with no plan.',
  'full_name, email, company_name, location_name, current_plan (NOT NULL = on a plan), cancellation_date','synced daily 02:00 UTC',30720),

 ('vw_nexudus_invoices','nexudus',
  'INTERNAL ONLY - contains customer billing data. Every invoice with number, member and company, location, period covered, currency, total/paid/outstanding amounts, tax, and status flags. For outstanding balances use due_amount > 0 with is_paid = 0, is_void = 0 and is_draft = 0. is_credit_note = 1 marks credit notes, which carry negative value.',
  'invoice_number, member_name, company, location_name, due_date, total_amount, due_amount, is_paid, is_overdue, invoice_status','synced daily 02:00 UTC',15297)
) AS src (view_name, domain, description, key_columns, refresh_note, row_estimate)
ON tgt.view_name = src.view_name
WHEN MATCHED THEN UPDATE SET
    domain = src.domain, description = src.description, key_columns = src.key_columns,
    refresh_note = src.refresh_note, row_estimate = src.row_estimate,
    is_active = 1, updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT
    (view_name, domain, description, key_columns, refresh_note, row_estimate)
    VALUES (src.view_name, src.domain, src.description, src.key_columns,
            src.refresh_note, src.row_estimate);
GO
