-- =============================================================================
-- ava.sp_refresh_product_availability
-- =============================================================================
-- Purpose  : Rebuild ava.product_availability from scratch every run.
--            Clears the table then inserts one row per bookable item,
--            grouped into 5 categories:
--              1. hot_desk        (silver.nexudus_products item_type = 3)
--              2. dedicated_desk  (silver.nexudus_products item_type = 2)
--              3. private_office  (silver.nexudus_products item_type = 1 + contracts)
--              4. meeting_room    (silver.nexudus_resources = rooms; extra_services = prices,
--                                  matched on resource type name — card location ignored)
--              5. day_pass        (silver.nexudus_extra_services, resource_type_names LIKE 'hot desk%',
--                                  location corrected via the site tag in the type name)
--
-- Called by: functions/ava_refresh.py  (Azure Function, timer 03:00 UTC)
-- Run time : ~5–30 s depending on data volume
-- =============================================================================

CREATE OR ALTER PROCEDURE ava.sp_refresh_product_availability
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION;
    BEGIN TRY

        -- ------------------------------------------------------------------
        -- Clear previous snapshot.
        -- DELETE (not TRUNCATE) so this runs with the function app's default
        -- DELETE permission. TRUNCATE requires ALTER, which gets dropped
        -- whenever the schema script recreates the table.
        -- ------------------------------------------------------------------
        DELETE FROM ava.product_availability;


        -- ==================================================================
        -- 1. HOT DESKS  (item_type = 3)
        --    • capacity = 1
        --    • price    = products.price
        --    • always available
        -- ==================================================================
        INSERT INTO ava.product_availability (
            location_source_id, location_name, city, country_name,
            item_category,
            product_source_id, resource_source_id, extra_service_source_id,
            item_name,
            capacity,
            price, external_price, currency_code, charge_period,
            is_available, available_from, occupied_until, next_occupied_from,
            chain_occupied_until, availability_notes,
            last_refreshed_at
        )
        SELECT
            p.location_source_id,
            l.name              AS location_name,
            l.city,
            l.country_name,
            'hot_desk'          AS item_category,
            p.source_id         AS product_source_id,
            NULL                AS resource_source_id,
            NULL                AS extra_service_source_id,
            p.name              AS item_name,
            1                   AS capacity,
            p.price,
            NULL                AS external_price,
            p.currency_code,
            'per_month'         AS charge_period,
            1                   AS is_available,
            NULL                AS available_from,
            NULL                AS occupied_until,
            NULL                AS next_occupied_from,
            NULL                AS chain_occupied_until,
            'Always available'  AS availability_notes,
            GETUTCDATE()        AS last_refreshed_at
        FROM silver.nexudus_products p
        JOIN silver.nexudus_locations l ON p.location_source_id = l.source_id
        WHERE p.item_type = 3
          AND p.is_available = 1
          AND p.is_deleted   = 0;


        -- ==================================================================
        -- 2. DEDICATED DESKS  (item_type = 2)
        --    Same rules as hot desks — always available, capacity 1.
        -- ==================================================================
        INSERT INTO ava.product_availability (
            location_source_id, location_name, city, country_name,
            item_category,
            product_source_id, resource_source_id, extra_service_source_id,
            item_name,
            capacity,
            price, external_price, currency_code, charge_period,
            is_available, available_from, occupied_until, next_occupied_from,
            chain_occupied_until, availability_notes,
            last_refreshed_at
        )
        SELECT
            p.location_source_id,
            l.name,
            l.city,
            l.country_name,
            'dedicated_desk',
            p.source_id,
            NULL,
            NULL,
            p.name,
            1,
            p.price,
            NULL,
            p.currency_code,
            'per_month',
            1,
            NULL,
            NULL,
            NULL,
            NULL,
            'Always available',
            GETUTCDATE()
        FROM silver.nexudus_products p
        JOIN silver.nexudus_locations l ON p.location_source_id = l.source_id
        WHERE p.item_type = 2
          AND p.is_available = 1
          AND p.is_deleted   = 0;


        -- ==================================================================
        -- 3. PRIVATE OFFICES  (item_type = 1)
        --
        --    Contract join:
        --      products.contract_ids_raw is a comma-separated list of
        --      silver.nexudus_contracts.source_id values.
        --      STRING_SPLIT + TRIM expands and joins them.
        --
        --    Key concepts:
        --      active_per_product  — the CURRENT active contract (active=1), its end date.
        --      future_ranked       — future contracts (start_date > now, active=0),
        --                            ordered by start_date per product.
        --      chain_cte           — recursive CTE that walks the future contracts
        --                            in order and extends the chain as long as each
        --                            next contract starts on or before the current
        --                            chain end (consecutive / contiguous).
        --      chain_final         — the deepest recursion per product = the actual
        --                            chain end. chain_start = first future contract's
        --                            start. chain_end = last consecutive contract's
        --                            end (NULL = chain is indefinitely occupied).
        --
        --    Availability scenarios handled (10 cases in availability_notes):
        --
        --    OCCUPIED (is_available = 0):
        --      A  Rolling contract, no future chain
        --      B  Rolling contract, future chain starts
        --      C  Known end, NO gap (chain_start <= end_date), chain ends at date
        --      D  Known end, NO gap, chain is indefinite
        --      E  Known end, GAP exists, future chain ends at date
        --      F  Known end, GAP exists, future chain is indefinite
        --      G  Known end, no future contracts
        --
        --    FREE (is_available = 1):
        --      H  No future contracts at all
        --      I  Future chain ends at a known date
        --      J  Future chain is indefinite (long-term occupancy ahead)
        -- ==================================================================
        ;WITH product_contracts AS (
            -- Expand contract_ids_raw into individual contract source_id strings.
            -- Products with NULL / empty contract_ids_raw emit zero rows here
            -- and fall through to the LEFT JOINs below as "no contracts".
            SELECT
                p.source_id             AS product_id,
                TRIM(s.value)           AS contract_id_str
            FROM silver.nexudus_products p
            CROSS APPLY STRING_SPLIT(ISNULL(p.contract_ids_raw, ''), ',') s
            WHERE p.item_type = 1
              AND p.is_available = 1
              AND p.is_deleted   = 0
              -- Exclude non-office junk Nexudus mis-types as ItemType=1:
              -- Parking bays and Meeting Rooms carry ItemType=1 but are not
              -- sellable private offices (capacity 0, or a Parking / Meeting
              -- Room name, e.g. "Parking 13", "Meeting Room QH 4-A"). A real
              -- private office always has a real capacity and is never named
              -- Parking / Meeting Room. (Price is NOT filtered — a real office
              -- with a missing price still shows as "Price on request".)
              AND p.capacity >= 1
              AND p.name NOT LIKE 'Parking%'
              AND p.name NOT LIKE '%Meeting Room%'
              AND TRIM(s.value) <> ''
        ),
        linked_contracts AS (
            SELECT
                pc.product_id,
                c.source_id         AS contract_source_id,
                c.active,
                c.start_date,
                c.contract_term,        -- end / expiry date
                c.cancellation_date     -- explicit cancellation (takes priority over contract_term)
            FROM product_contracts pc
            JOIN silver.nexudus_contracts c
                ON CAST(c.source_id AS NVARCHAR(20)) = pc.contract_id_str
               AND c.is_deleted = 0
        ),
        active_per_product AS (
            -- The current occupancy: summarise active contract(s) per product.
            --
            -- end_date semantics (NULL = rolling monthly, no fixed end):
            --   1. cancellation_date set         → cancellation_date wins (explicit end)
            --   2. contract_term in the future   → contract_term (real fixed end)
            --   3. contract_term in the past,
            --      cancellation_date NULL        → NULL (rolled into month-to-month;
            --                                            Nexudus keeps active=1 and
            --                                            leaves contract_term at the
            --                                            stale initial-term-end date)
            --
            -- MAX() so that if multiple active rows exist, we take the latest end.
            SELECT
                product_id,
                MAX(
                    CASE
                        WHEN cancellation_date IS NOT NULL
                            THEN CAST(cancellation_date AS DATE)
                        WHEN contract_term IS NOT NULL
                             AND CAST(contract_term AS DATE) >= CAST(GETUTCDATE() AS DATE)
                            THEN CAST(contract_term AS DATE)
                        ELSE NULL    -- rolled into month-to-month
                    END
                ) AS end_date
            FROM linked_contracts
            WHERE active = 1
            GROUP BY product_id
        ),
        future_ranked AS (
            -- All future contracts (not yet started), ordered by start_date.
            -- active = 0 means the contract exists in Nexudus but hasn't kicked in yet.
            SELECT
                product_id,
                start_date,
                COALESCE(cancellation_date, contract_term) AS end_date,
                ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY start_date ASC) AS rn
            FROM linked_contracts
            WHERE start_date > GETUTCDATE()
              AND active = 0
        ),
        chain_cte AS (
            -- Recursive CTE: build the consecutive chain of future contracts.
            --
            -- Anchor: the first future contract per product.
            SELECT
                product_id,
                start_date  AS chain_start,
                end_date    AS chain_end,
                rn          AS last_rn
            FROM future_ranked
            WHERE rn = 1

            UNION ALL

            -- Recursive step: extend the chain if the NEXT contract starts on or
            -- before the current chain ends (consecutive or overlapping).
            -- Stops when:
            --   • there is no next contract  (JOIN misses)
            --   • the next contract starts AFTER the current chain end (gap → new window)
            --   • the current chain_end IS NULL (rolling/indefinite — chain already open-ended)
            SELECT
                cc.product_id,
                cc.chain_start,
                fr.end_date     AS chain_end,   -- extend to next contract's end
                fr.rn           AS last_rn
            FROM chain_cte cc
            JOIN future_ranked fr
                ON  fr.product_id = cc.product_id
                AND fr.rn         = cc.last_rn + 1
                AND cc.chain_end IS NOT NULL         -- stop if chain is already indefinite
                AND fr.start_date <= cc.chain_end    -- consecutive: no gap
        ),
        chain_final AS (
            -- Keep only the deepest (last) recursion per product.
            -- chain_start = start of first future contract (= next_occupied_from for free offices)
            -- chain_end   = end of last consecutive contract (NULL = indefinitely occupied)
            SELECT
                product_id,
                chain_start,
                chain_end,
                ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_rn DESC) AS rev_rn
            FROM chain_cte
        ),
        office_avail AS (
            SELECT
                p.source_id         AS product_id,

                -- is_available -------------------------------------------------------
                CASE WHEN ap.product_id IS NOT NULL THEN 0 ELSE 1 END
                    AS is_available,

                -- occupied_until -----------------------------------------------------
                -- End of the CURRENT active contract only. NULL = rolling monthly.
                CASE WHEN ap.product_id IS NOT NULL THEN ap.end_date ELSE NULL END
                    AS occupied_until,

                -- available_from -----------------------------------------------------
                -- The FIRST date a new customer can actually move in.
                --
                -- Occupied + NO gap (chain_start <= occupied_until):
                --   The chain immediately re-occupies → next opening is chain_end.
                --   NULL if chain is indefinite.
                -- Occupied + GAP exists (chain_start > occupied_until) OR no future:
                --   The gap starts at occupied_until.
                -- Free: NULL (available right now).
                CASE
                    WHEN ap.product_id IS NOT NULL
                         AND ap.end_date IS NOT NULL
                         AND cf.chain_start IS NOT NULL
                         AND cf.chain_start <= ap.end_date    -- no gap → chain follows directly
                        THEN cf.chain_end                     -- NULL = indefinitely unavailable
                    WHEN ap.product_id IS NOT NULL AND ap.end_date IS NOT NULL
                        THEN ap.end_date                      -- gap exists or no future
                    ELSE NULL                                 -- currently free
                END AS available_from,

                -- next_occupied_from -------------------------------------------------
                -- For FREE offices only: when will it first be taken?
                CASE
                    WHEN ap.product_id IS NULL AND cf.chain_start IS NOT NULL
                        THEN cf.chain_start
                    ELSE NULL
                END AS next_occupied_from,

                -- chain_occupied_until -----------------------------------------------
                -- End of the consecutive future contract chain (NULL = indefinite).
                -- Set for all private offices that have at least one future contract.
                CASE WHEN cf.chain_start IS NOT NULL THEN cf.chain_end ELSE NULL END
                    AS chain_occupied_until,

                -- availability_notes -------------------------------------------------
                -- 10 distinct human-readable cases covering every scenario.
                CASE

                    -- ---- OCCUPIED CASES ------------------------------------------

                    -- A: Rolling monthly, no future chain
                    WHEN ap.product_id IS NOT NULL
                         AND ap.end_date IS NULL
                         AND cf.chain_start IS NULL
                        THEN 'Occupied – active monthly contract, no fixed end date'

                    -- B: Rolling monthly, future chain starts after renewal
                    WHEN ap.product_id IS NOT NULL
                         AND ap.end_date IS NULL
                         AND cf.chain_start IS NOT NULL
                        THEN 'Occupied (monthly renewal); re-occupied from '
                             + CONVERT(VARCHAR(10), cf.chain_start, 23)

                    -- C: Known end, NO gap, chain has a known end date
                    WHEN ap.product_id IS NOT NULL
                         AND ap.end_date IS NOT NULL
                         AND cf.chain_start IS NOT NULL
                         AND cf.chain_start <= ap.end_date   -- consecutive
                         AND cf.chain_end IS NOT NULL
                        THEN 'Occupied until ' + CONVERT(VARCHAR(10), ap.end_date, 23)
                             + '; re-occupied through ' + CONVERT(VARCHAR(10), cf.chain_end, 23)

                    -- D: Known end, NO gap, chain is indefinite (open-ended)
                    WHEN ap.product_id IS NOT NULL
                         AND ap.end_date IS NOT NULL
                         AND cf.chain_start IS NOT NULL
                         AND cf.chain_start <= ap.end_date   -- consecutive
                         AND cf.chain_end IS NULL
                        THEN 'Occupied until ' + CONVERT(VARCHAR(10), ap.end_date, 23)
                             + '; immediately re-occupied with no known end date'

                    -- E: Known end, GAP exists, chain re-occupies later, chain has known end
                    WHEN ap.product_id IS NOT NULL
                         AND ap.end_date IS NOT NULL
                         AND cf.chain_start IS NOT NULL
                         AND cf.chain_start > ap.end_date    -- real gap exists
                         AND cf.chain_end IS NOT NULL
                        THEN 'Occupied until ' + CONVERT(VARCHAR(10), ap.end_date, 23)
                             + '; briefly available, then re-occupied from '
                             + CONVERT(VARCHAR(10), cf.chain_start, 23)
                             + ' through ' + CONVERT(VARCHAR(10), cf.chain_end, 23)

                    -- F: Known end, GAP exists, chain re-occupies later, chain is indefinite
                    WHEN ap.product_id IS NOT NULL
                         AND ap.end_date IS NOT NULL
                         AND cf.chain_start IS NOT NULL
                         AND cf.chain_start > ap.end_date    -- real gap exists
                         AND cf.chain_end IS NULL
                        THEN 'Occupied until ' + CONVERT(VARCHAR(10), ap.end_date, 23)
                             + '; briefly available, then re-occupied from '
                             + CONVERT(VARCHAR(10), cf.chain_start, 23)

                    -- G: Known end, no future contracts at all
                    WHEN ap.product_id IS NOT NULL
                         AND ap.end_date IS NOT NULL
                         AND cf.chain_start IS NULL
                        THEN 'Occupied until ' + CONVERT(VARCHAR(10), ap.end_date, 23)

                    -- ---- FREE CASES -----------------------------------------------

                    -- H: No future contracts — genuinely free
                    WHEN ap.product_id IS NULL AND cf.chain_start IS NULL
                        THEN 'Available'

                    -- I: Future chain exists, chain ends at a known date
                    WHEN ap.product_id IS NULL
                         AND cf.chain_start IS NOT NULL
                         AND cf.chain_end IS NOT NULL
                        THEN 'Available now – reserved from '
                             + CONVERT(VARCHAR(10), cf.chain_start, 23)
                             + ' through ' + CONVERT(VARCHAR(10), cf.chain_end, 23)

                    -- J: Future chain exists, chain is indefinite (long-term occupancy ahead)
                    WHEN ap.product_id IS NULL
                         AND cf.chain_start IS NOT NULL
                         AND cf.chain_end IS NULL
                        THEN 'Available now – reserved from '
                             + CONVERT(VARCHAR(10), cf.chain_start, 23)
                             + ' (long-term occupancy follows)'

                    ELSE 'Available'

                END AS availability_notes

            FROM silver.nexudus_products p
            LEFT JOIN active_per_product ap ON p.source_id = ap.product_id
            LEFT JOIN chain_final cf        ON p.source_id = cf.product_id
                                           AND cf.rev_rn = 1
            WHERE p.item_type = 1
              AND p.is_available = 1
              AND p.is_deleted   = 0
              -- Exclude non-office junk Nexudus mis-types as ItemType=1
              -- (Parking bays, Meeting Rooms) — must match the product_contracts
              -- CTE filter above so contracts and offices stay in sync.
              AND p.capacity >= 1
              AND p.name NOT LIKE 'Parking%'
              AND p.name NOT LIKE '%Meeting Room%'
        )
        INSERT INTO ava.product_availability (
            location_source_id, location_name, city, country_name,
            item_category,
            product_source_id, resource_source_id, extra_service_source_id,
            item_name,
            capacity,
            price, external_price, currency_code, charge_period,
            is_available, available_from, occupied_until, next_occupied_from,
            chain_occupied_until, availability_notes,
            last_refreshed_at
        )
        SELECT
            p.location_source_id,
            l.name,
            l.city,
            l.country_name,
            'private_office',
            p.source_id,
            NULL,
            NULL,
            p.name,
            p.capacity,
            p.price,
            NULL,
            p.currency_code,
            'per_month',
            oa.is_available,
            oa.available_from,
            oa.occupied_until,
            oa.next_occupied_from,
            oa.chain_occupied_until,
            oa.availability_notes,
            GETUTCDATE()
        FROM office_avail oa
        JOIN silver.nexudus_products p  ON oa.product_id        = p.source_id
        JOIN silver.nexudus_locations l ON p.location_source_id = l.source_id;


        -- ==================================================================
        -- 4. MEETING ROOMS  (resources-driven — redesigned 2026-08-13)
        --
        --    Rooms   : silver.nexudus_resources — the rooms that physically
        --              exist per location. Filter: system_resource_type = 1
        --              (meeting room), visible, not archived/deleted, and the
        --              type name mentions meeting/board (drops Boats, Rooftop,
        --              UNLP and other srt=1 non-rooms). One row per
        --              (location, resource type) with a room count in the
        --              notes. A room hidden in Nexudus drops out on the next
        --              refresh automatically.
        --    Prices  : silver.nexudus_extra_services rate cards matched on
        --              the resource TYPE NAME — globally unique because it
        --              embeds the site tag ("8 person meeting room (FC)").
        --              The card's OWN BusinessId is deliberately IGNORED:
        --              several cards are filed under the wrong business in
        --              Nexudus (all four FC room rates under The Bower, the
        --              ZT 10P rates under Aldgate — verified against the live
        --              API 2026-08-13). The old extra-services-driven build
        --              trusted the card's location, which deleted Fox Court's
        --              real rooms from this table and left only the hidden
        --              £185 classroom — the "Claire" misquote incident.
        --              (The card's ResourceTypes IDs would be the ideal join
        --              key, but the /billing/extraservices LIST endpoint the
        --              sync uses does not return them — only the per-ID
        --              detail call does. The type name is derived from the
        --              same ID relationship, so it is a faithful stand-in.)
        --    Tiers   : cards named "…standard rate…" are preferred over
        --              discounted/special tiers; ties broken by newest
        --              updated_on, then lowest price. "(non-member…)" in the
        --              card name marks the external price. The old MIN/MAX
        --              across ALL tiers blended discounted + standard into
        --              prices nobody pays (e.g. Aldgate 6P £22/£48).
        --    No card → price 0 (renderer shows "Price on request") rather
        --              than hiding a room that exists.
        -- ==================================================================
        ;WITH meeting_rooms AS (
            SELECT
                r.location_source_id,
                r.resource_type_name,
                COUNT(*)          AS room_count,
                MAX(r.allocation) AS capacity,
                MIN(r.source_id)  AS representative_resource_id
            FROM silver.nexudus_resources r
            WHERE r.is_deleted = 0
              AND r.is_visible = 1
              AND ISNULL(r.is_archived, 0) = 0
              AND r.system_resource_type = 1
              AND r.location_source_id IS NOT NULL
              AND r.resource_type_name IS NOT NULL
              AND (LOWER(r.resource_type_name) LIKE '%meeting%'
                   OR LOWER(r.resource_type_name) LIKE '%board%')
            GROUP BY r.location_source_id, r.resource_type_name
        ),
        rate_cards AS (
            SELECT
                es.source_id,
                es.resource_type_names,
                es.price,
                es.currency_code,
                CASE WHEN LOWER(es.name) LIKE '%non%member%' THEN 1 ELSE 0 END
                    AS is_external,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        es.resource_type_names,
                        CASE WHEN LOWER(es.name) LIKE '%non%member%' THEN 1 ELSE 0 END
                    ORDER BY
                        CASE WHEN LOWER(es.name) LIKE '%standard%' THEN 0 ELSE 1 END,
                        es.updated_on DESC,
                        es.price
                ) AS pick_rank
            FROM silver.nexudus_extra_services es
            WHERE es.is_deleted = 0
              AND es.charge_period = 1          -- hourly room rates only
              AND es.price > 0
              AND es.resource_type_names IS NOT NULL
        )
        INSERT INTO ava.product_availability (
            location_source_id, location_name, city, country_name,
            item_category,
            product_source_id, resource_source_id, extra_service_source_id,
            item_name,
            capacity,
            price, external_price, currency_code, charge_period,
            is_available, available_from, occupied_until, next_occupied_from,
            chain_occupied_until, availability_notes,
            last_refreshed_at
        )
        SELECT
            mr.location_source_id,
            l.name,
            l.city,
            l.country_name,
            'meeting_room',
            NULL                            AS product_source_id,
            mr.representative_resource_id   AS resource_source_id,
            m.source_id                     AS extra_service_source_id,
            mr.resource_type_name           AS item_name,
            mr.capacity,
            COALESCE(m.price, e.price, 0)   AS price,   -- 0 → "Price on request"
            CASE WHEN m.price IS NOT NULL
                  AND e.price IS NOT NULL
                  AND e.price <> m.price
                 THEN e.price
                 ELSE NULL
            END                             AS external_price,
            COALESCE(m.currency_code, e.currency_code),
            'per_booking'                   AS charge_period,
            1,
            NULL,
            NULL,
            NULL,
            NULL,
            'Always available - '
                + CAST(mr.room_count AS VARCHAR(10))
                + CASE WHEN mr.room_count = 1 THEN ' room' ELSE ' rooms' END
                + ' of this type',
            GETUTCDATE()
        FROM meeting_rooms mr
        JOIN silver.nexudus_locations l
            ON mr.location_source_id = l.source_id
        LEFT JOIN rate_cards m
            ON  m.resource_type_names = mr.resource_type_name
            AND m.is_external = 0
            AND m.pick_rank   = 1
        LEFT JOIN rate_cards e
            ON  e.resource_type_names = mr.resource_type_name
            AND e.is_external = 1
            AND e.pick_rank   = 1;


        -- ==================================================================
        -- 5. DAY PASSES
        --    Source  : silver.nexudus_extra_services
        --              WHERE LOWER(resource_type_names) LIKE 'hot desk%'
        --              AND charge_period = 2  (per-day services only — keeps
        --              the £99 monthly "Hot Desk (TB)" bundle out)
        --              AND price > 0  (partner freebies like the £0 "Free
        --              BobW Hot Desk Day Pass" are not the public day rate)
        --    Location: the site tag Nexudus embeds in resource_type_names
        --              ("Hot desk (FC)") wins over the service's own
        --              BusinessId. Several day passes are filed under the
        --              WRONG business in Nexudus (both FC passes under The
        --              Bower, both C29 passes under Heidestrasse — verified
        --              against the live API 2026-08-13), which made e.g.
        --              Heidestrasse's day-pass row a blend of QH + C29
        --              passes. Unknown/missing tag → keep the service's own
        --              location (current behaviour).
        --    Capacity: always 1
        --    Price   : MIN(price) = member rate, MAX(price) = non-member
        --              rate (same convention as the meeting-room section;
        --              external_price NULL when only one tier exists).
        --    One row per location.
        --    Always available.
        -- ==================================================================
        ;WITH day_pass_services AS (
            SELECT
                es.source_id,
                es.name,
                es.price,
                es.currency_code,
                COALESCE(tag.location_source_id, es.location_source_id) AS location_source_id
            FROM silver.nexudus_extra_services es
            OUTER APPLY (
                -- Site tag → Nexudus business id. Add a row when a new
                -- location opens (tags follow the location's short code).
                SELECT TOP 1 t.location_source_id
                FROM (VALUES
                    ('(AT)',  CAST(1376491118 AS BIGINT)),  -- Aldgate Tower
                    ('(TB)',  1415499547),                  -- The Bower / Old Street
                    ('(FC)',  1420976575),                  -- Fox Court
                    ('(ZT)',  1414964753),                  -- Zuidtoren
                    ('(REP)', 1415079491),                  -- Republica Campus
                    ('(GB)',  1420951935),                  -- Gouden Bocht
                    ('(QH)',  1420962233),                  -- Quartier Heidestrasse
                    ('(C29)', 1420976475)                   -- Chausseestrasse
                ) AS t(tag, location_source_id)
                WHERE es.resource_type_names LIKE '%' + t.tag + '%'
                ORDER BY t.tag
            ) tag
            WHERE LOWER(es.resource_type_names) LIKE 'hot desk%'
              AND es.is_deleted = 0
              AND es.charge_period = 2
              AND es.price > 0
        ),
        day_pass_min AS (
            SELECT
                location_source_id,
                MIN(price)         AS min_price,
                MAX(price)         AS max_price,
                MAX(currency_code) AS currency_code,
                MIN(source_id)     AS min_source_id,
                MIN(name)          AS item_name    -- alphabetically first pass name for display
            FROM day_pass_services
            GROUP BY location_source_id
        )
        INSERT INTO ava.product_availability (
            location_source_id, location_name, city, country_name,
            item_category,
            product_source_id, resource_source_id, extra_service_source_id,
            item_name,
            capacity,
            price, external_price, currency_code, charge_period,
            is_available, available_from, occupied_until, next_occupied_from,
            chain_occupied_until, availability_notes,
            last_refreshed_at
        )
        SELECT
            dp.location_source_id,
            l.name,
            l.city,
            l.country_name,
            'day_pass',
            NULL,
            NULL,
            dp.min_source_id,
            dp.item_name,
            1,
            dp.min_price,
            CASE WHEN dp.max_price <> dp.min_price
                 THEN dp.max_price
                 ELSE NULL
            END,                -- non-member rate (NULL when only one tier)
            dp.currency_code,
            'per_day',
            1,
            NULL,
            NULL,
            NULL,
            NULL,
            'Always available',
            GETUTCDATE()
        FROM day_pass_min dp
        JOIN silver.nexudus_locations l ON dp.location_source_id = l.source_id;


        -- All 5 sections completed successfully
        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Re-raise the error so the Azure Function catches it and logs to meta
        THROW;
    END CATCH;
END;
GO

EXEC ava.sp_refresh_product_availability;



--1 Aldgate tower: location_id 1376491118
--2 Kingsbourne House: location_id 1414964752
--3 Zuidtoren: location_id 1414964753
--4 Republica Campus: location_id 1415079491
--5 The Bower: location_id 1415499547
--6 Gouden Bocht: location_id 1420951935
--7 Quartier Heidestrasse: location_id 1420962233
--8 Quartier Chaussestrasse: location_id 1420976475
--9 Foxcourt: location_id 1420976575

SELECT * FROM ava.product_availability
WHERE location_source_id = '1415079491'
AND item_category = 'private_office'
AND is_available = 1


-- SELECT
--     source_id, name, item_type, capacity,
--     location_source_id, location_name,
--     is_available, contract_ids_raw,
--     is_deleted, deleted_at, last_synced_at
-- FROM silver.nexudus_products
-- WHERE source_id = 1415196881;

