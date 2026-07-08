-- ─────────────────────────────────────────────────────────────────────────────
-- QA scan: contracts where Nexudus emitted price_with_products = 0 but a real,
-- non-zero `price` exists.
--
-- WHY THIS MATTERS
--   The landlord/strategic-partnership gold views derive sold_monthly_fee from
--   COALESCE(price_with_products, price, tariff_price, 0). Because COALESCE skips
--   only NULL (never 0), a contract with price_with_products = 0 and price > 0 is
--   silently counted as €0 revenue. This under-reported Weave Security's QH office
--   (contract 1418791794, desk 3012) by €6,000/mo — August 2026 showed €14,750
--   instead of €20,750.
--
--   Fixed in landlord_dashboard_schema.sql / landlord_dashboard_revenue_schema.sql
--   by wrapping the field as NULLIF(price_with_products, 0) inside every COALESCE.
--   This scan finds any remaining contracts with the same source-data glitch so
--   they can be corrected in Nexudus (and confirms the size of the exposure).
--
-- HOW TO READ
--   monthly_understatement = the €/mo the OLD views dropped for this contract
--   (= price, since price_with_products was 0). After the view fix these lines
--   are already corrected in the dashboard; fixing Nexudus is optional cleanup so
--   the raw source matches the UI.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Part 1: detail — one row per affected contract ───────────────────────────
SELECT
    c.source_id                                   AS contract_source_id,
    c.location_source_id,
    loc.name                                      AS location_name,
    COALESCE(NULLIF(c.coworker_company, N''),
             c.coworker_billing_name,
             c.coworker_name)                     AS member_company_name,
    c.coworker_name,
    c.tariff_name,
    c.floor_plan_desk_names                       AS desk_names,
    c.price,
    c.price_with_products,
    c.unit_price,
    c.tariff_price,
    CAST(c.price AS DECIMAL(18,2))                AS monthly_understatement,
    c.active,
    c.cancelled,
    CAST(c.start_date AS DATE)                    AS start_date,
    CAST(c.cancellation_date AS DATE)             AS cancellation_date,
    -- Does this still hit the live contract-book (current + future contracts)?
    CASE
        WHEN c.active = 1 THEN 1
        WHEN c.active = 0 AND c.cancelled = 0
             AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE) THEN 1
        ELSE 0
    END                                           AS affects_current_book,
    c.updated_on,
    c.last_synced_at
FROM silver.nexudus_contracts c
LEFT JOIN silver.nexudus_locations loc
    ON loc.source_id = c.location_source_id
WHERE c.is_deleted = 0
  AND ISNULL(c.price_with_products, 0) = 0        -- 0 or NULL "with products"
  AND ISNULL(c.price, 0) <> 0                     -- but a real base price exists
ORDER BY affects_current_book DESC, ABS(c.price) DESC;

-- ── Part 2: summary — exposure per location (current/future contracts only) ──
SELECT
    c.location_source_id,
    loc.name                                      AS location_name,
    COUNT(*)                                      AS affected_contracts,
    CAST(SUM(c.price) AS DECIMAL(18,2))           AS monthly_revenue_understated
FROM silver.nexudus_contracts c
LEFT JOIN silver.nexudus_locations loc
    ON loc.source_id = c.location_source_id
WHERE c.is_deleted = 0
  AND ISNULL(c.price_with_products, 0) = 0
  AND ISNULL(c.price, 0) > 0                      -- positive fees only for the € total
  AND (
        c.active = 1
        OR (c.active = 0 AND c.cancelled = 0
            AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE))
      )
GROUP BY c.location_source_id, loc.name
ORDER BY monthly_revenue_understated DESC;
