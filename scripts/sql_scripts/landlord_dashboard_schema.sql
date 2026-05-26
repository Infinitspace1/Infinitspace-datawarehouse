-- =============================================================================
-- landlord_dashboard_schema.sql
--
-- Gold layer: Landlord Dashboard views
--
-- Objects created:
--   gold.vw_landlord_current_contracts      current-month relevant contracts, pre-filtered
--   gold.vw_landlord_contract_book_monthly  one row per location per month, ±12 months (25 total)
--   gold.vw_landlord_pricing_summary        current-month KPI summary per location
--
-- KEY CONTRACT SEMANTICS (intentionally different from vw_finance_dashboard_membership_schedule):
--   - cancellation_date is the ONLY hard end date for forecasting.
--   - contract_term (the Nexudus "contract end date") is deliberately ignored for
--     forecasting. A contract whose contract_term has passed but whose
--     cancellation_date is NULL rolls forward as ongoing indefinitely.
--   - Zero-capacity contracts (capacity known = 0) are excluded.
--   - Contracts where capacity is UNKNOWN (product link missing but floor_plan_desk_ids
--     exists) are INCLUDED with list_price_missing = 1 so revenue is never silently dropped.
--
-- STATUS FILTER IN vw_landlord_current_contracts:
--   The view is pre-filtered — Flask does not need to filter by status.
--   Included:  active = 1 (all active/paused contracts)
--              cancelled = 1 AND cancellation_date >= first-of-current-month (notice period)
--   Excluded:  active = 0 AND cancelled = 0 (abandoned/historical with no cancellation date)
--              contracts cancelled before the current month started
--
-- PRICING JOIN:
--   - List price  = SUM(silver.nexudus_products.price) for products linked via
--                   silver.nexudus_contracts.floor_plan_desk_ids (comma-separated
--                   product source_ids).
--   - Sold price  = COALESCE(price_with_products, price, tariff_price, 0) from the
--                   contract — always populated regardless of product link.
--   - list_price_missing = 1 when no product link resolves. list_monthly_fee,
--     list_price_per_ws, discount_value, discount_pct are NULL in that case.
--
-- CAPACITY COUNTING (consistent with existing gold layer):
--   item_type 1 (Office)         → product.capacity field (default 1 if 0)
--   item_type 2 (Dedicated desk) → 1 workstation
--   item_type 3 (Hot desk)       → 1 workstation
--   Other item types             → 0 (not counted)
--
-- PIPELINE VIEW: Not implemented — no HubSpot/deal pipeline data in the ETL.
--
-- MONTH RANGE: ±12 months from the current UTC month = 25 months total.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC sp_executesql N'CREATE SCHEMA gold';
GO

-- =============================================================================
-- 1. gold.vw_landlord_current_contracts
--    Pre-filtered to current-month relevant contracts. Flask reads this
--    without any further status or date filtering.
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_current_contracts
AS
WITH product_link AS (
    -- Derive per-contract capacity and list monthly fee by resolving
    -- floor_plan_desk_ids to silver.nexudus_products rows.
    -- Returns NULL rows for contracts with no resolvable physical product.
    SELECT
        c.source_id AS contract_source_id,
        SUM(
            CASE
                WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                WHEN p.item_type IN (2, 3) THEN 1
                ELSE 0
            END
        ) AS capacity,
        SUM(ISNULL(p.price, 0)) AS list_monthly_fee,
        COUNT(1)                AS product_match_count
    FROM silver.nexudus_contracts c
    CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
    INNER JOIN silver.nexudus_products p
        ON p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
       AND p.item_type IN (1, 2, 3)
       AND p.is_deleted = 0
    WHERE TRIM(s.value) <> N''
      AND c.is_deleted = 0
    GROUP BY c.source_id
)
SELECT
    -- Location
    loc.source_id                       AS location_source_id,
    COALESCE(loc.name, c.location_name) AS location_name,
    loc.city                            AS location_city,
    loc.country_name                    AS location_country_name,

    -- Contract identity
    c.source_id                         AS contract_source_id,
    c.coworker_id,
    c.coworker_name,
    COALESCE(
        NULLIF(c.coworker_company, N''),
        c.coworker_billing_name,
        c.coworker_name
    )                                   AS member_company_name,
    c.tariff_id,
    c.tariff_name,

    -- Dates
    CAST(c.start_date AS DATE)          AS start_date,
    CAST(c.cancellation_date AS DATE)   AS cancellation_date,
    CAST(c.contract_term AS DATE)       AS contract_end_date,   -- informational only; NOT a stop date

    -- Capacity
    -- NULL when product link is missing (floor_plan_desk_ids has IDs that don't resolve);
    -- 0 only when products resolve but sum to zero.
    pl.capacity,
    c.currency_code,

    -- Sold price — always populated from contract fields
    COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS sold_monthly_fee,
    CAST(
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0)
        / NULLIF(pl.capacity, 0)
        AS DECIMAL(18,2)
    )                                   AS sold_price_per_ws,

    -- List price — NULL when product link is missing
    pl.list_monthly_fee,
    CAST(
        ISNULL(pl.list_monthly_fee, 0)
        / NULLIF(pl.capacity, 0)
        AS DECIMAL(18,2)
    )                                   AS list_price_per_ws,

    -- Discount — NULL when list price is unavailable
    CASE
        WHEN pl.list_monthly_fee IS NOT NULL
            THEN CAST(
                pl.list_monthly_fee
                - COALESCE(c.price_with_products, c.price, c.tariff_price, 0)
                AS DECIMAL(18,2)
            )
        ELSE NULL
    END                                 AS discount_value,
    CASE
        WHEN pl.list_monthly_fee IS NOT NULL AND pl.list_monthly_fee <> 0
            THEN CAST(
                (pl.list_monthly_fee - COALESCE(c.price_with_products, c.price, c.tariff_price, 0))
                / pl.list_monthly_fee
                AS DECIMAL(9,4)
            )
        ELSE NULL
    END                                 AS discount_pct,

    -- Contract value
    -- If cancellation_date set: fee × months(start → cancellation).
    -- If open-ended: fee × notice_period_months (minimum committed value).
    CAST(
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0)
        * CASE
            WHEN c.start_date IS NULL    THEN 0
            WHEN c.cancellation_date IS NOT NULL
                THEN DATEDIFF(MONTH, CAST(c.start_date AS DATE), CAST(c.cancellation_date AS DATE))
            ELSE CAST(CEILING(ISNULL(c.cancellation_limit_days, 0) / 30.0) AS INT)
          END
        AS DECIMAL(18,2)
    )                                   AS contract_value,

    -- Remaining contract value from today forward
    CAST(
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0)
        * CASE
            WHEN c.cancellation_date IS NOT NULL
                THEN CASE
                    WHEN CAST(c.cancellation_date AS DATE) < CAST(GETUTCDATE() AS DATE) THEN 0
                    ELSE DATEDIFF(MONTH, CAST(GETUTCDATE() AS DATE), CAST(c.cancellation_date AS DATE))
                END
            ELSE CAST(CEILING(ISNULL(c.cancellation_limit_days, 0) / 30.0) AS INT)
          END
        AS DECIMAL(18,2)
    )                                   AS remaining_contract_value,

    -- Terms
    CAST(CEILING(ISNULL(c.cancellation_limit_days, 0) / 30.0) AS INT) AS notice_period_months,
    c.term_duration_months              AS term_months,

    -- Status — derived, no further filtering needed in Flask
    CASE
        WHEN c.in_paused_period = 1                                     THEN N'paused'
        WHEN c.cancelled = 1 AND c.cancellation_date IS NOT NULL        THEN N'notice_period'
        WHEN c.active = 1                                               THEN N'active'
        ELSE N'active'     -- active=1 catch-all (other flag combinations)
    END                                 AS status,

    CASE
        WHEN c.cancellation_date IS NOT NULL
            THEN DATEDIFF(DAY, CAST(GETUTCDATE() AS DATE), CAST(c.cancellation_date AS DATE))
        ELSE NULL
    END                                 AS days_until_cancellation,

    -- Data quality flag: 1 when product link is missing so list price cannot be computed
    CASE WHEN pl.contract_source_id IS NULL THEN 1 ELSE 0 END AS list_price_missing,
    ISNULL(pl.product_match_count, 0)   AS product_match_count,

    -- Timestamps
    c.last_synced_at,
    CAST(GETUTCDATE() AS DATE)          AS last_refreshed_at

FROM silver.nexudus_contracts c
LEFT JOIN silver.nexudus_locations loc
    ON  loc.source_id = c.location_source_id
    AND loc.is_deleted = 0
LEFT JOIN product_link pl
    ON  pl.contract_source_id = c.source_id
WHERE c.is_deleted = 0

  -- Status pre-filter: include active contracts and notice-period contracts only.
  -- Excludes: abandoned (active=0, cancelled=0), pre-month cancellations.
  AND (
      c.active = 1
      OR (
          c.cancelled = 1
          AND c.cancellation_date IS NOT NULL
          AND CAST(c.cancellation_date AS DATE)
              >= DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)
      )
  )

  -- Date pre-filter: contract started on or before end of current month
  AND c.start_date IS NOT NULL
  AND CAST(c.start_date AS DATE) <= EOMONTH(GETUTCDATE())

  -- Cancellation pre-filter: not cancelled before the current month started
  AND (
      c.cancellation_date IS NULL
      OR CAST(c.cancellation_date AS DATE)
         >= DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)
  )

  -- Capacity pre-filter: exclude only contracts where capacity is KNOWN to be 0.
  -- Contracts with NULL capacity (product link missing) are retained — they still
  -- carry sold revenue and the gap is flagged via list_price_missing.
  AND (pl.capacity IS NULL OR pl.capacity > 0);
GO


-- =============================================================================
-- 2. gold.vw_landlord_contract_book_monthly
--    One row per location per month covering ±12 months (25 months total).
--    Contract active-in-month rule:
--      start_date <= EOMONTH(month_start)
--      AND (cancellation_date IS NULL OR cancellation_date >= month_start)
--    contract_term (contract_end_date) is deliberately NOT used as a stop criterion.
--
--    Contracts included in the monthly model:
--      active = 1  OR  (cancelled = 1 AND cancellation_date is set)
--    This mirrors the current-contracts view: abandoned (active=0, no cancellation)
--    contracts are excluded because they carry no reliable stop date.
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_contract_book_monthly
AS
WITH month_offsets AS (
    -- Integers -12 to +12 via recursion (25 rows, well within MAXRECURSION 100)
    SELECT -12 AS n
    UNION ALL
    SELECT n + 1 FROM month_offsets WHERE n < 12
),
month_spine AS (
    SELECT
        DATEADD(
            MONTH, n,
            DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)
        ) AS month_start
    FROM month_offsets
),
location_list AS (
    SELECT
        source_id       AS location_source_id,
        name            AS location_name,
        city            AS location_city,
        country_name    AS location_country_name
    FROM silver.nexudus_locations
    WHERE is_deleted = 0
),
-- Physical capacity: total available workstations per location (current snapshot).
location_capacity AS (
    SELECT
        p.location_source_id,
        SUM(
            CASE
                WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                WHEN p.item_type IN (2, 3) THEN 1
                ELSE 0
            END
        ) AS total_workstation_capacity
    FROM silver.nexudus_products p
    INNER JOIN silver.nexudus_locations loc
        ON  loc.source_id = p.location_source_id
        AND loc.is_deleted = 0
    WHERE p.item_type IN (1, 2, 3)
      AND p.is_available = 1
      AND p.is_deleted = 0
      -- Exclude floor-2 refurb products at Taurusavenue until re-enabled in Nexudus
      AND NOT (loc.name = N'Amsterdam - Hoofddorp - Taurusavenue 3' AND p.name LIKE N'2-%')
    GROUP BY p.location_source_id
),
-- Per-contract facts used for the monthly rollup.
-- LEFT JOIN to product_link so contracts without product resolution are still
-- counted for revenue (sold_monthly_fee) even if capacity is unknown.
contract_product_link AS (
    SELECT
        c.source_id AS contract_source_id,
        SUM(
            CASE
                WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                WHEN p.item_type IN (2, 3) THEN 1
                ELSE 0
            END
        ) AS capacity,
        SUM(ISNULL(p.price, 0)) AS list_monthly_fee
    FROM silver.nexudus_contracts c
    CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
    INNER JOIN silver.nexudus_products p
        ON  p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
        AND p.item_type IN (1, 2, 3)
        AND p.is_deleted = 0
    WHERE TRIM(s.value) <> N''
      AND c.is_deleted = 0
    GROUP BY c.source_id
),
contract_facts AS (
    SELECT
        c.source_id                 AS contract_source_id,
        c.location_source_id,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS sold_monthly_fee,
        CAST(c.start_date        AS DATE) AS start_date,
        CAST(c.cancellation_date AS DATE) AS cancellation_date,
        -- Use product-derived capacity when available; NULL otherwise
        pl.capacity,
        pl.list_monthly_fee
    FROM silver.nexudus_contracts c
    LEFT JOIN contract_product_link pl
        ON pl.contract_source_id = c.source_id
    WHERE c.is_deleted = 0
      AND c.start_date IS NOT NULL
      -- Same status filter as current-contracts view
      AND (
          c.active = 1
          OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
      )
      -- Exclude contracts where capacity is explicitly 0 (known zero-capacity)
      AND (pl.capacity IS NULL OR pl.capacity > 0)
),
-- Fan out: for each location+month, which contracts are active?
active_by_month AS (
    SELECT
        ms.month_start,
        cf.location_source_id,
        cf.contract_source_id,
        ISNULL(cf.capacity, 0)          AS capacity,
        cf.sold_monthly_fee,
        ISNULL(cf.list_monthly_fee, 0)  AS list_monthly_fee,
        CASE WHEN cf.list_monthly_fee IS NULL THEN 1 ELSE 0 END AS list_price_missing,
        CASE
            WHEN cf.start_date >= ms.month_start
             AND cf.start_date <= EOMONTH(ms.month_start) THEN 1
            ELSE 0
        END AS is_new_this_month,
        CASE
            WHEN cf.cancellation_date IS NOT NULL
             AND cf.cancellation_date >= ms.month_start
             AND cf.cancellation_date <= EOMONTH(ms.month_start) THEN 1
            ELSE 0
        END AS is_cancelling_this_month
    FROM month_spine ms
    INNER JOIN contract_facts cf
        ON  cf.start_date <= EOMONTH(ms.month_start)
        AND (
            cf.cancellation_date IS NULL
            OR cf.cancellation_date >= ms.month_start
        )
),
monthly_agg AS (
    SELECT
        month_start,
        location_source_id,
        COUNT(1)                                                            AS active_contract_count,
        SUM(capacity)                                                       AS occupied_workstations,
        SUM(sold_monthly_fee)                                               AS sold_monthly_revenue,
        SUM(list_monthly_fee)                                               AS list_monthly_revenue,
        SUM(list_price_missing)                                             AS contracts_missing_list_price,
        SUM(CASE WHEN is_new_this_month      = 1 THEN capacity ELSE 0 END) AS new_workstations_starting,
        SUM(CASE WHEN is_cancelling_this_month = 1 THEN capacity ELSE 0 END) AS workstations_cancelling
    FROM active_by_month
    GROUP BY month_start, location_source_id
)
SELECT
    FORMAT(ms.month_start, 'yyyy-MM')           AS period,
    ms.month_start                              AS month_start_date,
    ll.location_source_id,
    ll.location_name,
    ll.location_city,
    ll.location_country_name,

    -- Physical capacity (current snapshot denominator)
    ISNULL(lc.total_workstation_capacity, 0)    AS total_workstation_capacity,

    -- Activity
    ISNULL(ma.active_contract_count, 0)         AS active_contract_count,
    ISNULL(ma.occupied_workstations, 0)         AS occupied_workstations,
    CASE
        WHEN ISNULL(lc.total_workstation_capacity, 0) - ISNULL(ma.occupied_workstations, 0) < 0
            THEN 0
        ELSE ISNULL(lc.total_workstation_capacity, 0) - ISNULL(ma.occupied_workstations, 0)
    END                                         AS vacant_workstations,
    CAST(
        100.0 * ISNULL(ma.occupied_workstations, 0)
        / NULLIF(ISNULL(lc.total_workstation_capacity, 0), 0)
        AS DECIMAL(9,4)
    )                                           AS occupancy_pct,

    -- Revenue
    ISNULL(ma.sold_monthly_revenue, 0)          AS sold_monthly_revenue,
    ISNULL(ma.list_monthly_revenue, 0)          AS list_monthly_revenue,
    CAST(
        ISNULL(ma.sold_monthly_revenue, 0)
        / NULLIF(ISNULL(ma.occupied_workstations, 0), 0)
        AS DECIMAL(18,2)
    )                                           AS avg_sold_price_per_ws,
    CAST(
        ISNULL(ma.list_monthly_revenue, 0)
        / NULLIF(ISNULL(ma.occupied_workstations, 0), 0)
        AS DECIMAL(18,2)
    )                                           AS avg_list_price_per_ws,
    CAST(
        (ISNULL(ma.list_monthly_revenue, 0) - ISNULL(ma.sold_monthly_revenue, 0))
        / NULLIF(ISNULL(ma.list_monthly_revenue, 0), 0)
        AS DECIMAL(9,4)
    )                                           AS avg_discount_pct,
    CAST(
        ISNULL(ma.list_monthly_revenue, 0) - ISNULL(ma.sold_monthly_revenue, 0)
        AS DECIMAL(18,2)
    )                                           AS discount_monthly_value,

    -- Workstation flow
    ISNULL(ma.new_workstations_starting, 0)     AS new_workstations_starting,
    ISNULL(ma.workstations_cancelling, 0)       AS workstations_cancelling,
    ISNULL(ma.new_workstations_starting, 0)
        - ISNULL(ma.workstations_cancelling, 0) AS net_workstation_change,

    -- Data quality
    ISNULL(ma.contracts_missing_list_price, 0)  AS contracts_missing_list_price,

    N'contract_book'                            AS calculation_basis

FROM month_spine ms
CROSS JOIN location_list ll
LEFT JOIN location_capacity lc
    ON  lc.location_source_id = ll.location_source_id
LEFT JOIN monthly_agg ma
    ON  ma.month_start        = ms.month_start
    AND ma.location_source_id = ll.location_source_id;
GO


-- =============================================================================
-- 3. gold.vw_landlord_pricing_summary
--    One row per location for the current month — fast KPI reads for the dashboard.
--    Aggregates gold.vw_landlord_current_contracts.
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_pricing_summary
AS
SELECT
    location_source_id,
    location_name,
    location_city,
    location_country_name,
    FORMAT(
        DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1),
        'yyyy-MM'
    )                                               AS period,

    -- Revenue (sold always populated; list NULL where product link missing)
    CAST(SUM(sold_monthly_fee)  AS DECIMAL(18,2))  AS sold_monthly_revenue,
    CAST(SUM(ISNULL(list_monthly_fee, 0)) AS DECIMAL(18,2)) AS list_monthly_revenue,

    -- Per-workstation averages (capacity excludes NULL-capacity rows from average)
    CAST(
        SUM(sold_monthly_fee) / NULLIF(SUM(capacity), 0)
        AS DECIMAL(18,2)
    )                                               AS avg_sold_price_per_ws,
    CAST(
        SUM(ISNULL(list_monthly_fee, 0)) / NULLIF(SUM(capacity), 0)
        AS DECIMAL(18,2)
    )                                               AS avg_list_price_per_ws,

    -- Discount (NULL when no list prices exist for this location)
    CAST(
        (SUM(ISNULL(list_monthly_fee, 0)) - SUM(sold_monthly_fee))
        / NULLIF(SUM(ISNULL(list_monthly_fee, 0)), 0)
        AS DECIMAL(9,4)
    )                                               AS avg_discount_pct,
    CAST(
        SUM(ISNULL(list_monthly_fee, 0)) - SUM(sold_monthly_fee)
        AS DECIMAL(18,2)
    )                                               AS discount_monthly_value,

    -- Occupancy
    SUM(ISNULL(capacity, 0))                        AS occupied_workstations,
    COUNT(1)                                        AS active_contract_count,

    -- QA: fraction of contracts with a valid product-price link
    CAST(
        100.0 * SUM(CASE WHEN list_price_missing = 0 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(1), 0)
        AS DECIMAL(9,4)
    )                                               AS product_match_coverage_pct,

    -- Number of contracts where list price could not be determined
    SUM(list_price_missing)                         AS contracts_missing_list_price,

    CAST(GETUTCDATE() AS DATE)                      AS last_refreshed_at

FROM gold.vw_landlord_current_contracts
GROUP BY
    location_source_id,
    location_name,
    location_city,
    location_country_name;
GO


-- =============================================================================
-- QA / VALIDATION QUERIES
-- Run manually after deployment to validate correctness.
-- =============================================================================

-- ── QA 1: Compare current-month occupied workstations ────────────────────────
-- New landlord view vs existing finance_dashboard_revenue_occupancy.
-- Differences are expected because:
--   a) The landlord view does not filter active=1 AND in_paused_period=0 (the
--      existing SP does). Paused contracts contribute to occupancy here.
--   b) Contracts without product links are included in landlord view (with
--      capacity = NULL / 0); existing SP also derives capacity from products.
-- A large unexplained delta indicates a data issue worth investigating.
/*
SELECT
    COALESCE(l.location_name, r.location_name)      AS location_name,
    l.occupied_workstations                         AS landlord_view_occupied,
    r.occupied_workstations                         AS existing_dashboard_occupied,
    l.occupied_workstations - r.occupied_workstations AS delta,
    l.occupancy_pct                                 AS landlord_pct,
    r.occupancy_pct                                 AS existing_pct
FROM gold.vw_landlord_contract_book_monthly l
FULL OUTER JOIN gold.finance_dashboard_revenue_occupancy r
    ON  r.location_source_id = l.location_source_id
    AND r.as_of_date_utc     = CAST(GETUTCDATE() AS DATE)
WHERE l.period = FORMAT(GETUTCDATE(), 'yyyy-MM')
   OR r.as_of_date_utc = CAST(GETUTCDATE() AS DATE)
ORDER BY ABS(ISNULL(l.occupied_workstations, 0) - ISNULL(r.occupied_workstations, 0)) DESC;
*/

-- ── QA 2: Contracts with contract_end_date but no cancellation_date ──────────
-- Must appear as ONGOING in the monthly forecast past their contract_term.
-- If any of these contracts are missing from future months, the stop-date logic
-- has regressed to the old contract_term semantics.
/*
WITH sample AS (
    SELECT TOP 10
        contract_source_id,
        location_name,
        member_company_name,
        start_date,
        contract_end_date,
        cancellation_date,
        status
    FROM gold.vw_landlord_current_contracts
    WHERE contract_end_date IS NOT NULL
      AND cancellation_date IS NULL
    ORDER BY contract_end_date
)
SELECT
    s.contract_source_id,
    s.location_name,
    s.member_company_name,
    s.contract_end_date,
    future_months.period,
    future_months.active_contract_count
FROM sample s
CROSS APPLY (
    SELECT period, active_contract_count
    FROM gold.vw_landlord_contract_book_monthly m
    WHERE m.location_source_id = s.location_source_id
      AND m.period > FORMAT(s.contract_end_date, 'yyyy-MM')
      AND m.period <= FORMAT(DATEADD(MONTH, 3, s.contract_end_date), 'yyyy-MM')
) AS future_months
ORDER BY s.contract_end_date, future_months.period;
*/

-- ── QA 3: Contracts with missing product-price link ───────────────────────────
-- These appear in landlord views with list_price_missing = 1.
-- High count = investigate Nexudus product sync or floor_plan_desk_ids population.
-- Zero count = pricing coverage is 100%.
/*
SELECT
    location_name,
    contract_source_id,
    coworker_name,
    member_company_name,
    tariff_name,
    sold_monthly_fee,
    capacity,
    list_price_missing,
    product_match_count,
    status,
    start_date,
    cancellation_date
FROM gold.vw_landlord_current_contracts
WHERE list_price_missing = 1
ORDER BY location_name, sold_monthly_fee DESC;
-- If count is non-trivial, investigate whether floor_plan_desk_ids is populated
-- for these contracts in silver.nexudus_contracts.
*/

-- ── QA 3b: Root cause — floor_plan_desk_ids populated but IDs don't resolve ──
-- Distinguishes between contracts with empty IDs (no desk assigned) vs
-- contracts with IDs that reference soft-deleted or missing products.
/*
SELECT
    c.source_id         AS contract_source_id,
    loc.name            AS location_name,
    c.coworker_name,
    c.floor_plan_desk_ids,
    c.active,
    c.cancelled
FROM silver.nexudus_contracts c
LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
WHERE c.is_deleted = 0
  AND ISNULL(c.floor_plan_desk_ids, N'') <> N''   -- IDs exist...
  AND NOT EXISTS (                                  -- ...but none resolve to a product
      SELECT 1
      FROM STRING_SPLIT(c.floor_plan_desk_ids, N',') s
      INNER JOIN silver.nexudus_products p
          ON p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
         AND p.item_type IN (1, 2, 3)
         AND p.is_deleted = 0
      WHERE TRIM(s.value) <> N''
  )
ORDER BY loc.name, c.source_id;
*/

-- ── QA 4: Duplicate product IDs in floor_plan_desk_ids ───────────────────────
-- If the same product ID appears twice, capacity is double-counted.
/*
SELECT
    c.source_id AS contract_source_id,
    loc.name    AS location_name,
    s.value     AS raw_desk_id,
    COUNT(1)    AS occurrences
FROM silver.nexudus_contracts c
INNER JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
WHERE TRIM(s.value) <> N''
  AND c.is_deleted = 0
GROUP BY c.source_id, loc.name, s.value
HAVING COUNT(1) > 1
ORDER BY loc.name, c.source_id;
*/

-- ── QA 5: Division-by-zero safety check ──────────────────────────────────────
-- Contracts in the view with capacity > 0 should always have sold_price_per_ws.
-- list_price_per_ws and discount_pct are allowed to be NULL (missing product link).
/*
SELECT
    contract_source_id,
    location_name,
    capacity,
    sold_monthly_fee,
    sold_price_per_ws,
    list_monthly_fee,
    list_price_per_ws,
    discount_pct,
    list_price_missing
FROM gold.vw_landlord_current_contracts
WHERE (capacity > 0 AND sold_price_per_ws IS NULL)
   OR (list_price_missing = 0 AND list_price_per_ws IS NULL)
   OR (list_price_missing = 0 AND list_monthly_fee > 0 AND discount_pct IS NULL);
-- Expect 0 rows.
*/

-- ── QA 6: 25-month row completeness per location ─────────────────────────────
-- Every active location should have exactly 25 rows (−12 to +12 months).
/*
SELECT
    location_name,
    COUNT(1)    AS month_rows,
    MIN(period) AS earliest_period,
    MAX(period) AS latest_period
FROM gold.vw_landlord_contract_book_monthly
GROUP BY location_name
HAVING COUNT(1) <> 25
ORDER BY location_name;
-- Expect 0 rows.
*/

-- ── QA 7: Current-month pricing summary snapshot ─────────────────────────────
/*
SELECT
    location_name,
    period,
    active_contract_count,
    occupied_workstations,
    sold_monthly_revenue,
    list_monthly_revenue,
    avg_sold_price_per_ws,
    avg_list_price_per_ws,
    avg_discount_pct,
    discount_monthly_value,
    product_match_coverage_pct,
    contracts_missing_list_price
FROM gold.vw_landlord_pricing_summary
ORDER BY location_name;
*/

-- ── QA 8: Abandoned contracts leaking into the view ──────────────────────────
-- active=0, cancelled=0 contracts must NOT appear (pre-filter should catch them).
/*
SELECT COUNT(1) AS abandoned_contract_leaks
FROM gold.vw_landlord_current_contracts lc
INNER JOIN silver.nexudus_contracts c ON c.source_id = lc.contract_source_id
WHERE c.active = 0
  AND c.cancelled = 0;
-- Expect 0 rows.
*/
