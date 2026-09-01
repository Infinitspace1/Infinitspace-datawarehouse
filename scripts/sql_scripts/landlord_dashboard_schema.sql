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
--   - cancellation_date is the ONLY hard end date for FORECASTING (the monthly
--     contract-book view). A contract whose contract_term has passed but whose
--     cancellation_date is NULL rolls forward as ongoing indefinitely in the
--     forecast chart.
--   - contract_term IS used for VALUATION (contract_value /
--     remaining_contract_value), changed 2026-05-27. Previously valuation
--     fell back to fee × notice_period for any contract without a
--     cancellation_date — that's a legal-minimum interpretation, not a
--     forecasting one, and it understated long fixed-term contracts.
--     Valuation now uses, in priority order:
--       1. cancellation_date when set
--       2. contract_term when it's in the future (signed fixed-term)
--       3. otherwise fee × 12  (a 12-month forward horizon — matches the
--                                forecast chart; notice period is NOT used)
--   - Contracts with a linked physical product (item_type 1, 2, or 3) are included
--     normally. This excludes Beyond Access / parking-only / storage-only /
--     meeting-room-only contracts from the desk-product capacity count.
--     A mixed contract (e.g. office + parking in floor_plan_desk_ids) IS included;
--     only the physical components count toward capacity and list price. The
--     sold_monthly_fee still reflects the full contract price (unavoidable — Nexudus
--     stores a single price per contract, not per product component).
--   - Negative-fee contracts (discount / credit adjustments — Nexudus allows
--     price < 0, see silver.nexudus_contracts.price comment) are ALSO included
--     even when they have no floor_plan_desk_ids. They contribute zero capacity
--     and negative sold_monthly_fee, so SUM(sold_monthly_fee) correctly nets out
--     the adjustment instead of overstating revenue. The is_negative_adjustment
--     flag identifies these rows; list_price_missing is suppressed for them.
--
-- STATUS FILTER IN vw_landlord_current_contracts:
--   The view is pre-filtered — Flask does not need to filter by status.
--   Included:  active = 1 (all active/paused contracts)
--              cancelled = 1 AND cancellation_date >= EOMONTH(today) (notice period, still
--                active for the current month)
--   Excluded:  active = 0 AND cancelled = 0 (abandoned/historical)
--              contracts where cancellation_date < EOMONTH(today) (already past)
--              contracts where start_date > EOMONTH(today) (future — not yet current;
--                these DO appear in vw_landlord_contract_book_monthly from their
--                start month forward)
--
-- UNLINKED-FUTURE-CONTRACT BRANCH (added 2026-05-28):
--   The forecast (vw_landlord_contract_book_monthly + vw_landlord_monthly_contract_detail)
--   also includes future-signed positive-fee contracts that DON'T have
--   floor_plan_desk_ids yet. Renewal handovers commonly create the new
--   contract days before ops migrates the desk assignments — without this
--   branch, the new contract's revenue silently disappears from the forecast.
--   These contracts contribute their fee to revenue and 0 to capacity (we
--   don't know the desk count). Flagged via `is_unlinked_future = 1` in the
--   monthly detail view so dashboards can surface "Desks not linked".
--
-- MONTH-END CANCELLATION CONVENTION (changed 2026-05-27):
--   Nexudus often sets cancellation_date to the LAST DAY of a contract's final
--   billable month (e.g. a discount that applies April + May has cancellation_date
--   2026-05-31). Such contracts have `active = 1` in Nexudus through their last
--   day, so they ARE active for the cancellation month. The active-in-month rule
--   is therefore: cancellation_date >= EOMONTH(month_start), not strict >.
--   This aligns with how gold.vw_finance_dashboard_membership_schedule treats
--   `active = 1` rows. Without this, end-of-month discount lines silently drop
--   out of revenue for their own final month, overstating sold_monthly_revenue.
--
-- PRICING JOIN:
--   - List price  = SUM(silver.nexudus_products.price) for products linked via
--                   silver.nexudus_contracts.floor_plan_desk_ids (comma-separated
--                   product source_ids).
--   - Sold price  = COALESCE(NULLIF(price_with_products, 0), price, tariff_price, 0)
--                   from the contract — always populated regardless of product link.
--                   The NULLIF guards a spurious price_with_products = 0: Nexudus
--                   sometimes emits 0 for a genuinely priced contract (e.g. Weave
--                   Security desk 3012 at QH — price_with_products=0, price=6000),
--                   which a plain COALESCE would treat as valid and zero out the
--                   line. Skipping the 0 falls through to `price`. A truly comped
--                   contract has price=0 too, so it still resolves to 0.
--   - list_price_missing = 1 only for root cause E: physical product found (capacity > 0)
--     but price = NULL or 0 in Nexudus. Fix: update the product price in Nexudus and re-sync.
--
-- CAPACITY COUNTING (consistent with existing gold layer):
--   item_type 1 (Office)         → product.capacity field (default 1 if 0)
--   item_type 2 (Dedicated desk) → 1 workstation
--   item_type 3 (Hot desk)       → 1 workstation
--   Other item types             → 0 (not counted)
--
-- PRICING KPI SCOPE (added 2026-05-27):
--   Revenue columns (sold_monthly_revenue / list_monthly_revenue) include ALL
--   desk types — landlords still want the full revenue picture.
--   Per-WS average / discount columns (avg_sold_price_per_ws,
--   avg_list_price_per_ws, avg_discount_pct, discount_monthly_value) are now
--   computed from PRIVATE OFFICES ONLY (item_type = 1). Hot desks and
--   dedicated desks would otherwise drag the average down — a landlord
--   benchmarks pricing on offices, not on flex / pass products. Mixed
--   contracts (PO + parking, etc.) are excluded from the PO averages because
--   Nexudus stores a single contract price that cannot be cleanly split
--   per product component (is_pure_private_office = 1 marks the rows that
--   are eligible). The new private_office_* columns expose the underlying
--   capacity and revenue so dashboards can render "Avg based on N offices".
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
    --
    -- Item types (see ava_sp_refresh_product_availability.sql for full reference):
    --   1 = Private office (capacity = product.capacity, default 1)
    --   2 = Dedicated desk (capacity = 1)
    --   3 = Hot desk        (capacity = 1)
    --
    -- private_office_* columns expose the PO portion of each contract so the
    -- landlord pricing KPIs (avg sold / list / discount per WS) can be computed
    -- on private offices alone — the landlord brief is to price-benchmark on
    -- offices, not hot/dedicated desk averages that skew the number low.
    -- is_pure_private_office = 1 when ALL products on the contract are PO; mixed
    -- contracts (PO + hot desk + parking) are excluded from PO pricing averages
    -- because Nexudus stores a single contract price that cannot be cleanly split.
    -- item_type 4 (Other/storeroom/parking) is included so storeroom
    -- contracts (e.g. 50five at Chausseestrasse) flow revenue through.
    -- Item type 5 (Meeting Room) is deliberately NOT included: a contract
    -- pointing to a meeting-room product is a data-quality issue ops needs
    -- to fix in Nexudus, not something to silently absorb here.
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
        SUM(
            CASE WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1) ELSE 0 END
        ) AS private_office_capacity,
        SUM(
            CASE WHEN p.item_type = 1 THEN ISNULL(p.price, 0) ELSE 0 END
        ) AS private_office_list_fee,
        -- ── Per-type breakdown (MMRF / MARV, added 2026-07-16) ───────────────
        -- Workstation counts and list-price subtotals per Nexudus item_type,
        -- used downstream to ALLOCATE the single contract-level sold fee across
        -- types (Nexudus stores one price per contract — see header note). The
        -- weights below are list-price shares. private_office_* (item_type 1)
        -- already exist above and serve as the type-1 slice.
        --   MMRF = type 1 (Private Office) + 2 (Dedicated Desk) + 3 (Hot Desk)
        --   MARV = type 4 (Other: parking/storeroom) here; type 5 (Meeting Room)
        --          recurring is a data-quality exclusion (kept out of the join at
        --          item_type IN (1,2,3,4) below), so ad-hoc meeting-room revenue
        --          is sourced separately from invoice lines, not from contracts.
        SUM(CASE WHEN p.item_type = 2 THEN 1 ELSE 0 END)             AS dedicated_desk_capacity,
        SUM(CASE WHEN p.item_type = 3 THEN 1 ELSE 0 END)             AS hot_desk_capacity,
        SUM(CASE WHEN p.item_type = 4 THEN 1 ELSE 0 END)             AS additional_unit_count,
        SUM(CASE WHEN p.item_type = 2 THEN ISNULL(p.price, 0) ELSE 0 END) AS dedicated_desk_list_fee,
        SUM(CASE WHEN p.item_type = 3 THEN ISNULL(p.price, 0) ELSE 0 END) AS hot_desk_list_fee,
        SUM(CASE WHEN p.item_type = 4 THEN ISNULL(p.price, 0) ELSE 0 END) AS additional_list_fee,
        CASE
            WHEN SUM(CASE WHEN p.item_type IN (2, 3) THEN 1 ELSE 0 END) = 0
             AND SUM(CASE WHEN p.item_type = 1 THEN 1 ELSE 0 END) > 0
                THEN 1
            ELSE 0
        END AS is_pure_private_office,
        COUNT(1)                AS product_match_count
    FROM silver.nexudus_contracts c
    CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
    INNER JOIN silver.nexudus_products p
        ON p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
       AND p.item_type IN (1, 2, 3, 4)   -- 4 = Other (storeroom / parking)
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

    -- Private-office breakdown of the same product link (item_type = 1 only).
    -- Used by gold.vw_landlord_pricing_summary to compute PO-only price averages
    -- without polluting hot-desk / dedicated-desk numbers into the headline KPI.
    ISNULL(pl.private_office_capacity, 0)    AS private_office_capacity,
    pl.private_office_list_fee,
    ISNULL(pl.is_pure_private_office, 0)     AS is_pure_private_office,

    -- ── Per-type breakdown (MMRF / MARV, added 2026-07-16) ───────────────────
    -- Per-type workstation counts (type-1 = private_office_capacity above).
    ISNULL(pl.dedicated_desk_capacity, 0)    AS dedicated_desk_capacity,
    ISNULL(pl.hot_desk_capacity, 0)          AS hot_desk_capacity,
    ISNULL(pl.additional_unit_count, 0)      AS additional_unit_count,
    -- Per-type ALLOCATED sold revenue. The single contract fee is split by each
    -- type's list-price share (alloc.w_*), so
    --   rev_private_office + rev_dedicated_desk + rev_hot_desk + rev_additional
    --   = sold_monthly_fee   (reconciles exactly).
    -- MMRF = rev_private_office + rev_dedicated_desk + rev_hot_desk (types 1/2/3)
    -- MARV = rev_additional (type 4). See the alloc CROSS APPLY for the fallback
    -- when list price is missing (capacity share) or absent (residual → MMRF).
    CAST(alloc.sold_fee * alloc.w_po  AS DECIMAL(18,2)) AS rev_private_office,
    CAST(alloc.sold_fee * alloc.w_dd  AS DECIMAL(18,2)) AS rev_dedicated_desk,
    CAST(alloc.sold_fee * alloc.w_hd  AS DECIMAL(18,2)) AS rev_hot_desk,
    CAST(alloc.sold_fee * alloc.w_add AS DECIMAL(18,2)) AS rev_additional,

    c.currency_code,

    -- Sold price — for adjustment contracts (price_with_products < 0), use
    -- `price` instead. Adjustments have no linked products, so the two
    -- fields SHOULD be equal — but 4 contracts in Nexudus have a phantom
    -- delta of €20–180 (e.g. EBCONT 1417662289 at QH). Using `price`
    -- matches what Nexudus's own UI shows for those. The NULLIF(...,0) also
    -- skips a spurious price_with_products = 0 on positive contracts so the
    -- real `price` is used instead of zeroing the line.
    CASE
        WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
            THEN COALESCE(c.price, NULLIF(c.price_with_products, 0), c.tariff_price, 0)
        ELSE COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
    END                                 AS sold_monthly_fee,
    CAST(
        CASE
            WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
                THEN COALESCE(c.price, NULLIF(c.price_with_products, 0), c.tariff_price, 0)
            ELSE COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
        END
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
                - COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
                AS DECIMAL(18,2)
            )
        ELSE NULL
    END                                 AS discount_value,
    CASE
        WHEN pl.list_monthly_fee IS NOT NULL AND pl.list_monthly_fee <> 0
            THEN CAST(
                (pl.list_monthly_fee - COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0))
                / pl.list_monthly_fee
                AS DECIMAL(9,4)
            )
        ELSE NULL
    END                                 AS discount_pct,

    -- Contract value — value over the contract's KNOWN duration, or 12 months
    -- for truly open-ended contracts. This is a forecasting interpretation,
    -- not a legal-minimum one — the dashboard wants to answer "how much
    -- revenue will this contract generate?", not "what would they owe if
    -- they cancelled today?".
    --
    -- Three-tier fallback (changed 2026-05-27):
    --   1. cancellation_date set         → fee × months(start → cancellation)
    --                                       (customer is leaving on that date)
    --   2. contract_term in future       → fee × months(start → contract_term)
    --                                       (signed fixed-term, e.g. Cainiao's
    --                                        24-month office at €13,650/mo
    --                                        shows €327,600, not €40,950)
    --   3. Otherwise (truly open-ended)  → fee × 12
    --                                       (forward 12-month horizon, matching
    --                                        the dashboard's forecast chart.
    --                                        Notice period is NOT used — that
    --                                        was the previous behaviour and it
    --                                        understated ongoing contracts.)
    -- contract_term is only used HERE for valuation; the forecast chart still
    -- ignores it (rolls past it as ongoing) per the file header convention.
    CAST(
        COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
        * CASE
            WHEN c.start_date IS NULL    THEN 0
            WHEN c.cancellation_date IS NOT NULL
                THEN DATEDIFF(MONTH, CAST(c.start_date AS DATE), CAST(c.cancellation_date AS DATE))
            WHEN c.contract_term IS NOT NULL
             AND CAST(c.contract_term AS DATE) >= CAST(c.start_date AS DATE)
             AND CAST(c.contract_term AS DATE) >= CAST(GETUTCDATE() AS DATE)
                THEN DATEDIFF(MONTH, CAST(c.start_date AS DATE), CAST(c.contract_term AS DATE))
            ELSE 12
          END
        AS DECIMAL(18,2)
    )                                   AS contract_value,

    -- Remaining contract value from today forward. Same three-tier fallback as
    -- contract_value, but counting months from today:
    --   1. cancellation_date set   → months(today → cancellation)
    --   2. contract_term in future → months(today → contract_term)
    --   3. Otherwise (rolling)     → 12 (12-month forward assumption)
    CAST(
        COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
        * CASE
            WHEN c.cancellation_date IS NOT NULL
                THEN CASE
                    WHEN CAST(c.cancellation_date AS DATE) < CAST(GETUTCDATE() AS DATE) THEN 0
                    ELSE DATEDIFF(MONTH, CAST(GETUTCDATE() AS DATE), CAST(c.cancellation_date AS DATE))
                END
            WHEN c.contract_term IS NOT NULL
             AND CAST(c.contract_term AS DATE) >= CAST(GETUTCDATE() AS DATE)
                THEN DATEDIFF(MONTH, CAST(GETUTCDATE() AS DATE), CAST(c.contract_term AS DATE))
            ELSE 12
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

    -- list_price_missing = 1 when physical products exist (capacity > 0) but ALL have
    -- price = NULL or 0 in Nexudus (root cause E). Fix: set product price in Nexudus.
    -- Suppressed for negative-fee adjustment contracts (no list price expected).
    CASE
        WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0 THEN 0
        WHEN ISNULL(pl.list_monthly_fee, 0) = 0 THEN 1
        ELSE 0
    END                                 AS list_price_missing,

    -- 1 when sold_monthly_fee is negative — discount / credit / refund contract.
    -- These contribute zero capacity and negative revenue to aggregates.
    CASE
        WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0 THEN 1
        ELSE 0
    END                                 AS is_negative_adjustment,

    -- 1 when this contract's tariff sits on a "Membership Fee" financial
    -- account. Lets downstream PO KPI calculations filter out parking /
    -- ancillary discounts (e.g. Degura's €600 parking discount at QH would
    -- otherwise leak into the PO sold side).
    CASE
        WHEN LOWER(ISNULL(fa.name, N'')) LIKE N'%membership fee%' THEN 1
        ELSE 0
    END                                 AS is_membership_fee_account,
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
LEFT JOIN silver.nexudus_tariffs t
    ON  t.source_id = c.tariff_id
    AND t.is_deleted = 0
LEFT JOIN silver.nexudus_financial_accounts fa
    ON  fa.source_id = t.financial_account_id
    AND fa.is_deleted = 0
-- Per-type revenue allocation (added 2026-07-16). Computes the resolved sold fee
-- once, plus the four type weights. Weight basis, in priority order:
--   1. list_monthly_fee > 0 → each type's list-price share (sums to 1)
--   2. else capacity > 0    → each type's workstation share (type-4 = 0 desks)
--   3. else                 → whole fee to the private-office (MMRF) bucket,
--                             so negative-fee adjustments / unlinked contracts
--                             are never dropped from the per-type totals.
CROSS APPLY (
    SELECT
        CASE
            WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
                THEN COALESCE(c.price, NULLIF(c.price_with_products, 0), c.tariff_price, 0)
            ELSE COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
        END AS sold_fee,
        CASE
            WHEN ISNULL(pl.list_monthly_fee, 0) > 0 THEN ISNULL(pl.private_office_list_fee, 0) / pl.list_monthly_fee
            WHEN ISNULL(pl.capacity, 0) > 0 THEN CAST(ISNULL(pl.private_office_capacity, 0) AS FLOAT) / pl.capacity
            ELSE 1.0
        END AS w_po,
        CASE
            WHEN ISNULL(pl.list_monthly_fee, 0) > 0 THEN ISNULL(pl.dedicated_desk_list_fee, 0) / pl.list_monthly_fee
            WHEN ISNULL(pl.capacity, 0) > 0 THEN CAST(ISNULL(pl.dedicated_desk_capacity, 0) AS FLOAT) / pl.capacity
            ELSE 0.0
        END AS w_dd,
        CASE
            WHEN ISNULL(pl.list_monthly_fee, 0) > 0 THEN ISNULL(pl.hot_desk_list_fee, 0) / pl.list_monthly_fee
            WHEN ISNULL(pl.capacity, 0) > 0 THEN CAST(ISNULL(pl.hot_desk_capacity, 0) AS FLOAT) / pl.capacity
            ELSE 0.0
        END AS w_hd,
        CASE
            WHEN ISNULL(pl.list_monthly_fee, 0) > 0 THEN ISNULL(pl.additional_list_fee, 0) / pl.list_monthly_fee
            ELSE 0.0
        END AS w_add
) alloc
WHERE c.is_deleted = 0

  -- Status pre-filter: active contracts and notice-period (cancelled but still in
  -- their final billable month -- see MONTH-END CANCELLATION CONVENTION in header).
  -- Excludes: abandoned (active=0, cancelled=0).
  AND (
      c.active = 1
      OR (
          c.cancelled = 1
          AND c.cancellation_date IS NOT NULL
          AND CAST(c.cancellation_date AS DATE) >= EOMONTH(GETUTCDATE())
      )
  )

  -- Date pre-filter: contract started on or before end of current month.
  -- DATEADD(HOUR, 4, start_date) converts Nexudus's UTC end-of-day timestamps
  -- (22:00 UTC = midnight local in EU summer) so a contract booked at
  -- "2026-06-30 22:00 UTC" is treated as starting July 1, not June 30 -- preventing
  -- a brand-new contract from being shown as "current" on the cutover day.
  AND c.start_date IS NOT NULL
  AND CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) <= EOMONTH(GETUTCDATE())

  -- Cancellation pre-filter: a contract is active in the current month if its
  -- cancellation date is on or after month-end (Nexudus marks month-end-cancelled
  -- contracts as active=1 through their last day).
  --   cancellation_date = 2026-04-30 (last day of April) → NOT active in May
  --   cancellation_date = 2026-05-15 (mid-May)            → NOT active in May (partial-month
  --                                                          cancellations excluded; conservative)
  --   cancellation_date = 2026-05-31 (last day of May)    → ACTIVE in May
  --   cancellation_date = 2026-06-01 (first of June)      → ACTIVE in May
  AND (
      c.cancellation_date IS NULL
      OR CAST(c.cancellation_date AS DATE) >= EOMONTH(GETUTCDATE())
  )

  -- Product-link pre-filter: a contract must EITHER have at least one
  -- resolved product link (pl.contract_source_id IS NOT NULL — covers desks
  -- AND storerooms/parking at item_type=4, which contribute 0 to capacity
  -- but carry real revenue), OR be a negative-fee adjustment (discount /
  -- credit — no product link, but the negative sold_monthly_fee must reach
  -- the aggregates).
  -- Excludes: Beyond Access (no floor_plan_desk_ids at all) and other
  -- ancillary contracts with no resolvable products, when their price
  -- is also zero or positive.
  AND (
      pl.contract_source_id IS NOT NULL
      OR COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
  );
GO


-- =============================================================================
-- 2. gold.vw_landlord_contract_book_monthly
--    One row per location per month covering ±12 months (25 months total).
--    Contract active-in-month rule:
--      start_date <= EOMONTH(month_start)
--      AND (cancellation_date IS NULL OR cancellation_date >= EOMONTH(month_start))
--    See MONTH-END CANCELLATION CONVENTION in the file header for why >= (not >).
--    cancellation_date = 2025-04-30 → active in Apr, NOT in May.
--    cancellation_date = 2025-05-31 → active in May (final billable month).
--    contract_term (contract_end_date) is deliberately NOT used as a stop criterion.
--
--    Contracts included in the monthly model:
--      a) active = 1
--      b) cancelled = 1 AND cancellation_date is set        (in-notice / past notice)
--      c) active = 0 AND cancelled = 0 AND start_date > today
--           — future-signed contracts: not yet active in Nexudus, but already
--             booked. They appear in the forecast from their start month forward,
--             so the ±12 month book reflects committed bookings.
--      d) sold_monthly_fee < 0 (regardless of (a)/(b)/(c)) — discount / credit
--           adjustment contracts. They net negative revenue into the month they're
--           active without contributing capacity.
--    Excluded: abandoned (active=0, cancelled=0, start_date <= today) — these
--    have no reliable stop date and no committed future date.
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_contract_book_monthly
AS
WITH month_offsets AS (
    -- Window widened to -24 (2026-09-01): the period selector offers 13 months
    -- back, and each of those must still be able to show a full 12 months of
    -- history BEFORE it. At -12 the spine rolled with today, so on 1 Sep 2026
    -- Aug-2025 dropped out and the Past-12 chart for Aug-2026 lost a bar.
    -- Integers -24 to +12 via recursion (37 rows, well within MAXRECURSION 100)
    SELECT -24 AS n
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
    -- Capacity is PER (location, month) so historical months reflect the
    -- inventory that was actually bookable at the time.
    --
    -- A product counts toward month M's capacity when ALL of:
    --   1. is_available = 1                           (currently bookable in Nexudus)
    --   2. is_deleted = 0
    --   3. effective available_from <= EOMONTH(M) AND (available_to IS NULL OR available_to >= month_start(M))
    --   4. price > 0                                  (excludes Chair-style €0 placeholders)
    --
    -- Rationale for (4): the "price > 0" filter distinguishes real inventory
    -- (ops always sets a real rent on a new desk) from junk placeholders
    -- (Chair = €0, test products) without depending on contract existence —
    -- so a brand-new PO created today still counts from day 1, while
    -- never-priced placeholders are excluded forever.
    --
    -- available_from gets the same UTC end-of-day shift as contract
    -- effective_start_date (see contract_facts): Nexudus stores "available
    -- from D" as D 22:00/23:00 UTC, i.e. midnight LOCAL on D+1. Without the
    -- shift, a desk made available "from Jul 31" (= Aug 1 local) counts a
    -- full month early — e.g. Zuidtoren floor 2 (available_from
    -- 2026-07-31 22:00) belongs to August, not July. available_to keeps the
    -- raw date: "to Jul 31 22:00" = available through Jul 31, gone in August.
    SELECT
        ms.month_start,
        p.location_source_id,
        SUM(
            CASE
                WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                WHEN p.item_type IN (2, 3) THEN 1
                ELSE 0
            END
        ) AS total_workstation_capacity
    FROM month_spine ms
    INNER JOIN silver.nexudus_products p
        ON  p.item_type IN (1, 2, 3)
        AND p.is_deleted = 0
        AND p.is_available = 1
        AND ISNULL(p.price, 0) > 0
        AND (p.available_from IS NULL OR CAST(DATEADD(HOUR, 4, p.available_from) AS DATE) <= EOMONTH(ms.month_start))
        AND (p.available_to   IS NULL OR CAST(p.available_to   AS DATE) >= ms.month_start)
    INNER JOIN silver.nexudus_locations loc
        ON  loc.source_id = p.location_source_id
        AND loc.is_deleted = 0
    GROUP BY ms.month_start, p.location_source_id
),
-- Per-contract facts used for the monthly rollup.
-- LEFT JOIN to product_link so contracts without product resolution are still
-- counted for revenue (sold_monthly_fee) even if capacity is unknown.
--
-- private_office_* and is_pure_private_office come along for the ride so the
-- monthly view can compute price/discount averages from private offices only
-- (item_type = 1), matching gold.vw_landlord_pricing_summary's behaviour. See
-- that view's comments for the rationale.
contract_product_link AS (
    -- item_type 4 ("Other" — storerooms, parking, etc.) is included so
    -- contracts billing for storage flow into revenue. Storerooms
    -- contribute 0 to capacity — they're not desks. Meeting rooms
    -- (item_type 5) are deliberately NOT included: a contract pointing
    -- to a meeting-room product is a data-quality issue ops needs to fix
    -- in Nexudus (the desk needs to become item_type 1), not something
    -- to silently absorb into revenue here.
    SELECT
        c.source_id AS contract_source_id,
        SUM(
            CASE
                WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                WHEN p.item_type IN (2, 3) THEN 1
                ELSE 0  -- item_type 4 contributes 0 to capacity
            END
        ) AS capacity,
        SUM(ISNULL(p.price, 0)) AS list_monthly_fee,
        SUM(
            CASE WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1) ELSE 0 END
        ) AS private_office_capacity,
        SUM(
            CASE WHEN p.item_type = 1 THEN ISNULL(p.price, 0) ELSE 0 END
        ) AS private_office_list_fee,
        CASE
            WHEN SUM(CASE WHEN p.item_type IN (2, 3) THEN 1 ELSE 0 END) = 0
             AND SUM(CASE WHEN p.item_type = 1 THEN 1 ELSE 0 END) > 0
                THEN 1
            ELSE 0
        END AS is_pure_private_office
    FROM silver.nexudus_contracts c
    CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
    INNER JOIN silver.nexudus_products p
        ON  p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
        AND p.item_type IN (1, 2, 3, 4)   -- 4 = Other (storeroom / parking)
        AND p.is_deleted = 0
    WHERE TRIM(s.value) <> N''
      AND c.is_deleted = 0
    GROUP BY c.source_id
),
contract_facts AS (
    SELECT
        c.source_id                 AS contract_source_id,
        c.location_source_id,
        -- Member identity — used to group positive contracts with their
        -- negative-adjustment discount lines for the per-WS PO KPI.
        COALESCE(
            NULLIF(c.coworker_company, N''),
            c.coworker_billing_name,
            c.coworker_name
        )                            AS member_company_name,
        -- For adjustment contracts (price < 0), use `price` instead of
        -- `price_with_products`. Adjustments have no linked products so the
        -- two SHOULD be equal — but 4 contracts in Nexudus have a phantom
        -- delta of €20–180 (e.g. EBCONT 1417662289: price=-1149,
        -- price_with_products=-1224). Using `price` matches what Nexudus's
        -- own UI shows. Positive contracts keep using price_with_products
        -- so add-on fees still count — but via NULLIF(...,0) so a spurious
        -- price_with_products = 0 (Nexudus glitch) falls through to `price`
        -- instead of zeroing the line.
        CASE
            WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
                THEN COALESCE(c.price, NULLIF(c.price_with_products, 0), c.tariff_price, 0)
            ELSE COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
        END                          AS sold_monthly_fee,
        -- Whether this contract's tariff is a "Membership Fee" financial
        -- account. Used downstream to make sure parking/ancillary discounts
        -- don't get netted into the PO membership-fee KPI (e.g. Degura's
        -- €600 parking discount at QH).
        CASE
            WHEN LOWER(ISNULL(fa.name, N'')) LIKE N'%membership fee%' THEN 1
            ELSE 0
        END                          AS is_membership_fee_account,
        CAST(c.start_date        AS DATE) AS start_date,
        CAST(c.cancellation_date AS DATE) AS cancellation_date,
        -- effective_start_date converts Nexudus's UTC end-of-day convention to a
        -- local-time start date. Contracts that take over at "22:00 UTC" on day D
        -- are really starting at midnight local on day D+1, so for monthly billing
        -- they belong to the next month. Without this, a contract with
        -- start_date = 2026-06-30 22:00 (cutover) gets double-counted in June
        -- alongside the contract it's replacing (whose cancellation = 2026-06-30).
        CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) AS effective_start_date,
        -- Day-weighted occupancy convention (2026-08-31, matches the Budget
        -- Tracker's facts_repo._aggregate_month): the +4h-shifted cancellation
        -- date is EXCLUSIVE — the first day NOT occupied. A cancellation stored
        -- as "D 22:00 UTC" shifts to D+1, so the desk counts as occupied
        -- through D itself.
        CAST(DATEADD(HOUR, 4, c.cancellation_date) AS DATE) AS effective_cancel_excl,
        -- Use product-derived capacity when available; NULL otherwise
        pl.capacity,
        pl.list_monthly_fee,
        ISNULL(pl.private_office_capacity, 0) AS private_office_capacity,
        ISNULL(pl.private_office_list_fee, 0) AS private_office_list_fee,
        ISNULL(pl.is_pure_private_office, 0)  AS is_pure_private_office,
        CASE
            WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0 THEN 1
            ELSE 0
        END                          AS is_negative_adjustment
    FROM silver.nexudus_contracts c
    LEFT JOIN contract_product_link pl
        ON pl.contract_source_id = c.source_id
    LEFT JOIN silver.nexudus_tariffs t
        ON  t.source_id  = c.tariff_id
        AND t.is_deleted = 0
    LEFT JOIN silver.nexudus_financial_accounts fa
        ON  fa.source_id  = t.financial_account_id
        AND fa.is_deleted = 0
    WHERE c.is_deleted = 0
      AND c.start_date IS NOT NULL
      -- Status filter:
      --   active=1                                            → current/active
      --   cancelled=1 AND cancellation_date IS NOT NULL       → in or past notice
      --   active=0 AND cancelled=0 AND start_date > today     → future-signed (booked but not yet started)
      -- Abandoned (active=0, cancelled=0, start_date <= today) is excluded.
      AND (
          c.active = 1
          OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
          OR (
              c.active = 0
              AND c.cancelled = 0
              AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
          )
      )
      -- Product-link filter:
      --   1. pl.contract_source_id IS NOT NULL → contract has at least one
      --      resolved product (desk, storeroom, parking — item_type 1/2/3/4).
      --      Storeroom contracts have capacity=0 but still carry revenue.
      --   2. price < 0  → negative-fee adjustment (discount/credit), no product
      --                   link needed but must net out of monthly revenue.
      --   3. Future-signed positive-fee contracts WITHOUT floor_plan_desk_ids
      --      (renewal handovers / new tenants — Allianz #1418600394 et al.).
      --      They contribute fee to revenue, 0 to capacity.
      AND (
          pl.contract_source_id IS NOT NULL
          OR COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
          OR (
              c.active = 0
              AND c.cancelled = 0
              AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
              AND COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) > 0
          )
      )
),
-- Fan out: for each location+month, which contracts are active?
active_by_month AS (
    SELECT
        ms.month_start,
        cf.location_source_id,
        cf.contract_source_id,
        cf.member_company_name,
        cf.capacity,
        cf.sold_monthly_fee,
        ISNULL(cf.list_monthly_fee, 0)  AS list_monthly_fee,
        cf.private_office_capacity,
        cf.private_office_list_fee,
        cf.is_pure_private_office,
        cf.is_negative_adjustment,
        cf.is_membership_fee_account,
        -- list_price_missing: 1 only when a physical product exists (capacity > 0)
        -- but its price in Nexudus is NULL/0 (root cause E). Suppressed for
        -- negative-fee adjustment contracts since they're not expected to carry one.
        CASE
            WHEN cf.is_negative_adjustment = 1 THEN 0
            WHEN ISNULL(cf.list_monthly_fee, 0) = 0 THEN 1
            ELSE 0
        END AS list_price_missing,
        -- Uses effective_start_date so cutover contracts are flagged "new" in their
        -- real first billing month, not in the cutover-day month.
        CASE
            WHEN cf.effective_start_date >= ms.month_start
             AND cf.effective_start_date <= EOMONTH(ms.month_start) THEN 1
            ELSE 0
        END AS is_new_this_month,
        -- Last active month: cancellation_date falls in the NEXT calendar month (or earlier).
        -- Example: cancellation_date=2025-05-01 → April is last active month, flagged here.
        -- Example: cancellation_date=2025-05-15 → April is still last active month (May excluded).
        CASE
            WHEN cf.cancellation_date IS NOT NULL
             AND cf.cancellation_date <= EOMONTH(DATEADD(MONTH, 1, ms.month_start)) THEN 1
            ELSE 0
        END AS is_cancelling_this_month
    FROM month_spine ms
    INNER JOIN contract_facts cf
        -- Active in month: started on/before month-end AND cancellation is on/after
        -- month-end. See MONTH-END CANCELLATION CONVENTION in header for rationale.
        --   cancellation_date = 2025-04-30 → NOT active in May
        --   cancellation_date = 2025-05-15 → NOT active in May (partial-month exclusion)
        --   cancellation_date = 2025-05-31 → ACTIVE in May (final billable month)
        --   cancellation_date = 2025-06-01 → ACTIVE in May
        --
        -- Uses effective_start_date (not raw start_date) so cutover contracts that
        -- begin at 22:00 UTC on the last day of a month are correctly treated as
        -- starting the next month, preventing double-counting against the contract
        -- they replace.
        ON  cf.effective_start_date <= EOMONTH(ms.month_start)
        AND (
            cf.cancellation_date IS NULL
            OR cf.cancellation_date >= EOMONTH(ms.month_start)
        )
),
-- ── Per-member roll-up per (location, month) ─────────────────────────────────
-- Lets the avg_sold_price_per_ws / avg_list_price_per_ws KPIs filter out
-- members whose NET sold is 0 (fully comped — e.g. Elusis B.V at Herengracht).
-- Each member's gross pure-PO sold gets netted with that same member's
-- negative-fee adjustment contracts (Nexudus stores discounts as separate
-- contracts), giving the actual amount they pay. We then keep only members
-- whose net > 0 to feed the PO benchmark. Mirrors the popup table row-by-row.
per_member_per_month AS (
    SELECT
        month_start,
        location_source_id,
        member_company_name,
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0
                 THEN sold_monthly_fee ELSE 0 END)         AS po_gross_sold,
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0
                 THEN ISNULL(list_monthly_fee, 0) ELSE 0 END) AS po_list,
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0
                 THEN ISNULL(capacity, 0) ELSE 0 END)      AS po_capacity,
        -- Only net adjustments whose tariff sits on a Membership Fee
        -- financial account. Parking/ancillary discounts (e.g. Degura's
        -- €600 parking discount at QH) are excluded — they belong to a
        -- different revenue line.
        SUM(CASE WHEN is_negative_adjustment = 1
                  AND is_membership_fee_account = 1
                 THEN sold_monthly_fee ELSE 0 END)         AS adj_sold
    FROM active_by_month
    GROUP BY month_start, location_source_id, member_company_name
),
-- Filter: members with PO capacity AND net sold > 0
po_members_monthly AS (
    SELECT
        month_start,
        location_source_id,
        po_gross_sold + adj_sold AS net_sold,
        po_list,
        po_capacity
    FROM per_member_per_month
    WHERE po_capacity > 0
      AND (po_gross_sold + adj_sold) > 0
),
-- Roll up to (location, month) for the KPI calculation
po_kpi_monthly AS (
    SELECT
        month_start,
        location_source_id,
        SUM(net_sold)    AS po_net_sold,
        SUM(po_list)     AS po_list_total,
        SUM(po_capacity) AS po_capacity_total,
        -- PLAIN (per-tenant) averages: average each member's own €/desk rate so
        -- every tenant counts equally, regardless of how many desks they hold.
        -- This matches the "Average" row at the bottom of the dashboard popup.
        -- (The desk-weighted SUM/SUM totals above are retained for
        -- discount_monthly_value and any reconciliation that needs them.)
        AVG(CAST(net_sold AS FLOAT) / NULLIF(po_capacity, 0))           AS avg_sold_rate,
        AVG(CAST(po_list  AS FLOAT) / NULLIF(po_capacity, 0))           AS avg_list_rate,
        AVG(CASE WHEN po_list > 0
                 THEN (CAST(po_list AS FLOAT) - net_sold) / po_list
            END)                                                        AS avg_discount_rate
    FROM po_members_monthly
    GROUP BY month_start, location_source_id
),
monthly_agg AS (
    SELECT
        month_start,
        location_source_id,
        -- Exclude negative-fee adjustment contracts from the contract count so
        -- the headline number reflects real bookings, not adjustment lines.
        SUM(CASE WHEN is_negative_adjustment = 0 THEN 1 ELSE 0 END)         AS active_contract_count,
        SUM(ISNULL(capacity, 0))                                            AS occupied_workstations,
        SUM(sold_monthly_fee)                                               AS sold_monthly_revenue,
        SUM(list_monthly_fee)                                               AS list_monthly_revenue,
        SUM(list_price_missing)                                             AS contracts_missing_list_price,
        SUM(CASE WHEN is_new_this_month      = 1 THEN ISNULL(capacity, 0) ELSE 0 END) AS new_workstations_starting,
        SUM(CASE WHEN is_cancelling_this_month = 1 THEN ISNULL(capacity, 0) ELSE 0 END) AS workstations_cancelling,
        SUM(CASE WHEN is_negative_adjustment = 1 THEN 1 ELSE 0 END)         AS adjustment_contract_count,
        SUM(CASE WHEN is_negative_adjustment = 1 THEN sold_monthly_fee ELSE 0 END) AS adjustment_monthly_value,

        -- Private-office-only aggregates that feed avg_sold_price_per_ws /
        -- avg_list_price_per_ws / avg_discount_pct downstream. Restricted to:
        --   1. is_pure_private_office = 1 (every product is item_type = 1)
        --   2. sold_monthly_fee <> 0      (drop zero-priced / comped contracts
        --                                  so they don't deflate the per-WS avg)
        -- Negative-fee adjustment contracts (the actual discounts) are netted
        -- in downstream via adjustment_monthly_value.
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0 THEN ISNULL(capacity, 0) ELSE 0 END)
                                                                            AS private_office_capacity,
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0 THEN sold_monthly_fee ELSE 0 END)
                                                                            AS private_office_sold_revenue,
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0 THEN list_monthly_fee ELSE 0 END)
                                                                            AS private_office_list_revenue,
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0 AND is_negative_adjustment = 0 THEN 1 ELSE 0 END)
                                                                            AS private_office_contract_count
    FROM active_by_month
    GROUP BY month_start, location_source_id
),
-- ── Day-weighted occupancy (2026-08-31) ─────────────────────────────────────
-- occupancy_pct changed basis: occupied DESK-DAYS over (capacity × days in
-- month) — the hotel "room-nights" convention, matching the Budget Tracker's
-- formula. A contract starting the 28th contributes 4/31 of its desks; a
-- mid-month leaver gets credit for its actual days (the old month-end rule
-- counted it as zero). occupied_workstations (end-of-month position), contract
-- counts and the flow columns keep the original month-end rule on purpose.
-- Window per contract in month M: [max(eff_start, M.start), min(eff_cancel
-- EXCLUSIVE − 1 day, M.end)]. Contracts overlapping M at all are included —
-- including ones that both start and end inside M (the Budget Tracker's
-- known blind spot, deliberately fixed here).
--
-- ── Day-weighted REVENUE (2026-09-01) ───────────────────────────────────────
-- sold_revenue_day_weighted carries each contract's fee scaled by the share of
-- the month it was live, over the SAME window as the desk-days. The month-end
-- sold_monthly_revenue is kept unchanged beside it, so nothing downstream
-- moves until it opts in.
--
-- Why: the month-end rule is wrong at BOTH ends of a month, and both errors
-- landed in one month at beyond The Bower (Aug-2026).
--   * Faculty Science cancelled on the 19th, £70,975/mo → month-end scored it
--     £0, though it held the space for 19 of 31 days.
--   * Capi Money started on the 23rd, £40,500/mo over 90 desks → month-end
--     credited a FULL month for 8 days.
-- Day-weighting gives 70,975 × 19/31 and 40,500 × 8/31 = 43,500.81 and
-- 10,451.61, matching what was actually invoiced to the penny.
--
-- Contract universe here is the desk-days one, NOT active_by_month's: a
-- mid-month leaver must be present to contribute its days, and active_by_month
-- drops anything cancelled before month end.
desk_days_by_month AS (
    SELECT
        ms.month_start,
        cf.location_source_id,
        SUM(ISNULL(cf.capacity, 0) * w.days_occupied)   AS occupied_desk_days,
        SUM(
            CAST(cf.sold_monthly_fee AS DECIMAL(18,6)) * w.days_occupied
            / DAY(EOMONTH(ms.month_start))
        )                                               AS sold_revenue_day_weighted
    FROM month_spine ms
    INNER JOIN contract_facts cf
        ON  cf.effective_start_date <= EOMONTH(ms.month_start)
        AND (cf.effective_cancel_excl IS NULL
             OR cf.effective_cancel_excl > ms.month_start)
        AND (cf.effective_cancel_excl IS NULL
             OR cf.effective_start_date < cf.effective_cancel_excl)
    -- The occupied-day count is lifted into its own APPLY so desks and revenue
    -- are weighted by ONE expression rather than two copies that could drift.
    CROSS APPLY (
        SELECT DATEDIFF(
            DAY,
            CASE WHEN cf.effective_start_date > ms.month_start
                 THEN cf.effective_start_date ELSE ms.month_start END,
            CASE WHEN cf.effective_cancel_excl IS NULL
                      OR DATEADD(DAY, -1, cf.effective_cancel_excl) > EOMONTH(ms.month_start)
                 THEN EOMONTH(ms.month_start)
                 ELSE DATEADD(DAY, -1, cf.effective_cancel_excl) END
        ) + 1 AS days_occupied
    ) w
    GROUP BY ms.month_start, cf.location_source_id
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
    -- DAY-WEIGHTED basis (2026-08-31): desk-days ÷ (capacity × days in month).
    -- See desk_days_by_month above. The old month-end-position percentage is
    -- kept as occupancy_pct_eom (last column) for reconciliation.
    CAST(
        100.0 * ISNULL(dd.occupied_desk_days, 0)
        / NULLIF(ISNULL(lc.total_workstation_capacity, 0) * DAY(EOMONTH(ms.month_start)), 0)
        AS DECIMAL(9,4)
    )                                           AS occupancy_pct,

    -- Revenue — includes ALL desk types (private offices, dedicated desks,
    -- hot desks). The landlord still wants the total revenue picture.
    -- MONTH-END basis: a contract alive on the last day bills a full month, one
    -- cancelled mid-month bills nothing. See sold_revenue_day_weighted below.
    ISNULL(ma.sold_monthly_revenue, 0)          AS sold_monthly_revenue,
    ISNULL(ma.list_monthly_revenue, 0)          AS list_monthly_revenue,
    -- DAY-WEIGHTED basis (2026-09-01): fee × days live ÷ days in month, over
    -- the same window as occupancy_pct. This is what the invoices actually bill.
    CAST(ISNULL(dd.sold_revenue_day_weighted, 0) AS DECIMAL(18,2))
                                                AS sold_revenue_day_weighted,

    -- ── Per-workstation pricing KPIs (PRIVATE OFFICES ONLY) ──────────────
    -- PLAIN per-tenant average: each PO member's own €/desk rate is computed
    -- first (net sold ÷ that member's PO desks), then those rates are averaged
    -- giving every tenant equal weight. This matches the "Average" row at the
    -- bottom of the dashboard popup. "Net sold" = member's gross pure-PO sold +
    -- their own negative-fee adjustment contracts (Nexudus stores discounts as
    -- separate negative rows). Fully-comped members (net = 0) are dropped so
    -- their desks don't deflate the average.
    CAST(ISNULL(pk.avg_sold_rate, 0) AS DECIMAL(18,2)) AS avg_sold_price_per_ws,
    CAST(ISNULL(pk.avg_list_rate, 0) AS DECIMAL(18,2)) AS avg_list_price_per_ws,
    CAST(ISNULL(pk.avg_discount_rate, 0) AS DECIMAL(9,4)) AS avg_discount_pct,
    CAST(
        ISNULL(pk.po_list_total, 0) - ISNULL(pk.po_net_sold, 0)
        AS DECIMAL(18,2)
    )                                           AS discount_monthly_value,

    -- Transparency / drill-down: how many private-office contracts + desks
    -- feed the averages above. Lets the UI show "Avg based on N private
    -- offices" so a small-sample KPI isn't misread.
    ISNULL(ma.private_office_contract_count, 0) AS private_office_contract_count,
    ISNULL(ma.private_office_capacity, 0)       AS private_office_capacity,
    CAST(ISNULL(ma.private_office_sold_revenue, 0) AS DECIMAL(18,2))
                                                AS private_office_sold_revenue,
    CAST(ISNULL(ma.private_office_list_revenue, 0) AS DECIMAL(18,2))
                                                AS private_office_list_revenue,

    -- Workstation flow
    ISNULL(ma.new_workstations_starting, 0)     AS new_workstations_starting,
    ISNULL(ma.workstations_cancelling, 0)       AS workstations_cancelling,
    ISNULL(ma.new_workstations_starting, 0)
        - ISNULL(ma.workstations_cancelling, 0) AS net_workstation_change,

    -- Data quality
    ISNULL(ma.contracts_missing_list_price, 0)  AS contracts_missing_list_price,

    -- Negative-fee adjustment contracts (discount / credit lines). The negative
    -- value is already netted into sold_monthly_revenue above; surfaced here for
    -- QA / dashboard transparency.
    ISNULL(ma.adjustment_contract_count, 0)     AS adjustment_contract_count,
    CAST(ISNULL(ma.adjustment_monthly_value, 0) AS DECIMAL(18,2)) AS adjustment_monthly_value,

    N'contract_book'                            AS calculation_basis,

    -- Day-weighted occupancy detail (2026-08-31)
    ISNULL(dd.occupied_desk_days, 0)            AS occupied_desk_days,
    DAY(EOMONTH(ms.month_start))                AS days_in_month,
    CAST(
        1.0 * ISNULL(dd.occupied_desk_days, 0) / DAY(EOMONTH(ms.month_start))
        AS DECIMAL(9,2)
    )                                           AS occupied_ws_avg,
    CAST(
        100.0 * ISNULL(ma.occupied_workstations, 0)
        / NULLIF(ISNULL(lc.total_workstation_capacity, 0), 0)
        AS DECIMAL(9,4)
    )                                           AS occupancy_pct_eom

FROM month_spine ms
CROSS JOIN location_list ll
LEFT JOIN location_capacity lc
    ON  lc.location_source_id = ll.location_source_id
    AND lc.month_start        = ms.month_start
LEFT JOIN monthly_agg ma
    ON  ma.month_start        = ms.month_start
    AND ma.location_source_id = ll.location_source_id
LEFT JOIN po_kpi_monthly pk
    ON  pk.month_start        = ms.month_start
    AND pk.location_source_id = ll.location_source_id
LEFT JOIN desk_days_by_month dd
    ON  dd.month_start        = ms.month_start
    AND dd.location_source_id = ll.location_source_id;
GO


-- =============================================================================
-- 2b. gold.vw_landlord_monthly_contract_detail
--     One row per (location, month, contract) for ±12 months. Lets the
--     dashboard drill into a single bar of vw_landlord_contract_book_monthly
--     and see which contracts make up that month's revenue and occupancy.
--
--     Active-in-month rule is IDENTICAL to vw_landlord_contract_book_monthly's
--     active_by_month CTE — so a SUM over this view by (location, period)
--     reconciles exactly with the contract-book totals. Don't change the rule
--     in one place without changing it in the other.
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_monthly_contract_detail
AS
WITH month_offsets AS (
    -- Window widened to -24 (2026-09-01): the period selector offers 13 months
    -- back, and each of those must still be able to show a full 12 months of
    -- history BEFORE it. At -12 the spine rolled with today, so on 1 Sep 2026
    -- Aug-2025 dropped out and the Past-12 chart for Aug-2026 lost a bar.
    SELECT -24 AS n
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
        SUM(ISNULL(p.price, 0)) AS list_monthly_fee,
        SUM(
            CASE WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1) ELSE 0 END
        ) AS private_office_capacity,
        CASE
            WHEN SUM(CASE WHEN p.item_type IN (2, 3) THEN 1 ELSE 0 END) = 0
             AND SUM(CASE WHEN p.item_type = 1 THEN 1 ELSE 0 END) > 0
                THEN 1
            ELSE 0
        END AS is_pure_private_office
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
        c.source_id                       AS contract_source_id,
        c.location_source_id,
        COALESCE(
            NULLIF(c.coworker_company, N''),
            c.coworker_billing_name,
            c.coworker_name
        )                                 AS member_company_name,
        c.coworker_name,
        c.tariff_name,
        COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) AS sold_monthly_fee,
        ISNULL(pl.list_monthly_fee, 0)    AS list_monthly_fee,
        pl.capacity,
        ISNULL(pl.private_office_capacity, 0) AS private_office_capacity,
        ISNULL(pl.is_pure_private_office, 0)  AS is_pure_private_office,
        CAST(c.start_date AS DATE)        AS start_date,
        CAST(c.cancellation_date AS DATE) AS cancellation_date,
        CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) AS effective_start_date,
        CASE
            WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0 THEN 1
            ELSE 0
        END                               AS is_negative_adjustment
    FROM silver.nexudus_contracts c
    LEFT JOIN contract_product_link pl
        ON pl.contract_source_id = c.source_id
    WHERE c.is_deleted = 0
      AND c.start_date IS NOT NULL
      AND (
          c.active = 1
          OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
          OR (
              c.active = 0
              AND c.cancelled = 0
              AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
          )
      )
      -- Product-link filter — mirrors vw_landlord_contract_book_monthly.
      -- See that view for the rationale on each branch.
      AND (
          pl.contract_source_id IS NOT NULL
          OR COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
          OR (
              c.active = 0
              AND c.cancelled = 0
              AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
              AND COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) > 0
          )
      )
)
SELECT
    FORMAT(ms.month_start, 'yyyy-MM')          AS period,
    ms.month_start                              AS month_start_date,
    cf.location_source_id,
    cf.contract_source_id,
    cf.member_company_name,
    cf.coworker_name,
    cf.tariff_name,
    ISNULL(cf.capacity, 0)                      AS capacity,
    cf.private_office_capacity,
    cf.is_pure_private_office,
    -- Flag for unlinked future contracts (no floor_plan_desk_ids but a positive
    -- fee committed for a future month). Lets the drill-down modal surface
    -- "Desks not linked yet — confirm with ops" without re-deriving the filter.
    CASE
        WHEN cf.capacity IS NULL
         AND cf.sold_monthly_fee > 0
         AND cf.is_pure_private_office = 0
         AND cf.private_office_capacity = 0
         AND cf.effective_start_date > CAST(GETUTCDATE() AS DATE)
            THEN 1
        ELSE 0
    END                                         AS is_unlinked_future,
    CAST(cf.sold_monthly_fee AS DECIMAL(18,2))  AS sold_monthly_fee,
    CAST(cf.list_monthly_fee AS DECIMAL(18,2))  AS list_monthly_fee,
    CAST(
        cf.sold_monthly_fee / NULLIF(cf.capacity, 0)
        AS DECIMAL(18,2)
    )                                           AS sold_price_per_ws,
    CAST(
        cf.list_monthly_fee / NULLIF(cf.capacity, 0)
        AS DECIMAL(18,2)
    )                                           AS list_price_per_ws,
    cf.is_negative_adjustment,
    cf.start_date,
    cf.cancellation_date,
    -- Flag the contract relative to THIS month so the drill-down UI can
    -- annotate "started this month" or "leaving end of month" pills.
    CASE
        WHEN cf.effective_start_date >= ms.month_start
         AND cf.effective_start_date <= EOMONTH(ms.month_start) THEN 1
        ELSE 0
    END                                         AS is_new_this_month,
    CASE
        WHEN cf.cancellation_date IS NOT NULL
         AND cf.cancellation_date <= EOMONTH(DATEADD(MONTH, 1, ms.month_start)) THEN 1
        ELSE 0
    END                                         AS is_cancelling_this_month
FROM month_spine ms
INNER JOIN contract_facts cf
    -- Same active-in-month rule as vw_landlord_contract_book_monthly so totals
    -- reconcile. See MONTH-END CANCELLATION CONVENTION in the file header.
    ON  cf.effective_start_date <= EOMONTH(ms.month_start)
    AND (
        cf.cancellation_date IS NULL
        OR cf.cancellation_date >= EOMONTH(ms.month_start)
    );
GO


-- =============================================================================
-- 3. gold.vw_landlord_pricing_summary
--    One row per location for the current month — fast KPI reads for the dashboard.
--    Aggregates gold.vw_landlord_current_contracts.
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_pricing_summary
AS
-- ── Per-member roll-up FIRST (member_company_name × location) ─────────────────
-- Lets us filter by "net sold > 0 per member" before the location-level
-- aggregate, so fully-comped members (Elusis at Herengracht etc.) drop their
-- desks from both numerator and denominator. Matches what the dashboard
-- popup shows row-by-row.
WITH per_member AS (
    SELECT
        location_source_id,
        location_name,
        location_city,
        location_country_name,
        member_company_name,
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0
                 THEN sold_monthly_fee ELSE 0 END) AS po_gross_sold,
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0
                 THEN ISNULL(list_monthly_fee, 0) ELSE 0 END) AS po_list,
        SUM(CASE WHEN is_pure_private_office = 1 AND sold_monthly_fee <> 0
                 THEN capacity ELSE 0 END) AS po_capacity,
        -- Only net adjustments whose tariff is a "Membership Fee" account.
        -- Parking / ancillary discounts (e.g. Degura's €600 parking discount
        -- at QH) are on different financial accounts and would otherwise
        -- pollute the membership-fee PO sold side.
        SUM(CASE WHEN is_negative_adjustment = 1
                  AND is_membership_fee_account = 1
                 THEN sold_monthly_fee ELSE 0 END) AS adj_sold
    FROM gold.vw_landlord_current_contracts
    GROUP BY
        location_source_id, location_name, location_city, location_country_name,
        member_company_name
),
-- Members with at least one pure-PO contract AND positive net sold after
-- their own discounts. Members with no PO, or net = 0 (fully comped), don't
-- contribute to the per-WS PO benchmark.
po_members AS (
    SELECT *,
           po_gross_sold + adj_sold AS net_sold
    FROM per_member
    WHERE po_capacity > 0
      AND (po_gross_sold + adj_sold) > 0
),
-- Location-level aggregates for revenue / occupancy (ALL contracts, not just PO).
loc_totals AS (
    SELECT
        location_source_id,
        location_name,
        location_city,
        location_country_name,
        CAST(SUM(sold_monthly_fee)            AS DECIMAL(18,2)) AS sold_monthly_revenue,
        CAST(SUM(ISNULL(list_monthly_fee,0))  AS DECIMAL(18,2)) AS list_monthly_revenue,
        SUM(ISNULL(capacity, 0))                                AS occupied_workstations,
        SUM(CASE WHEN is_negative_adjustment = 0 THEN 1 ELSE 0 END) AS active_contract_count,
        SUM(CASE WHEN is_pure_private_office = 1 AND is_negative_adjustment = 0 THEN 1 ELSE 0 END) AS private_office_contract_count,
        SUM(CASE WHEN is_pure_private_office = 1 THEN capacity ELSE 0 END)        AS private_office_capacity,
        SUM(list_price_missing)                                 AS contracts_missing_list_price,
        SUM(is_negative_adjustment)                             AS adjustment_contract_count,
        CAST(SUM(CASE WHEN is_negative_adjustment = 1 THEN sold_monthly_fee ELSE 0 END) AS DECIMAL(18,2)) AS adjustment_monthly_value,
        CAST(
            100.0 * SUM(CASE WHEN is_negative_adjustment = 0 AND list_price_missing = 0 THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN is_negative_adjustment = 0 THEN 1 ELSE 0 END), 0)
            AS DECIMAL(9,4)
        )                                                       AS product_match_coverage_pct
    FROM gold.vw_landlord_current_contracts
    GROUP BY location_source_id, location_name, location_city, location_country_name
)
SELECT
    lt.location_source_id,
    lt.location_name,
    lt.location_city,
    lt.location_country_name,
    FORMAT(DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1), 'yyyy-MM') AS period,

    -- Revenue: includes ALL desk types
    lt.sold_monthly_revenue,
    lt.list_monthly_revenue,

    -- ── Per-WS pricing KPIs (PRIVATE OFFICES ONLY) ───────────────────────
    -- PLAIN per-tenant average: average each PO member's own €/desk rate so
    -- every tenant counts equally regardless of office size. Matches the
    -- "Average" row at the bottom of the dashboard popup.
    CAST(
        ISNULL((SELECT AVG(CAST(net_sold AS FLOAT) / NULLIF(po_capacity, 0))
                  FROM po_members pm WHERE pm.location_source_id = lt.location_source_id), 0)
        AS DECIMAL(18,2)
    ) AS avg_sold_price_per_ws,
    CAST(
        ISNULL((SELECT AVG(CAST(po_list AS FLOAT) / NULLIF(po_capacity, 0))
                  FROM po_members pm WHERE pm.location_source_id = lt.location_source_id), 0)
        AS DECIMAL(18,2)
    ) AS avg_list_price_per_ws,
    CAST(
        ISNULL((SELECT AVG(CASE WHEN po_list > 0
                                THEN (CAST(po_list AS FLOAT) - net_sold) / po_list
                           END)
                  FROM po_members pm WHERE pm.location_source_id = lt.location_source_id), 0)
        AS DECIMAL(9,4)
    ) AS avg_discount_pct,
    CAST(
        ISNULL((SELECT SUM(po_list)  FROM po_members pm WHERE pm.location_source_id = lt.location_source_id), 0)
        - ISNULL((SELECT SUM(net_sold) FROM po_members pm WHERE pm.location_source_id = lt.location_source_id), 0)
        AS DECIMAL(18,2)
    ) AS discount_monthly_value,

    -- Transparency
    lt.private_office_contract_count,
    lt.private_office_capacity,
    lt.occupied_workstations,
    lt.active_contract_count,
    lt.product_match_coverage_pct,
    lt.contracts_missing_list_price,
    lt.adjustment_contract_count,
    lt.adjustment_monthly_value,

    CAST(GETUTCDATE() AS DATE) AS last_refreshed_at
FROM loc_totals lt;
GO


-- =============================================================================
-- 4. gold.vw_landlord_current_companies
--    One row per location + member_company_name for current-month contracts.
--    Aggregates gold.vw_landlord_current_contracts so the dashboard reads one
--    row per company instead of one row per contract/desk component.
--
--    Filter: location must be known AND contract must have capacity > 0 OR
--    sold_monthly_fee > 0 — removes zero-value admin/placeholder rows.
--
--    Status priority: notice_period > active > paused > inactive.
--    Pricing is re-derived from summed totals, not averaged from contract rows.
--
--    FOLLOW-UP CONTRACT DETECTION (added 2026-05-27):
--      Nexudus stores renewals as separate contracts: the current contract has
--      cancellation_date = X, and a new contract with start_date = X (or later)
--      replaces it. The aggregate's MAX(cancellation_date) used to show "X" as
--      the company's termination date even when a follow-up was already signed,
--      misleading dashboards into showing "terminating" for renewing members.
--
--      The view now exposes:
--        has_followup_contract       BIT  -- 1 if any signed follow-up exists
--        followup_contract_count     INT
--        followup_total_monthly_fee  DECIMAL
--        latest_followup_end_date    DATE -- effective end across all follow-ups
--        effective_end_date          DATE -- = latest_followup_end_date when a
--                                              follow-up exists, else cancellation_date
--      Dashboards should render `effective_end_date` (or hide cancellation_date
--      entirely when has_followup_contract = 1) instead of the raw aggregate
--      cancellation_date.
--
--    CONTRACT VALUE AGGREGATION (fixed 2026-05-27):
--      `contract_value` and `remaining_contract_value` now include the value
--      of follow-up renewals (gap = 0) AND re-engagements (gap > 0) in
--      addition to the contracts currently active. Previously they only
--      summed contracts in vw_landlord_current_contracts, which excluded
--      future-signed renewals — so a company with one active contract and
--      one renewal booked underreported their committed value.
--      Breakdown columns let dashboards split the total:
--        current_contract_value / current_remaining_value
--        followup_contract_value / followup_remaining_value
--        re_engagement_contract_value / re_engagement_remaining_value
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_current_companies
AS
WITH base AS (
    SELECT
        location_source_id,
        location_name,
        location_city,
        location_country_name,
        member_company_name,

        -- Date range across all contracts for this company at this location.
        -- has_open_ended_current_contract: 1 when any positive-fee contract
        -- has no cancellation_date. We use it later to decide whether to
        -- suppress effective_end_date — a company with at least one open-ended
        -- office is NOT terminating, even if their discount line has a date
        -- (e.g. ADP Nederland: two offices through 2028 + a discount ending July).
        MAX(CASE
                WHEN cancellation_date IS NULL AND sold_monthly_fee > 0 THEN 1
                ELSE 0
            END)                                                AS has_open_ended_current_contract,
        MIN(start_date)                                         AS start_date,
        MAX(cancellation_date)                                  AS cancellation_date,
        MAX(contract_end_date)                                  AS contract_end_date,

        -- Aggregated workstations and revenue.
        -- sold_monthly_fee / list_monthly_fee / discount_value are filtered
        -- to is_membership_fee_account = 1 so the membership-schedule row
        -- shows ONLY the member's membership-fee revenue (matches what's
        -- in the Nexudus pivot export). Parking / Ancillary / Business
        -- Address contracts have their own financial accounts and don't
        -- belong in this row. Without this filter, the asymmetry between
        -- a +€600 parking contract (dropped by the capacity filter
        -- upstream) and a -€600 parking discount (kept by the price<0
        -- branch) would produce wrong nets (e.g. Degura at QH).
        SUM(ISNULL(capacity, 0))                                AS capacity,
        SUM(CASE WHEN is_membership_fee_account = 1 THEN sold_monthly_fee ELSE 0 END) AS sold_monthly_fee,
        SUM(CASE WHEN is_membership_fee_account = 1 THEN list_monthly_fee ELSE 0 END) AS list_monthly_fee,
        SUM(CASE WHEN is_membership_fee_account = 1 THEN discount_value   ELSE 0 END) AS discount_value,
        SUM(contract_value)                                     AS contract_value,
        SUM(remaining_contract_value)                           AS remaining_contract_value,

        -- ── Per-type revenue split (MMRF / MARV, added 2026-07-16) ───────────
        -- Allocated by product list-price share upstream in
        -- vw_landlord_current_contracts, so these are NOT financial-account
        -- filtered — they reconcile as mmrf + marv = SUM(sold_monthly_fee over
        -- ALL the company's current contracts, all types).
        --   MMRF (Monthly Membership Revenues Fee) = types 1+2+3
        --   MARV (Monthly Additional Revenues Fee) = type 4 (recurring; ad-hoc
        --         meeting-room/day-pass MARV is invoice-sourced elsewhere)
        SUM(ISNULL(rev_private_office, 0) + ISNULL(rev_dedicated_desk, 0) + ISNULL(rev_hot_desk, 0)) AS mmrf,
        SUM(ISNULL(rev_additional, 0))                          AS marv,
        SUM(ISNULL(dedicated_desk_capacity, 0))                 AS dedicated_desk_capacity,
        SUM(ISNULL(hot_desk_capacity, 0))                       AS hot_desk_capacity,
        SUM(ISNULL(additional_unit_count, 0))                   AS additional_unit_count,

        -- Notice / term info: take the most conservative (longest) values
        MAX(notice_period_months)                               AS notice_period_months,
        MAX(term_months)                                        AS term_months,
        MAX(days_until_cancellation)                            AS days_until_cancellation,

        -- Data quality
        SUM(product_match_count)                                AS product_match_count,
        SUM(CASE WHEN list_price_missing = 1 THEN 1 ELSE 0 END) AS contracts_missing_list_price,

        -- Company-level status: highest-priority status across all contracts wins
        CASE
            WHEN MAX(CASE WHEN status = N'notice_period' THEN 3 ELSE 0 END) > 0 THEN N'notice_period'
            WHEN MAX(CASE WHEN status = N'active'        THEN 2 ELSE 0 END) > 0 THEN N'active'
            WHEN MAX(CASE WHEN status = N'paused'        THEN 1 ELSE 0 END) > 0 THEN N'paused'
            ELSE N'inactive'
        END                                                     AS status

    FROM gold.vw_landlord_current_contracts
    WHERE location_source_id IS NOT NULL
      -- A company can have a mix of positive desk contracts and negative-fee
      -- adjustment lines (discount / credit). Both are aggregated here so the
      -- company row shows true net revenue. Adjustment-only companies (no positive
      -- contract) will appear with capacity = 0 and a negative sold_monthly_fee —
      -- this is intentional, dashboards can filter by status / capacity if needed.
    GROUP BY
        location_source_id,
        location_name,
        location_city,
        location_country_name,
        member_company_name
),
-- Follow-up contracts: positive-fee silver contracts for the same
-- (location, company) that start on/after the company's current cancellation
-- and that are active or future-signed (not abandoned). We require positive fee
-- so pure discount/credit lines don't count as a "renewal".
--
-- Boundary semantics (no arbitrary threshold):
--   gap_days = 0 → Nexudus same-day cutover convention. The new contract takes
--                  over at exactly the moment the old one ends -- continuous
--                  occupancy. Treated as a RENEWAL.
--   gap_days > 0 → ANY gap, however small or large (1 day to 8+ months). The
--                  company genuinely exits and re-engages later. Treated as a
--                  RE-ENGAGEMENT. effective_end_date stays at cancellation_date
--                  so the dashboard correctly shows the company leaving; the
--                  return date is exposed separately via next_engagement_date.
--
-- The forecast chart (vw_landlord_contract_book_monthly) handles gap months
-- correctly out-of-the-box because each contract is only counted in its own
-- active months -- a 3-month gap shows up as 3 empty months in the chart,
-- whatever the gap length.
followup_candidates AS (
    SELECT
        b.location_source_id,
        b.member_company_name,
        b.cancellation_date,
        c.source_id                                                AS followup_contract_id,
        CAST(c.start_date AS DATE)                                 AS followup_start,
        DATEDIFF(DAY, b.cancellation_date, CAST(c.start_date AS DATE)) AS gap_days,
        COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) AS followup_fee,
        CAST(
            CASE
                WHEN c.cancellation_date IS NOT NULL THEN c.cancellation_date
                WHEN c.contract_term IS NOT NULL
                    AND CAST(c.contract_term AS DATE) >= CAST(GETUTCDATE() AS DATE)
                    THEN c.contract_term
                ELSE NULL
            END AS DATE
        )                                                          AS followup_end,
        CASE
            WHEN c.cancellation_date IS NULL
             AND (
                 c.contract_term IS NULL
                 OR CAST(c.contract_term AS DATE) < CAST(GETUTCDATE() AS DATE)
             )
            THEN 1 ELSE 0
        END                                                        AS is_open_ended,

        -- Per-contract value of the follow-up.
        -- Mirrors gold.vw_landlord_current_contracts.contract_value:
        --   - cancellation set       → fee × months(start → cancellation)
        --   - contract_term in future → fee × months(start → contract_term)
        --   - else (truly open-ended) → fee × 12  (12-month horizon assumption,
        --                                          NOT notice period — see the
        --                                          comment in vw_landlord_current_contracts)
        -- followup_contract_value reflects the FULL value of the follow-up
        -- (every month from its own start to its own end). The current
        -- contract's value is still tracked in `base.contract_value` — adding
        -- the two at the SELECT below gives the company's committed total.
        CAST(
            COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
            * CASE
                WHEN c.start_date IS NULL    THEN 0
                WHEN c.cancellation_date IS NOT NULL
                    THEN DATEDIFF(MONTH, CAST(c.start_date AS DATE), CAST(c.cancellation_date AS DATE))
                WHEN c.contract_term IS NOT NULL
                 AND CAST(c.contract_term AS DATE) >= CAST(c.start_date AS DATE)
                    THEN DATEDIFF(MONTH, CAST(c.start_date AS DATE), CAST(c.contract_term AS DATE))
                ELSE 12
              END
            AS DECIMAL(18,2)
        )                                                          AS followup_contract_value,

        -- Per-contract remaining value of the follow-up from TODAY forward.
        -- For a future-dated follow-up the months window starts at its own
        -- start_date (not today) so the gap doesn't inflate the number.
        --   - cancellation set        → months(today_or_start → cancellation)
        --   - contract_term in future → months(today_or_start → contract_term)
        --   - else (rolling)          → 12 (12-month forward horizon)
        CAST(
            COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
            * CASE
                WHEN c.cancellation_date IS NOT NULL
                    THEN CASE
                        WHEN CAST(c.cancellation_date AS DATE) < CAST(GETUTCDATE() AS DATE) THEN 0
                        ELSE DATEDIFF(
                            MONTH,
                            CASE
                                WHEN CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
                                    THEN CAST(c.start_date AS DATE)
                                ELSE CAST(GETUTCDATE() AS DATE)
                            END,
                            CAST(c.cancellation_date AS DATE)
                        )
                    END
                WHEN c.contract_term IS NOT NULL
                 AND CAST(c.contract_term AS DATE) >= CAST(GETUTCDATE() AS DATE)
                    THEN DATEDIFF(
                        MONTH,
                        CASE
                            WHEN CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
                                THEN CAST(c.start_date AS DATE)
                            ELSE CAST(GETUTCDATE() AS DATE)
                        END,
                        CAST(c.contract_term AS DATE)
                    )
                ELSE 12
              END
            AS DECIMAL(18,2)
        )                                                          AS followup_remaining_value
    FROM base b
    INNER JOIN silver.nexudus_contracts c
        ON  c.location_source_id = b.location_source_id
        AND COALESCE(NULLIF(c.coworker_company, N''), c.coworker_billing_name, c.coworker_name)
            = b.member_company_name
        AND c.is_deleted = 0
        AND COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) > 0
        AND CAST(c.start_date AS DATE) >= b.cancellation_date
        AND (
            c.active = 1
            OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
            OR (
                c.active = 0
                AND c.cancelled = 0
                AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
            )
        )
    WHERE b.cancellation_date IS NOT NULL
),
-- Renewals: gap_days = 0 only. These take over immediately and extend the
-- company's effective_end_date (continuous occupancy, no break).
followups AS (
    SELECT
        location_source_id,
        member_company_name,
        COUNT(*)                                              AS followup_contract_count,
        SUM(followup_fee)                                     AS followup_total_monthly_fee,
        SUM(followup_contract_value)                          AS followup_total_contract_value,
        SUM(followup_remaining_value)                         AS followup_total_remaining_value,
        MIN(followup_start)                                   AS earliest_followup_start,
        MAX(followup_end)                                     AS latest_followup_end_date,
        MAX(is_open_ended)                                    AS has_open_ended_followup
    FROM followup_candidates
    WHERE gap_days = 0
    GROUP BY location_source_id, member_company_name
),
-- Re-engagements: gap_days >= 1, ANY gap regardless of length. Surfaced
-- separately as next_engagement_date so the dashboard can show e.g.
-- "Terminating 01/06/2026 — returning 01/09/2026 (3-month gap)" -- without
-- pretending the occupancy is continuous.
re_engagements AS (
    SELECT
        location_source_id,
        member_company_name,
        COUNT(*)                                              AS re_engagement_contract_count,
        SUM(followup_fee)                                     AS re_engagement_total_monthly_fee,
        SUM(followup_contract_value)                          AS re_engagement_total_contract_value,
        SUM(followup_remaining_value)                         AS re_engagement_total_remaining_value,
        MIN(followup_start)                                   AS next_engagement_date,
        MIN(gap_days)                                         AS next_engagement_gap_days,
        MAX(followup_end)                                     AS latest_re_engagement_end_date
    FROM followup_candidates
    WHERE gap_days >= 1
    GROUP BY location_source_id, member_company_name
)
SELECT
    b.location_source_id,
    b.location_name,
    b.location_city,
    b.location_country_name,
    b.member_company_name,

    b.start_date,
    b.cancellation_date,
    b.contract_end_date,

    b.capacity,
    b.sold_monthly_fee,
    b.list_monthly_fee,
    b.discount_value,

    -- ── Per-type revenue split (MMRF / MARV, added 2026-07-16) ───────────────
    -- MMRF (Monthly Membership Revenues Fee) = types 1+2+3 (office/dedicated/hot)
    -- MARV (Monthly Additional Revenues Fee) = type 4 (parking/storeroom, recurring)
    -- mmrf_per_ws = MMRF ÷ workstations (b.capacity = types 1+2+3 desks).
    -- Contract Value / Remaining above stay TOTAL across all types (unchanged).
    ISNULL(b.mmrf, 0)                                          AS mmrf,
    ISNULL(b.marv, 0)                                          AS marv,
    CAST(ISNULL(b.mmrf, 0) / NULLIF(b.capacity, 0) AS DECIMAL(18,2)) AS mmrf_per_ws,
    ISNULL(b.dedicated_desk_capacity, 0)                      AS dedicated_desk_capacity,
    ISNULL(b.hot_desk_capacity, 0)                            AS hot_desk_capacity,
    ISNULL(b.additional_unit_count, 0)                        AS additional_unit_count,

    -- ── Contract value (FIXED 2026-05-27 to include follow-ups + re-engagements) ──
    -- Historically `contract_value` and `remaining_contract_value` exposed only
    -- the value of contracts currently in vw_landlord_current_contracts — i.e.
    -- the contracts that are active or in their final billable month TODAY.
    -- That excluded:
    --   1. Continuous renewals (gap = 0) — booked but not yet active.
    --   2. Re-engagements (gap > 0)      — booked but on the other side of a gap.
    -- The dashboard now needs the FULL booked-and-committed value per company,
    -- so the headline columns are now the sum of current + renewals + re-engagements.
    -- The breakdown columns below let the UI show "€480k (€420k current + €60k future)".
    b.contract_value
      + ISNULL(f.followup_total_contract_value, 0)
      + ISNULL(re.re_engagement_total_contract_value, 0)             AS contract_value,
    b.remaining_contract_value
      + ISNULL(f.followup_total_remaining_value, 0)
      + ISNULL(re.re_engagement_total_remaining_value, 0)            AS remaining_contract_value,

    -- Breakdown components — same denominators as the totals above, split so the
    -- dashboard can attribute "what's current vs what's future" in tooltips.
    b.contract_value                                                  AS current_contract_value,
    b.remaining_contract_value                                        AS current_remaining_value,
    ISNULL(f.followup_total_contract_value, 0)                        AS followup_contract_value,
    ISNULL(f.followup_total_remaining_value, 0)                       AS followup_remaining_value,
    ISNULL(re.re_engagement_total_contract_value, 0)                  AS re_engagement_contract_value,
    ISNULL(re.re_engagement_total_remaining_value, 0)                 AS re_engagement_remaining_value,

    b.notice_period_months,
    b.term_months,
    b.days_until_cancellation,

    b.product_match_count,
    b.contracts_missing_list_price,

    -- Derived per-WS pricing from aggregated totals (NOT averaged from rows)
    CAST(
        b.sold_monthly_fee
        / NULLIF(b.capacity, 0)
        AS DECIMAL(18,2)
    )                                                       AS sold_price_per_ws,
    CAST(
        b.list_monthly_fee
        / NULLIF(b.capacity, 0)
        AS DECIMAL(18,2)
    )                                                       AS list_price_per_ws,
    CAST(
        b.discount_value
        / NULLIF(b.list_monthly_fee, 0)
        AS DECIMAL(9,4)
    )                                                       AS discount_pct,

    b.status,

    -- ── Follow-up contract surfacing (added 2026-05-27) ────────────────────
    b.has_open_ended_current_contract,
    -- has_followup_contract = 1 only for continuous renewals (gap ≤ 7 days).
    -- For re-engagements across a gap, see next_engagement_date below.
    CASE WHEN f.followup_contract_count > 0 THEN 1 ELSE 0 END  AS has_followup_contract,
    ISNULL(f.followup_contract_count, 0)                       AS followup_contract_count,
    ISNULL(f.followup_total_monthly_fee, 0)                    AS followup_total_monthly_fee,
    f.earliest_followup_start,
    f.latest_followup_end_date,

    -- Re-engagement: the company is leaving but has another contract booked
    -- to start after a gap of any length (1 day to 8+ months). Dashboards
    -- should show this as "Terminating [cancellation_date] — returning
    -- [next_engagement_date]". During the gap months the forecast chart
    -- naturally shows zero contribution from this company, which is correct.
    re.next_engagement_date,
    re.next_engagement_gap_days,
    ISNULL(re.re_engagement_contract_count, 0)              AS re_engagement_contract_count,
    ISNULL(re.re_engagement_total_monthly_fee, 0)           AS re_engagement_total_monthly_fee,
    re.latest_re_engagement_end_date,
    CASE WHEN re.next_engagement_date IS NOT NULL THEN 1 ELSE 0 END
                                                              AS has_re_engagement,
    -- Convenience label so dashboards can render category strings without
    -- their own decision tree:
    --   'ongoing'        -- open-ended current contract (e.g. ADP)
    --   'renewing'       -- continuous renewal (gap=0) extends end date (e.g. Allianz, RxSight)
    --   'returning'      -- has re-engagement after a gap (e.g. TransferGo)
    --   'terminating'    -- has cancellation_date, no follow-up at all
    --   'active'         -- no cancellation_date, no relevant follow-up info
    CASE
        WHEN b.has_open_ended_current_contract = 1                                THEN N'ongoing'
        WHEN ISNULL(f.has_open_ended_followup, 0) = 1 OR f.followup_contract_count > 0 THEN N'renewing'
        WHEN re.next_engagement_date IS NOT NULL                                  THEN N'returning'
        WHEN b.cancellation_date IS NOT NULL                                      THEN N'terminating'
        ELSE N'active'
    END                                                       AS lifecycle_state,

    -- effective_end_date: what dashboards should display as "termination".
    --   - Open-ended current contract (e.g. office without cancellation)  → NULL
    --   - Continuous renewal that's open-ended (no cancellation, no term) → NULL
    --   - Continuous renewal with known end                                → latest_followup_end_date
    --   - Otherwise (including re-engagement across a gap)                 → cancellation_date
    --     because the company IS leaving, even if they'll return later.
    CASE
        WHEN b.has_open_ended_current_contract = 1               THEN NULL
        WHEN ISNULL(f.has_open_ended_followup, 0) = 1            THEN NULL
        WHEN f.latest_followup_end_date IS NOT NULL              THEN f.latest_followup_end_date
        ELSE b.cancellation_date
    END                                                        AS effective_end_date,

    CAST(GETUTCDATE() AS DATE)                                 AS last_refreshed_at

FROM base b
LEFT JOIN followups f
    ON  f.location_source_id  = b.location_source_id
    AND f.member_company_name = b.member_company_name
LEFT JOIN re_engagements re
    ON  re.location_source_id  = b.location_source_id
    AND re.member_company_name = b.member_company_name;
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

-- ── QA 9: Future-signed contracts appearing in the monthly forecast ──────────
-- Contracts with start_date > today should appear in vw_landlord_contract_book_monthly
-- from their start month forward, regardless of their active flag. They must NOT
-- appear in vw_landlord_current_contracts.
/*
WITH future_contracts AS (
    SELECT TOP 20
        c.source_id     AS contract_source_id,
        loc.name        AS location_name,
        c.coworker_name,
        CAST(c.start_date AS DATE)        AS start_date,
        CAST(c.cancellation_date AS DATE) AS cancellation_date,
        c.active,
        c.cancelled
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.is_deleted = 0
      AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
      AND c.cancelled = 0
    ORDER BY c.start_date
)
SELECT
    fc.contract_source_id,
    fc.location_name,
    fc.coworker_name,
    fc.start_date,
    fc.active,
    -- Should be present in the monthly view from start_date onward
    EXISTS(
        SELECT 1 FROM gold.vw_landlord_contract_book_monthly m
        WHERE m.location_source_id = (SELECT location_source_id FROM silver.nexudus_contracts WHERE source_id = fc.contract_source_id)
          AND m.month_start_date >= DATEFROMPARTS(YEAR(fc.start_date), MONTH(fc.start_date), 1)
    ) AS in_monthly_forecast,
    -- Should be ABSENT from current contracts (start_date is in the future)
    EXISTS(
        SELECT 1 FROM gold.vw_landlord_current_contracts cc
        WHERE cc.contract_source_id = fc.contract_source_id
    ) AS in_current_contracts
FROM future_contracts fc;
-- Expect: in_monthly_forecast = 1, in_current_contracts = 0 for all rows.
*/

-- ── QA 10: Negative-fee (discount / credit) adjustment contracts ─────────────
-- Inventory of negative contracts and their effect on the current month.
/*
SELECT
    location_name,
    member_company_name,
    contract_source_id,
    capacity,
    sold_monthly_fee,
    list_monthly_fee,
    is_negative_adjustment,
    status
FROM gold.vw_landlord_current_contracts
WHERE is_negative_adjustment = 1
ORDER BY sold_monthly_fee ASC;

-- Net adjustment value per location for the current month
SELECT
    location_name,
    sold_monthly_revenue,
    adjustment_contract_count,
    adjustment_monthly_value
FROM gold.vw_landlord_pricing_summary
WHERE adjustment_contract_count > 0
ORDER BY adjustment_monthly_value;
*/

SELECT
  member_company_name,
  contract_source_id,
  capacity,
  sold_monthly_fee,
  list_monthly_fee,
  list_price_missing,
  product_match_count
FROM gold.vw_landlord_current_contracts
WHERE location_source_id = 1414964753



-- 1. Add silver columns
IF COL_LENGTH('silver.nexudus_coworker_invoices', 'invoice_status') IS NULL
    ALTER TABLE silver.nexudus_coworker_invoices ADD invoice_status NVARCHAR(64) NULL;
GO

IF COL_LENGTH('silver.nexudus_coworker_invoices', 'processing') IS NULL
    ALTER TABLE silver.nexudus_coworker_invoices ADD processing BIT NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworker_invoices_processing'
      AND object_id = OBJECT_ID('silver.nexudus_coworker_invoices')
)
    CREATE INDEX ix_silver_nexudus_coworker_invoices_processing
    ON silver.nexudus_coworker_invoices (processing, invoice_status);
GO

-- 2. Add gold worklist columns
IF COL_LENGTH('gold.finance_dashboard_invoice_worklist', 'invoice_status') IS NULL
    ALTER TABLE gold.finance_dashboard_invoice_worklist ADD invoice_status NVARCHAR(64) NULL;
GO

IF COL_LENGTH('gold.finance_dashboard_invoice_worklist', 'processing') IS NULL
    ALTER TABLE gold.finance_dashboard_invoice_worklist
    ADD processing BIT NOT NULL
        CONSTRAINT df_gold_finance_dashboard_invoice_worklist_processing DEFAULT 0;
GO



UPDATE s
SET
    due_date = CAST(
        (TRY_CONVERT(DATETIME2, REPLACE(JSON_VALUE(b.raw_json, '$.DueDate'), 'Z', ''), 126)
            AT TIME ZONE 'UTC'
            AT TIME ZONE 'Central European Standard Time') AS DATETIME2
    ),
    invoice_status = COALESCE(
        NULLIF(JSON_VALUE(b.raw_json, '$.Status'), ''),
        NULLIF(JSON_VALUE(b.raw_json, '$.InvoiceStatus'), ''),
        NULLIF(JSON_VALUE(b.raw_json, '$.PaymentStatus'), '')
    ),
    processing = CASE
        WHEN UPPER(COALESCE(
            JSON_VALUE(b.raw_json, '$.Status'),
            JSON_VALUE(b.raw_json, '$.InvoiceStatus'),
            JSON_VALUE(b.raw_json, '$.PaymentStatus'),
            ''
        )) LIKE '%PROCESSING%' THEN 1
        ELSE 0
    END
FROM silver.nexudus_coworker_invoices s
JOIN bronze.nexudus_coworker_invoices b
    ON b.source_id = s.source_id
WHERE JSON_VALUE(b.raw_json, '$.DueDate') IS NOT NULL;
GO


SELECT TOP 1
    c.source_id, c.coworker_name, c.location_source_id,
    CAST(c.start_date AS DATE) AS start_date, c.active, c.cancelled
FROM silver.nexudus_contracts c
WHERE c.is_deleted = 0
  AND c.start_date > GETUTCDATE()
  AND c.active = 0 AND c.cancelled = 0
ORDER BY c.start_date;



SELECT invoice_number, due_date, workflow_type, invoice_status, processing
FROM gold.finance_dashboard_invoice_worklist
WHERE invoice_number IN ('GB-INV-2026.05-0188', 'GB-INV-2026.05-0186');


SELECT *
FROM gold.finance_dashboard_invoice_worklist
WHERE location_name = 'Amsterdam - Center - Herengracht 471'


EXEC gold.sp_refresh_finance_dashboard


SELECT COUNT(1) FROM gold.finance_dashboard_invoice_worklist