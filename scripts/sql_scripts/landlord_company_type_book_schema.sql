-- =============================================================================
-- landlord_company_type_book_schema.sql        (added 2026-07-16)
--
-- gold.vw_landlord_company_type_book_monthly
--   One row per (location, month, company) over -12..+24 months (37 total),
--   carrying per-Nexudus-item_type ALLOCATED sold revenue + workstation counts.
--
--   Powers the Strategic Partnership dashboard's new features:
--     * Stacked-by-type revenue/occupancy charts — SUM over companies per
--       (location, month) gives the per-type monthly series (T1/T2/T3/T4).
--     * 24-month per-company Cashflow table — cell = total_monthly_fee
--       (mmrf + marv) for that company in that month.
--     * Membership Schedule MMRF/MARV columns — current-month slice per company
--       (also available directly on gold.vw_landlord_current_companies).
--
--   BASIS: contract book (forward-looking, recurring). Nexudus stores ONE price
--   per contract, so per-type revenue is ALLOCATED by each type's product
--   list-price share (identical rule to gold.vw_landlord_current_contracts):
--     rev_typeN = sold_fee * list_typeN / list_monthly_fee
--   with a capacity-share fallback when list price is missing and an MMRF
--   residual for unlinked negative-fee adjustments — so the four rev_* columns
--   always reconcile to the contract's sold fee.
--
--   MMRF = types 1 (Private Office) + 2 (Dedicated Desk) + 3 (Hot Desk)
--   MARV = type 4 (Other: parking/storeroom, recurring). Type 5 (Meeting Room)
--          recurring is a data-quality exclusion (kept out of the product join);
--          ad-hoc meeting-room / day-pass MARV is past/billed only and comes
--          from the invoice-line stream view, NOT from this contract-book view.
--
--   Active-in-month, effective-start (+4h UTC shift), and future-signed rules
--   are identical to gold.vw_landlord_contract_book_monthly, so the two
--   reconcile: SUM(mmrf + marv) here = sold_monthly_revenue there per
--   (location, month). See that view's header for the MONTH-END CANCELLATION
--   CONVENTION rationale.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC sp_executesql N'CREATE SCHEMA gold';
GO


CREATE OR ALTER VIEW gold.vw_landlord_company_type_book_monthly
AS
WITH month_offsets AS (
    -- -12 .. +24 (37 rows, well within MAXRECURSION 100)
    SELECT -12 AS n
    UNION ALL
    SELECT n + 1 FROM month_offsets WHERE n < 24
),
month_spine AS (
    SELECT
        DATEADD(MONTH, n, DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)) AS month_start
    FROM month_offsets
),
-- Per-contract product resolution with per-type list-price + workstation
-- subtotals. Mirrors gold.vw_landlord_contract_book_monthly.contract_product_link
-- (item_type IN (1,2,3,4); 4 carries revenue but 0 desks; 5 excluded).
contract_product_link AS (
    SELECT
        c.source_id AS contract_source_id,
        SUM(CASE WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                 WHEN p.item_type IN (2, 3) THEN 1 ELSE 0 END)      AS capacity,
        SUM(ISNULL(p.price, 0))                                     AS list_monthly_fee,
        SUM(CASE WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1) ELSE 0 END) AS ws_po,
        SUM(CASE WHEN p.item_type = 2 THEN 1 ELSE 0 END)            AS ws_dd,
        SUM(CASE WHEN p.item_type = 3 THEN 1 ELSE 0 END)            AS ws_hd,
        SUM(CASE WHEN p.item_type = 4 THEN 1 ELSE 0 END)            AS ws_add,
        SUM(CASE WHEN p.item_type = 1 THEN ISNULL(p.price, 0) ELSE 0 END) AS list_po,
        SUM(CASE WHEN p.item_type = 2 THEN ISNULL(p.price, 0) ELSE 0 END) AS list_dd,
        SUM(CASE WHEN p.item_type = 3 THEN ISNULL(p.price, 0) ELSE 0 END) AS list_hd,
        SUM(CASE WHEN p.item_type = 4 THEN ISNULL(p.price, 0) ELSE 0 END) AS list_add
    FROM silver.nexudus_contracts c
    CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
    INNER JOIN silver.nexudus_products p
        ON  p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
        AND p.item_type IN (1, 2, 3, 4)
        AND p.is_deleted = 0
    WHERE TRIM(s.value) <> N''
      AND c.is_deleted = 0
    GROUP BY c.source_id
),
contract_facts AS (
    SELECT
        c.source_id                       AS contract_source_id,
        c.location_source_id,
        COALESCE(NULLIF(c.coworker_company, N''), c.coworker_billing_name, c.coworker_name) AS member_company_name,
        CAST(c.start_date        AS DATE)            AS start_date,
        CAST(c.cancellation_date AS DATE)            AS cancellation_date,
        CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) AS effective_start_date,
        -- +4h-shifted cancellation, EXCLUSIVE (first day NOT occupied) — the
        -- same convention the contract-book view uses for desk-days. Needed by
        -- the day-weighted revenue columns added 2026-09-01.
        CAST(DATEADD(HOUR, 4, c.cancellation_date) AS DATE) AS effective_cancel_excl,
        -- Per-type allocated revenue (list-price share; capacity-share fallback;
        -- MMRF residual for unlinked adjustments).
        CAST(alloc.sold_fee * alloc.w_po  AS DECIMAL(18,2)) AS rev_po,
        CAST(alloc.sold_fee * alloc.w_dd  AS DECIMAL(18,2)) AS rev_dd,
        CAST(alloc.sold_fee * alloc.w_hd  AS DECIMAL(18,2)) AS rev_hd,
        CAST(alloc.sold_fee * alloc.w_add AS DECIMAL(18,2)) AS rev_add,
        ISNULL(pl.ws_po, 0)  AS ws_po,
        ISNULL(pl.ws_dd, 0)  AS ws_dd,
        ISNULL(pl.ws_hd, 0)  AS ws_hd,
        ISNULL(pl.ws_add, 0) AS ws_add
    FROM silver.nexudus_contracts c
    LEFT JOIN contract_product_link pl
        ON pl.contract_source_id = c.source_id
    CROSS APPLY (
        SELECT
            CASE
                WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
                    THEN COALESCE(c.price, NULLIF(c.price_with_products, 0), c.tariff_price, 0)
                ELSE COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)
            END AS sold_fee,
            CASE
                WHEN ISNULL(pl.list_monthly_fee, 0) > 0 THEN ISNULL(pl.list_po, 0) / pl.list_monthly_fee
                WHEN ISNULL(pl.capacity, 0) > 0 THEN CAST(ISNULL(pl.ws_po, 0) AS FLOAT) / pl.capacity
                ELSE 1.0
            END AS w_po,
            CASE
                WHEN ISNULL(pl.list_monthly_fee, 0) > 0 THEN ISNULL(pl.list_dd, 0) / pl.list_monthly_fee
                WHEN ISNULL(pl.capacity, 0) > 0 THEN CAST(ISNULL(pl.ws_dd, 0) AS FLOAT) / pl.capacity
                ELSE 0.0
            END AS w_dd,
            CASE
                WHEN ISNULL(pl.list_monthly_fee, 0) > 0 THEN ISNULL(pl.list_hd, 0) / pl.list_monthly_fee
                WHEN ISNULL(pl.capacity, 0) > 0 THEN CAST(ISNULL(pl.ws_hd, 0) AS FLOAT) / pl.capacity
                ELSE 0.0
            END AS w_hd,
            CASE
                WHEN ISNULL(pl.list_monthly_fee, 0) > 0 THEN ISNULL(pl.list_add, 0) / pl.list_monthly_fee
                ELSE 0.0
            END AS w_add
    ) alloc
    WHERE c.is_deleted = 0
      AND c.start_date IS NOT NULL
      -- Same status filter as the contract book: active, in-notice, or future-signed.
      -- The future-signed test uses the +4h-SHIFTED start date with >=, matching
      -- every other date test in the landlord views. This was applied straight to
      -- prod and never committed; recovered here 2026-09-01 by diffing
      -- sys.sql_modules against the repo, so re-applying this file no longer
      -- silently reverts it (it moved 139 company-months when dry-run).
      AND (
          c.active = 1
          OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
          OR (c.active = 0 AND c.cancelled = 0 AND CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) >= CAST(GETUTCDATE() AS DATE))
      )
      -- Same product-link filter: resolved products, OR negative adjustment,
      -- OR future-signed positive contract without a desk link yet.
      AND (
          pl.contract_source_id IS NOT NULL
          OR COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
          OR (
              c.active = 0 AND c.cancelled = 0
              AND CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) >= CAST(GETUTCDATE() AS DATE)
              AND COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) > 0
          )
      )
),
-- The join now admits every contract that OVERLAPS the month, not just the ones
-- alive on its last day, because the day-weighted columns (2026-09-01) need
-- mid-month leavers present to contribute their days. The month-end columns are
-- unchanged: is_at_month_end zeroes exactly the rows the old predicate excluded,
-- so rev_po/ws_po/… still reconcile to gold.vw_landlord_contract_book_monthly.
active_by_month AS (
    SELECT
        ms.month_start,
        cf.location_source_id,
        cf.member_company_name,
        -- MONTH-END basis (unchanged)
        CASE WHEN me.is_at_month_end = 1 THEN cf.rev_po  ELSE 0 END AS rev_po,
        CASE WHEN me.is_at_month_end = 1 THEN cf.rev_dd  ELSE 0 END AS rev_dd,
        CASE WHEN me.is_at_month_end = 1 THEN cf.rev_hd  ELSE 0 END AS rev_hd,
        CASE WHEN me.is_at_month_end = 1 THEN cf.rev_add ELSE 0 END AS rev_add,
        CASE WHEN me.is_at_month_end = 1 THEN cf.ws_po   ELSE 0 END AS ws_po,
        CASE WHEN me.is_at_month_end = 1 THEN cf.ws_dd   ELSE 0 END AS ws_dd,
        CASE WHEN me.is_at_month_end = 1 THEN cf.ws_hd   ELSE 0 END AS ws_hd,
        CASE WHEN me.is_at_month_end = 1 THEN cf.ws_add  ELSE 0 END AS ws_add,
        -- DAY-WEIGHTED basis (2026-09-01): each type's allocated fee scaled by
        -- the share of the month the contract was live. Same window as the
        -- contract-book view's desk-days, so the two bases reconcile.
        cf.rev_po  * w.day_fraction AS rev_po_dw,
        cf.rev_dd  * w.day_fraction AS rev_dd_dw,
        cf.rev_hd  * w.day_fraction AS rev_hd_dw,
        cf.rev_add * w.day_fraction AS rev_add_dw
    FROM month_spine ms
    INNER JOIN contract_facts cf
        ON  cf.effective_start_date <= EOMONTH(ms.month_start)
        AND (
            cf.effective_cancel_excl IS NULL
            OR cf.effective_cancel_excl > ms.month_start
            -- Never lose a row the ORIGINAL month-end universe had.
            OR cf.cancellation_date >= EOMONTH(ms.month_start)
        )
    CROSS APPLY (
        SELECT
            CASE WHEN cf.effective_start_date > ms.month_start
                 THEN cf.effective_start_date ELSE ms.month_start END AS win_start,
            CASE WHEN cf.effective_cancel_excl IS NULL
                      OR DATEADD(DAY, -1, cf.effective_cancel_excl) > EOMONTH(ms.month_start)
                 THEN EOMONTH(ms.month_start)
                 ELSE DATEADD(DAY, -1, cf.effective_cancel_excl) END   AS win_end
    ) win
    CROSS APPLY (
        -- Clamped at zero: a contract that starts and cancels on the same day
        -- would otherwise produce a negative span.
        SELECT CAST(
            CASE WHEN win.win_end >= win.win_start
                 THEN DATEDIFF(DAY, win.win_start, win.win_end) + 1
                 ELSE 0 END AS DECIMAL(18,6)
        ) / DAY(EOMONTH(ms.month_start)) AS day_fraction
    ) w
    CROSS APPLY (
        SELECT CASE
            WHEN cf.cancellation_date IS NULL
              OR cf.cancellation_date >= EOMONTH(ms.month_start) THEN 1 ELSE 0
        END AS is_at_month_end
    ) me
)
SELECT
    FORMAT(abm.month_start, 'yyyy-MM')                          AS period,
    abm.month_start                                            AS month_start_date,
    abm.location_source_id,
    MAX(loc.name)                                              AS location_name,
    MAX(loc.currency_code)                                     AS currency_code,
    abm.member_company_name,

    CAST(SUM(abm.rev_po)  AS DECIMAL(18,2))                    AS rev_private_office,
    CAST(SUM(abm.rev_dd)  AS DECIMAL(18,2))                    AS rev_dedicated_desk,
    CAST(SUM(abm.rev_hd)  AS DECIMAL(18,2))                    AS rev_hot_desk,
    CAST(SUM(abm.rev_add) AS DECIMAL(18,2))                    AS rev_additional,
    CAST(SUM(abm.rev_po + abm.rev_dd + abm.rev_hd) AS DECIMAL(18,2)) AS mmrf,
    CAST(SUM(abm.rev_add) AS DECIMAL(18,2))                    AS marv,
    CAST(SUM(abm.rev_po + abm.rev_dd + abm.rev_hd + abm.rev_add) AS DECIMAL(18,2)) AS total_monthly_fee,

    -- Day-weighted twins (2026-09-01). Same allocation, scaled by the share of
    -- the month each contract was live, so the stacked forecast bars can be
    -- drawn on the basis the invoices actually bill.
    CAST(SUM(abm.rev_po_dw)  AS DECIMAL(18,2))                 AS rev_private_office_day_weighted,
    CAST(SUM(abm.rev_dd_dw)  AS DECIMAL(18,2))                 AS rev_dedicated_desk_day_weighted,
    CAST(SUM(abm.rev_hd_dw)  AS DECIMAL(18,2))                 AS rev_hot_desk_day_weighted,
    CAST(SUM(abm.rev_add_dw) AS DECIMAL(18,2))                 AS rev_additional_day_weighted,
    CAST(SUM(abm.rev_po_dw + abm.rev_dd_dw + abm.rev_hd_dw) AS DECIMAL(18,2)) AS mmrf_day_weighted,
    CAST(SUM(abm.rev_add_dw) AS DECIMAL(18,2))                 AS marv_day_weighted,
    CAST(SUM(abm.rev_po_dw + abm.rev_dd_dw + abm.rev_hd_dw + abm.rev_add_dw) AS DECIMAL(18,2))
                                                               AS total_monthly_fee_day_weighted,

    SUM(abm.ws_po)                                             AS ws_private_office,
    SUM(abm.ws_dd)                                             AS ws_dedicated_desk,
    SUM(abm.ws_hd)                                             AS ws_hot_desk,
    SUM(abm.ws_add)                                            AS ws_additional,

    CAST(GETUTCDATE() AS DATE)                                 AS last_refreshed_at
FROM active_by_month abm
LEFT JOIN silver.nexudus_locations loc
    ON  loc.source_id  = abm.location_source_id
    AND loc.is_deleted = 0
GROUP BY
    abm.month_start,
    abm.location_source_id,
    abm.member_company_name;
GO
