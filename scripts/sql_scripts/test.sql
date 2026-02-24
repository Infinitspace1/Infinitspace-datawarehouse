CREATE SCHEMA bronze;   -- Raw ingestion: exact API/source data
GO
CREATE SCHEMA silver;   -- Cleaned, typed, normalized
GO
CREATE SCHEMA core;     -- Production-ready, source-agnostic
GO
CREATE SCHEMA meta;     -- Pipeline runs, sync logs, source registry
GO






SELECT TOP(10) * FROM silver.nexudus_products
WHERE product_type_label = 'Other'

SELECT * FROM silver.nexudus_products

-- delete all the 
SELECT COUNT(1), location_name FROM silver.nexudus_products
GROUP BY location_name
ORDER BY COUNT(1) DESC


SELECT * FROM silver.nexudus_contracts where (start_date>=GETDATE() or (start_date<GETDATE() and isnull(contract_term,GETDATE())>=GETDATE())) and cancellation_date IS NULL
and next_tariff_name like '%Private Office%' and isnull(contract_term,dateadd(m,6,GETDATE()))<= dateadd(m,6,GETDATE())
and floor_plan_desk_ids is NOT null






WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY coworker_id ORDER BY start_date ASC)  AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY coworker_id ORDER BY start_date DESC) AS rn_last
    FROM silver.nexudus_contracts
    WHERE next_tariff_name LIKE '%Private Office%'
),
contract_bounds AS (
    SELECT
        f.coworker_id,
        f.coworker_name,
        f.coworker_company,
        f.start_date          AS first_start_date,
        l.cancellation_date   AS last_cancellation_date
    FROM ranked f
    JOIN ranked l
        ON f.coworker_id = l.coworker_id
        AND f.rn_first = 1
        AND l.rn_last = 1
    WHERE l.cancellation_date IS NOT NULL
),
tenure AS (
    SELECT
        coworker_id,
        coworker_name,
        coworker_company,
        first_start_date,
        last_cancellation_date,
        DATEDIFF(MONTH, first_start_date, last_cancellation_date) AS tenure_months
    FROM contract_bounds
)
SELECT
    coworker_id,
    coworker_name,
    coworker_company,
    first_start_date,
    last_cancellation_date,
    tenure_months
FROM tenure
ORDER BY tenure_months DESC;




WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY coworker_id ORDER BY start_date ASC)  AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY coworker_id ORDER BY start_date DESC) AS rn_last
    FROM silver.nexudus_contracts
    WHERE next_tariff_name LIKE '%Private Office%'
),
contract_bounds AS (
    SELECT
        f.coworker_id,
        f.coworker_name,
        f.coworker_company,
        f.start_date          AS first_start_date,
        l.cancellation_date   AS last_cancellation_date
    FROM ranked f
    JOIN ranked l
        ON f.coworker_id = l.coworker_id
        AND f.rn_first = 1
        AND l.rn_last = 1
    WHERE l.cancellation_date IS NOT NULL
),
tenure AS (
    SELECT
        coworker_id,
        coworker_name,
        coworker_company,
        first_start_date,
        last_cancellation_date,
        DATEDIFF(MONTH, first_start_date, last_cancellation_date) AS tenure_months
    FROM contract_bounds
)
SELECT
    coworker_id,
    coworker_name,
    coworker_company,
    first_start_date,
    last_cancellation_date,
    tenure_months,
    AVG(tenure_months) OVER () AS avg_tenure_months
FROM tenure
WHERE tenure_months > 0
ORDER BY tenure_months DESC;



SELECT * FROM silver.nexudus_contracts
WHERE coworker_id = 1417796101

SELECT * FROM silver.nexudus_products
WHERE source_id='1415402884';