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
and next_tariff_name like '%Private Office%' and isnull(contract_term,dateadd(m,6,GETDATE()))>= dateadd(m,6,GETDATE())
and floor_plan_desk_ids is NOT null
and coworker_id=1421595984




SELECT coworker_id,count(1) FROM silver.nexudus_contracts where (start_date>=GETDATE() or (start_date<GETDATE() and isnull(contract_term,GETDATE())>=GETDATE())) and cancellation_date IS NULL
and next_tariff_name like '%Private Office%' and isnull(contract_term,dateadd(m,6,GETDATE()))>= dateadd(m,6,GETDATE())
and floor_plan_desk_ids is NOT null
GROUP BY coworker_id
ORDER BY count(1) DESC
--1419973994,1418180999, 1420307815, 1416791192, 1418464972, 1419993265 ,1420850140, 1420493211

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
WHERE coworker_company LIKE 'CAXTON%'

SELECT * FROM silver.nexudus_contracts
WHERE coworker_id IN (
    1421514305,
    1421595984,
    1421613509,
    1421350691,
    1421418413,
    1421217069,
    1421156108,
    1420967583,
    1421031635,
    1421038636,
    1421085235,
    1420877277,
    1420602557,
    1419993255,
    1418746700,
    1418596930,
    1418658663,
    1418664146,
    1417158018,
    1417159415,
    1415754617,
    1416207332,
    1416560940,
    1417546767,
    1417901599,
    1418049569,
    1418070472,
    1418082630,
    1418126535,
    1418180999,
    1418507711,
    1415686807,
    1418225045,
    1419993302,
    1419993309,
    1419993310,
    1419993257,
    1419993267,
    1419993268,
    1419993285,
    1419993274,
    1420031099,
    1419993325,
    1419993330,
    1419993331,
    1420235924,
    1420300555,
    1420524279,
    1420517850
)

SELECT * FROM silver.nexudus_contracts
WHERE coworker_id='1420517850';