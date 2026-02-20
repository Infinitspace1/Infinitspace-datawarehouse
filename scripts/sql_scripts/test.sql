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
