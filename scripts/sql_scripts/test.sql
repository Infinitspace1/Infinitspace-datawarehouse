CREATE SCHEMA bronze;   -- Raw ingestion: exact API/source data
GO
CREATE SCHEMA silver;   -- Cleaned, typed, normalized
GO
CREATE SCHEMA core;     -- Production-ready, source-agnostic
GO
CREATE SCHEMA meta;     -- Pipeline runs, sync logs, source registry
GO





--remove those 
SELECT * FROM silver.nexudus_product_offices;
SELECT * FROM silver.nexudus_product_desks;


SELECT TOP(10) * FROM silver.nexudus_products
WHERE id = 1415475208

SELECT * FROM silver.nexudus_product_rooms
WHERE source_id = 1420976575