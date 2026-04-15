-- costar_extraction_schema.sql
-- Tables for CoStar PDF extraction results in Real Estate DB.
-- Run against the database pointed to by AZURE_SQL_PDF_JOBS_CONNECTION_STRING.

-- Ensure bronze schema exists (should already exist for costar_pdf_extractor_logs)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

-- ============================================================
-- bronze.costar_building
-- One row per unique building extracted from a CoStar PDF.
-- ============================================================
IF OBJECT_ID('bronze.costar_building', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.costar_building (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        job_id          UNIQUEIDENTIFIER NOT NULL,
        building_name   NVARCHAR(500),
        building_address NVARCHAR(500),
        city            NVARCHAR(200),
        state           NVARCHAR(200),
        zip_code        NVARCHAR(20),
        county          NVARCHAR(200),
        submarket       NVARCHAR(200),
        property_type   NVARCHAR(100),
        star_rating     INT,
        latitude        FLOAT,
        longitude       FLOAT,
        geocoded_at     DATETIME2,
        source_page     INT,
        created_at      DATETIME2 DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_costar_building_job_id ON bronze.costar_building(job_id);
END;
GO

-- ============================================================
-- bronze.costar_building_contacts
-- One row per contact person extracted from a CoStar PDF.
-- ============================================================
IF OBJECT_ID('bronze.costar_building_contacts', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.costar_building_contacts (
        id                INT IDENTITY(1,1) PRIMARY KEY,
        job_id            UNIQUEIDENTIFIER NOT NULL,
        building_id       INT NOT NULL REFERENCES bronze.costar_building(id),
        contact_type      NVARCHAR(200),
        contact_name      NVARCHAR(300),
        contact_company   NVARCHAR(300),
        contact_job_title NVARCHAR(300),
        contact_email     NVARCHAR(300),
        contact_phone     NVARCHAR(100),
        company_address   NVARCHAR(500),
        company_city      NVARCHAR(200),
        company_state     NVARCHAR(200),
        company_zip       NVARCHAR(20),
        company_country   NVARCHAR(100),
        company_phone     NVARCHAR(100),
        company_website   NVARCHAR(300),
        source_page       INT,
        created_at        DATETIME2 DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_costar_contacts_job_id ON bronze.costar_building_contacts(job_id);
    CREATE INDEX IX_costar_contacts_building_id ON bronze.costar_building_contacts(building_id);
END;
GO
