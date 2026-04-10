-- scripts/sql_scripts/bamboohr_schema.sql
-- BambooHR bronze + silver tables

-- ── Bronze ────────────────────────────────────────────────────────────────

CREATE TABLE bronze.bamboohr_employees (
    id           INT IDENTITY(1,1) NOT NULL,
    source_id    INT NOT NULL,
    raw_json     NVARCHAR(MAX) NOT NULL,
    synced_at    DATETIME2 NOT NULL CONSTRAINT df_bronze_bamboohr_employees_synced_at DEFAULT GETUTCDATE(),
    sync_run_id  UNIQUEIDENTIFIER NOT NULL,
    CONSTRAINT pk_bronze_bamboohr_employees PRIMARY KEY (id),
    CONSTRAINT uq_bronze_bamboohr_employees_source_id UNIQUE (source_id)
);

-- ── Silver ────────────────────────────────────────────────────────────────

CREATE TABLE silver.bamboohr_employees (
    source_id              INT NOT NULL,
    employee_number        NVARCHAR(50),
    first_name             NVARCHAR(100),
    last_name              NVARCHAR(100),
    work_email             NVARCHAR(255),
    job_title              NVARCHAR(200),
    department             NVARCHAR(200),
    division               NVARCHAR(200),
    location               NVARCHAR(200),
    manager_id             INT,
    manager_name           NVARCHAR(200),
    hire_date              DATE,
    work_phone             NVARCHAR(50),
    work_phone_ext         NVARCHAR(20),
    address1               NVARCHAR(255),
    city                   NVARCHAR(100),
    bronze_id              INT,
    sync_run_id            UNIQUEIDENTIFIER,
    last_synced_at         DATETIME2 NOT NULL CONSTRAINT df_silver_bamboohr_employees_synced_at DEFAULT GETUTCDATE(),
    CONSTRAINT pk_silver_bamboohr_employees PRIMARY KEY (source_id)
);

-- ── Migration: drop removed columns from existing table ───────────────────
-- Run this once against the live database if the table already exists:
--
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN display_name;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN personal_email;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN employment_status;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN termination_date;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN mobile_phone;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN cost_center;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN pay_group;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN flsa_code;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN gender;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN nationality;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN marital_status;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN date_of_birth;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN state;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN country;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN zip_code;
-- ALTER TABLE silver.bamboohr_employees DROP COLUMN photo_url;
