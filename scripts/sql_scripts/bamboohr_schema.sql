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
    display_name           NVARCHAR(200),
    work_email             NVARCHAR(255),
    personal_email         NVARCHAR(255),
    job_title              NVARCHAR(200),
    department             NVARCHAR(200),
    division               NVARCHAR(200),
    location               NVARCHAR(200),
    manager_id             INT,
    manager_name           NVARCHAR(200),
    employment_status      NVARCHAR(100),
    hire_date              DATE,
    termination_date       DATE,
    work_phone             NVARCHAR(50),
    work_phone_ext         NVARCHAR(20),
    mobile_phone           NVARCHAR(50),
    cost_center            NVARCHAR(200),
    pay_group              NVARCHAR(100),
    flsa_code              NVARCHAR(50),
    gender                 NVARCHAR(20),
    nationality            NVARCHAR(100),
    marital_status         NVARCHAR(50),
    date_of_birth          DATE,
    address1               NVARCHAR(255),
    city                   NVARCHAR(100),
    state                  NVARCHAR(100),
    country                NVARCHAR(100),
    zip_code               NVARCHAR(20),
    photo_url              NVARCHAR(500),
    bronze_id              INT,
    sync_run_id            UNIQUEIDENTIFIER,
    last_synced_at         DATETIME2 NOT NULL CONSTRAINT df_silver_bamboohr_employees_synced_at DEFAULT GETUTCDATE(),
    CONSTRAINT pk_silver_bamboohr_employees PRIMARY KEY (source_id)
);
