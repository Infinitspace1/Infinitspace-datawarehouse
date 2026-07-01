/* =====================================================================
   50_company_index.sql  —  cross-list dedupe index + claim lock
   Kept in Azure SQL (not Table Storage) specifically because the PK on
   `domain` is the atomic claim lock that replaces Firestore .create()-
   AlreadyExists in individual-scraping's _claim_domain. INSERT on a taken
   domain raises a duplicate-key (2627) = "already claimed".
   ~20,081 rows. Soft ref lead_uid -> leads.uid (nullable while pending).
   ===================================================================== */

CREATE TABLE teamandy.company_index (
  domain       NVARCHAR(255)  NOT NULL,
  lead_uid     NVARCHAR(128)  NULL,        -- soft ref leads.uid (null while status='pending')
  website      NVARCHAR(2048) NULL,
  status       NVARCHAR(32)   NOT NULL CONSTRAINT DF_ci_status DEFAULT('pending'),
  company_name NVARCHAR(512)  NULL,
  created_at   DATETIME2(3)   NOT NULL CONSTRAINT DF_ci_created DEFAULT SYSUTCDATETIME(),
  updated_at   DATETIME2(3)   NOT NULL CONSTRAINT DF_ci_updated DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_company_index PRIMARY KEY CLUSTERED (domain)   -- the claim lock
);
GO
CREATE NONCLUSTERED INDEX IX_ci_lead_uid ON teamandy.company_index(lead_uid) WHERE lead_uid IS NOT NULL;  -- reverse lead->index
GO
CREATE NONCLUSTERED INDEX IX_ci_status   ON teamandy.company_index(status) INCLUDE (lead_uid);            -- release filters status='pending'
GO

/* leadListIds[] -> rows (ArrayUnion becomes INSERT-IF-NOT-EXISTS) */
CREATE TABLE teamandy.company_index_lead_list (
  domain       NVARCHAR(255) NOT NULL,
  lead_list_id NVARCHAR(128) NOT NULL,
  CONSTRAINT PK_company_index_lead_list PRIMARY KEY CLUSTERED (domain, lead_list_id),
  CONSTRAINT FK_cill_ci FOREIGN KEY (domain) REFERENCES teamandy.company_index(domain) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_cill_lead_list ON teamandy.company_index_lead_list(lead_list_id);
GO

/* sourcedLocations[] (array of {leadListId,city,country,place_id,maps_url}) -> rows */
CREATE TABLE teamandy.company_index_sourced_location (
  sourced_location_id BIGINT IDENTITY(1,1) NOT NULL,
  domain              NVARCHAR(255) NOT NULL,
  lead_list_id        NVARCHAR(128) NULL,
  city                NVARCHAR(256) NULL,
  country             NVARCHAR(256) NULL,
  place_id            NVARCHAR(256) NULL,
  maps_url            NVARCHAR(2048) NULL,
  CONSTRAINT PK_company_index_sourced_location PRIMARY KEY CLUSTERED (sourced_location_id),
  CONSTRAINT FK_cisl_ci FOREIGN KEY (domain) REFERENCES teamandy.company_index(domain) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_cisl_domain ON teamandy.company_index_sourced_location(domain);
GO
