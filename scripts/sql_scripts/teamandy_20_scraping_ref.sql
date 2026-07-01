/* =====================================================================
   20_scraping_ref.sql  —  reference data + scraping operational tables
   Depends on: lead_lists (soft refs only; no hard FK to lead_lists here)
   ===================================================================== */

/* ---------- organization_ids_apollo  (1 Firestore doc whose ~170 fields ARE the data) ----------
   Transform: for each (industry_name, apollo_id) field in the single doc -> one row. */
CREATE TABLE teamandy.organization_ids_apollo (
  industry_name NVARCHAR(256) NOT NULL,
  apollo_id     NVARCHAR(128) NOT NULL,
  source_doc_id NVARCHAR(256) NULL,   -- provenance: original single Firestore doc id
  created_at    DATETIME2(3)  NOT NULL CONSTRAINT DF_oia_created DEFAULT SYSUTCDATETIME(),
  updated_at    DATETIME2(3)  NOT NULL CONSTRAINT DF_oia_updated DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_organization_ids_apollo PRIMARY KEY CLUSTERED (industry_name)
);
GO
-- reverse lookup apollo_id -> industry. Drop UNIQUE if multiple industries can share an apollo id.
CREATE NONCLUSTERED INDEX IX_oia_apollo_id ON teamandy.organization_ids_apollo(apollo_id) WHERE apollo_id IS NOT NULL;  -- non-unique: industries can share an apollo id
GO

/* ---------- job_title_mappings (+ synonyms child) ---------- */
CREATE TABLE teamandy.job_title_mappings (
  mapping_id             NVARCHAR(256) NOT NULL,   -- Firestore doc id
  original_job_title     NVARCHAR(256) NULL,
  synonyms_justification NVARCHAR(MAX) NULL,
  synonyms_json          NVARCHAR(MAX) NULL,
  created_at             DATETIME2(3)  NOT NULL CONSTRAINT DF_jtm_created DEFAULT SYSUTCDATETIME(),
  updated_at             DATETIME2(3)  NOT NULL CONSTRAINT DF_jtm_updated DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_job_title_mappings PRIMARY KEY CLUSTERED (mapping_id)
);
GO
CREATE NONCLUSTERED INDEX IX_jtm_original ON teamandy.job_title_mappings(original_job_title);
GO
CREATE TABLE teamandy.job_title_synonyms (
  synonym_id BIGINT IDENTITY(1,1) NOT NULL,
  mapping_id NVARCHAR(256) NOT NULL,
  synonym    NVARCHAR(256) NOT NULL,
  CONSTRAINT PK_job_title_synonyms PRIMARY KEY CLUSTERED (synonym_id),
  CONSTRAINT FK_jts_mapping FOREIGN KEY (mapping_id) REFERENCES teamandy.job_title_mappings(mapping_id) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_jts_mapping ON teamandy.job_title_synonyms(mapping_id) INCLUDE (synonym);
GO
CREATE NONCLUSTERED INDEX IX_jts_synonym ON teamandy.job_title_synonyms(synonym);  -- reverse: which mapping has this synonym
GO

/* ---------- competence  (legacy; location.city/country promoted to columns) ---------- */
CREATE TABLE teamandy.competence (
  uid                  NVARCHAR(128) NOT NULL,   -- Firestore doc-id
  location_city        NVARCHAR(256) NULL,        -- promoted from location.city (dotted-path equality query)
  location_country     NVARCHAR(128) NULL,        -- promoted from location.country
  location_json        NVARCHAR(MAX) NULL,
  list_competence_json NVARCHAR(MAX) NULL,
  created_at           DATETIME2(3)  NOT NULL CONSTRAINT DF_comp_created DEFAULT SYSUTCDATETIME(),
  updated_at           DATETIME2(3)  NULL,
  CONSTRAINT PK_competence PRIMARY KEY CLUSTERED (uid)
);
GO
CREATE NONCLUSTERED INDEX IX_competence_city_country ON teamandy.competence(location_city, location_country);
GO

/* ---------- competence_new (+ competitors child; 15,012 competitor rows total) ---------- */
CREATE TABLE teamandy.competence_new (
  uid                    NVARCHAR(128) NOT NULL,   -- doc-id: auto-id OR '{ISO2}_AUTO'
  competitor_list_name   NVARCHAR(512) NULL,
  country                NVARCHAR(128) NULL,
  country_code           NVARCHAR(8)   NULL,
  status                 NVARCHAR(40)  NULL,
  auto_managed           BIT NOT NULL CONSTRAINT DF_cn_auto   DEFAULT(0),
  schema_version         INT NOT NULL CONSTRAINT DF_cn_schema DEFAULT(2),
  competitor_count       INT NOT NULL CONSTRAINT DF_cn_count  DEFAULT(0),  -- cached count
  owner_user_uid         NVARCHAR(128) NULL,        -- 'uid' owner field if used as creator ref
  apify_input_json       NVARCHAR(MAX) NULL,
  polygon_points_json    NVARCHAR(MAX) NULL,
  competitors_legacy_json NVARCHAR(MAX) NULL,
  last_run_stats_json    NVARCHAR(MAX) NULL,
  last_error             NVARCHAR(MAX) NULL,
  created_at             DATETIME2(3)  NOT NULL CONSTRAINT DF_cn_created DEFAULT SYSUTCDATETIME(),
  updated_at             DATETIME2(3)  NULL,
  last_run_at            DATETIME2(3)  NULL,
  migrated_at            DATETIME2(3)  NULL,
  CONSTRAINT PK_competence_new PRIMARY KEY CLUSTERED (uid)
);
GO
CREATE NONCLUSTERED INDEX IX_cn_country      ON teamandy.competence_new(country) INCLUDE (status, competitor_count);
GO
CREATE NONCLUSTERED INDEX IX_cn_country_code ON teamandy.competence_new(country_code) WHERE country_code IS NOT NULL;
GO
CREATE TABLE teamandy.competence_new_competitors (
  competitor_id   NVARCHAR(200)  NOT NULL,   -- subcollection doc-id: sanitized placeId OR 'k_<sha1>' (200 keeps PK <900-byte limit)
  list_uid        NVARCHAR(128)  NOT NULL,   -- parent competence_new.uid
  title           NVARCHAR(512)  NULL,
  website         NVARCHAR(512)  NULL,
  address         NVARCHAR(1024) NULL,
  city            NVARCHAR(256)  NULL,
  street          NVARCHAR(512)  NULL,
  postal_code     NVARCHAR(40)   NULL,
  phone           NVARCHAR(64)   NULL,
  latitude        FLOAT          NULL,
  longitude       FLOAT          NULL,
  place_id        NVARCHAR(512)  NULL,
  google_maps_url NVARCHAR(1024) NULL,
  category_name   NVARCHAR(256)  NULL,
  created_at      DATETIME2(3)   NULL,
  updated_at      DATETIME2(3)   NULL,
  CONSTRAINT PK_competence_new_competitors PRIMARY KEY CLUSTERED (list_uid, competitor_id),
  CONSTRAINT FK_cnc_list FOREIGN KEY (list_uid) REFERENCES teamandy.competence_new(uid) ON DELETE CASCADE
);
GO
-- CRITICAL: replaces Firestore collection_group('competitors').where('placeId','==',...) cross-parent lookup
CREATE NONCLUSTERED INDEX IX_cnc_place_id ON teamandy.competence_new_competitors(place_id) INCLUDE (list_uid, title, website);
GO
CREATE NONCLUSTERED INDEX IX_cnc_list     ON teamandy.competence_new_competitors(list_uid) INCLUDE (place_id);
GO

/* ---------- scraping_jobs  (createdAt:str -> DATETIME2; lead_list_id soft ref) ---------- */
CREATE TABLE teamandy.scraping_jobs (
  uid                          NVARCHAR(128) NOT NULL,   -- Firestore auto-id (mirrored in doc as 'uid')
  lead_list_id                 NVARCHAR(128) NULL,        -- soft ref lead_lists.uid
  job_type                     NVARCHAR(64)  NULL,        -- model field 'type'
  status                       NVARCHAR(128) NULL,        -- Scheduled/Running/Completed/Failed (+ longer refresh statuses)
  start_time                   DATETIME2(3)  NULL,
  end_time                     DATETIME2(3)  NULL,
  leads_generated              INT NOT NULL CONSTRAINT DF_sj_leads DEFAULT(0),
  error_log                    NVARCHAR(MAX) NULL,
  config_snapshot_json         NVARCHAR(MAX) NULL,
  is_refresh                   BIT NOT NULL CONSTRAINT DF_sj_refresh DEFAULT(0),
  refresh_cleanup_status       NVARCHAR(40)  NULL,
  refresh_cleanup_snapshot_json NVARCHAR(MAX) NULL,
  refresh_cleanup_at           DATETIME2(3)  NULL,
  created_at                   DATETIME2(3)  NOT NULL CONSTRAINT DF_sj_created DEFAULT SYSUTCDATETIME(),
  updated_at                   DATETIME2(3)  NOT NULL CONSTRAINT DF_sj_updated DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_scraping_jobs PRIMARY KEY CLUSTERED (uid)
);
GO
-- 'latest job per list' (replaces orchestrator Python-side sort) + frontend (leadListId + status + order_by createdAt)
CREATE NONCLUSTERED INDEX IX_sj_list_created        ON teamandy.scraping_jobs(lead_list_id, created_at DESC) INCLUDE (status, is_refresh, leads_generated);
GO
CREATE NONCLUSTERED INDEX IX_sj_list_status_created ON teamandy.scraping_jobs(lead_list_id, status, created_at DESC);
GO
CREATE NONCLUSTERED INDEX IX_sj_status              ON teamandy.scraping_jobs(status) WHERE status <> 'Completed';
GO

/* ---------- jobs_queue  (single-running-job invariant enforced by filtered unique index) ---------- */
CREATE TABLE teamandy.jobs_queue (
  lead_id       NVARCHAR(128) NOT NULL,   -- Firestore doc-id == lead_id == lead_lists.uid (soft ref)
  status        NVARCHAR(20)  NOT NULL CONSTRAINT DF_jq_status DEFAULT('pending'),
  error_message NVARCHAR(MAX) NULL,
  created_at    DATETIME2(3)  NOT NULL CONSTRAINT DF_jq_created DEFAULT SYSUTCDATETIME(),
  updated_at    DATETIME2(3)  NOT NULL CONSTRAINT DF_jq_updated DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_jobs_queue PRIMARY KEY CLUSTERED (lead_id),
  CONSTRAINT CK_jq_status CHECK (status IN ('pending','running','completed','failed'))
);
GO
CREATE NONCLUSTERED INDEX IX_jq_status_created ON teamandy.jobs_queue(status, created_at) INCLUDE (lead_id);  -- oldest pending
GO
-- ENFORCES the single-running-job invariant at the DB level (replaces racy where(status==running) reads)
CREATE UNIQUE NONCLUSTERED INDEX UQ_jq_single_running ON teamandy.jobs_queue(status) WHERE status = 'running';
GO

/* ---------- deduplication_tracking  (doc-id '{place_id}_{lead_list_id}') ---------- */
CREATE TABLE teamandy.deduplication_tracking (
  dedup_key    NVARCHAR(256) NOT NULL,   -- doc-id == '{place_id}_{lead_list_id}'
  place_id     NVARCHAR(512) NULL,
  lead_list_id NVARCHAR(128) NULL,        -- soft ref lead_lists.uid
  deduped_to   NVARCHAR(128) NULL,        -- canonical lead uid (soft ref leads.uid)
  domain       NVARCHAR(512) NULL,
  deduped_at   DATETIME2(3)  NOT NULL CONSTRAINT DF_dedup_at DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_deduplication_tracking PRIMARY KEY CLUSTERED (dedup_key)
);
GO
CREATE NONCLUSTERED INDEX IX_dedup_lead_list ON teamandy.deduplication_tracking(lead_list_id) INCLUDE (place_id, deduped_to, domain);
GO
CREATE NONCLUSTERED INDEX IX_dedup_place_id  ON teamandy.deduplication_tracking(place_id)   WHERE place_id IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_dedup_deduped_to ON teamandy.deduplication_tracking(deduped_to) WHERE deduped_to IS NOT NULL;
GO
