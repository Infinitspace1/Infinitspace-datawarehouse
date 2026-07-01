/* =====================================================================
   30_ops_config.sql  —  jobs, logs, idempotency, config, ACL, analytics
   No hard FKs to core tables (logs may outlive the rows they reference).
   ===================================================================== */

/* ---------- export_jobs  (job_id = client uuid4; compare-and-set state machine) ---------- */
CREATE TABLE teamandy.export_jobs (
  job_id        NVARCHAR(64)  NOT NULL,   -- Firestore doc id (uuid4 / explicit)
  job_type      NVARCHAR(64)  NULL,        -- 'hubspot_export' | 'reply_io_export' | ...
  status        NVARCHAR(32)  NOT NULL CONSTRAINT DF_ej_status DEFAULT('pending'),
  error_message NVARCHAR(MAX) NULL,
  payload_json  NVARCHAR(MAX) NULL,
  progress_json NVARCHAR(MAX) NULL,
  result_json   NVARCHAR(MAX) NULL,
  created_at    DATETIME2(3)  NOT NULL CONSTRAINT DF_ej_created DEFAULT SYSUTCDATETIME(),
  updated_at    DATETIME2(3)  NOT NULL CONSTRAINT DF_ej_updated DEFAULT SYSUTCDATETIME(),
  completed_at  DATETIME2(3)  NULL,
  CONSTRAINT PK_export_jobs PRIMARY KEY CLUSTERED (job_id)
);
GO
-- status==pending ORDER BY created_at FETCH 1 ; status==running guard
CREATE NONCLUSTERED INDEX IX_ej_status_created   ON teamandy.export_jobs(status, created_at) INCLUDE (job_type, completed_at);
GO
-- status==completed/failed ORDER BY completed_at DESC FETCH 10
CREATE NONCLUSTERED INDEX IX_ej_status_completed ON teamandy.export_jobs(status, completed_at DESC) INCLUDE (job_type);
GO

/* ---------- processed_webhook_events  (PK = the idempotency mechanism) ---------- */
CREATE TABLE teamandy.processed_webhook_events (
  event_id     NVARCHAR(128) NOT NULL,   -- external webhook event id; dedup key
  event_type   NVARCHAR(64)  NULL,
  processed_at DATETIME2(3)  NOT NULL CONSTRAINT DF_pwe_processed DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_processed_webhook_events PRIMARY KEY CLUSTERED (event_id)
);
GO
CREATE NONCLUSTERED INDEX IX_pwe_processed_at ON teamandy.processed_webhook_events(processed_at) INCLUDE (event_type);
GO

/* ---------- warmup_handled_leads  (PK = contactId; idempotency log) ---------- */
CREATE TABLE teamandy.warmup_handled_leads (
  contact_id NVARCHAR(128) NOT NULL,   -- == contactId; one row per handled contact
  handled_by NVARCHAR(256) NULL,
  handled_at DATETIME2(3)  NOT NULL CONSTRAINT DF_whl_handled DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_warmup_handled_leads PRIMARY KEY CLUSTERED (contact_id)
);
GO
CREATE NONCLUSTERED INDEX IX_whl_handled_at ON teamandy.warmup_handled_leads(handled_at) INCLUDE (handled_by);
GO

/* ---------- warmup_reply_logs  (surrogate PK; original auto-id preserved for re-import) ---------- */
CREATE TABLE teamandy.warmup_reply_logs (
  log_id                   BIGINT IDENTITY(1,1) NOT NULL,
  firestore_doc_id         NVARCHAR(64)  NULL,        -- original auto-id (traceability)
  contact_id               NVARCHAR(128) NULL,
  contact_email            NVARCHAR(256) NULL,
  subject                  NVARCHAR(MAX) NULL,
  sent_by                  NVARCHAR(256) NULL,
  sent_via                 NVARCHAR(64)  NULL,
  sent_internet_message_id NVARCHAR(512) NULL,
  email_account_id         NVARCHAR(64)  NULL,        -- int|str in source -> string
  sent_at                  DATETIME2(3)  NOT NULL CONSTRAINT DF_wrl_sent DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_warmup_reply_logs PRIMARY KEY CLUSTERED (log_id)
);
GO
CREATE NONCLUSTERED INDEX IX_wrl_contact_id ON teamandy.warmup_reply_logs(contact_id, sent_at DESC);
GO
CREATE NONCLUSTERED INDEX IX_wrl_sent_at    ON teamandy.warmup_reply_logs(sent_at);
GO
CREATE UNIQUE NONCLUSTERED INDEX UQ_wrl_firestore_doc ON teamandy.warmup_reply_logs(firestore_doc_id) WHERE firestore_doc_id IS NOT NULL;
GO

/* ---------- settings (+ broker_firms child; array -> rows with UNIQUE) ---------- */
CREATE TABLE teamandy.settings (
  setting_id   NVARCHAR(128) NOT NULL,   -- Firestore doc id, e.g. 'broker_firms'
  payload_json NVARCHAR(MAX) NULL,
  updated_at   DATETIME2(3)  NOT NULL CONSTRAINT DF_settings_updated DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_settings PRIMARY KEY CLUSTERED (setting_id)
);
GO
CREATE TABLE teamandy.settings_broker_firms (
  broker_firm_id INT IDENTITY(1,1) NOT NULL,
  setting_id     NVARCHAR(128) NOT NULL CONSTRAINT DF_sbf_setting DEFAULT('broker_firms'),
  name           NVARCHAR(256) NOT NULL,
  created_at     DATETIME2(3)  NOT NULL CONSTRAINT DF_sbf_created DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_settings_broker_firms PRIMARY KEY CLUSTERED (broker_firm_id),
  CONSTRAINT FK_sbf_settings FOREIGN KEY (setting_id) REFERENCES teamandy.settings(setting_id),
  CONSTRAINT UQ_sbf_name UNIQUE (setting_id, name)   -- replaces array_contains dedup append
);
GO

/* ---------- acl_users  (users_with_access.users[] -> rows; kills RMW clobber) ---------- */
CREATE TABLE teamandy.acl_users (
  acl_user_id INT IDENTITY(1,1) NOT NULL,
  email       NVARCHAR(256) NOT NULL,   -- ACL key (lowercased on write); logical ref users.email (not enforced)
  has_access  BIT NOT NULL CONSTRAINT DF_acl_access DEFAULT(1),
  is_admin    BIT NOT NULL CONSTRAINT DF_acl_admin  DEFAULT(0),
  created_at  DATETIME2(3) NOT NULL CONSTRAINT DF_acl_created DEFAULT SYSUTCDATETIME(),
  updated_at  DATETIME2(3) NOT NULL CONSTRAINT DF_acl_updated DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_acl_users PRIMARY KEY CLUSTERED (acl_user_id),
  CONSTRAINT UQ_acl_users_email UNIQUE (email)   -- atomic grant/revoke
);
GO

/* ---------- graph_subscription_monitor  (singleton 'summary'; RECOMPUTE) ---------- */
CREATE TABLE teamandy.graph_subscription_monitor (
  doc_id                      NVARCHAR(64)  NOT NULL,   -- always 'summary'
  overall_status              NVARCHAR(32)  NULL,
  checked_at                  NVARCHAR(64)  NULL,        -- ISO-8601 string as written by monitor
  issue_signature             NVARCHAR(MAX) NULL,
  summary_json                NVARCHAR(MAX) NULL,
  accounts_json               NVARCHAR(MAX) NULL,
  issues_json                 NVARCHAR(MAX) NULL,
  orphaned_subscriptions_json NVARCHAR(MAX) NULL,
  reply_io_error              NVARCHAR(MAX) NULL,
  last_alert_signature        NVARCHAR(MAX) NULL,
  last_alert_status           NVARCHAR(32)  NULL,
  last_alert_sent_at          DATETIME2(3)  NULL,
  updated_at                  DATETIME2(3)  NOT NULL CONSTRAINT DF_gsm_updated DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_graph_subscription_monitor PRIMARY KEY CLUSTERED (doc_id)
);
GO
