/* =====================================================================
   40_runtime_phantom.sql  —  collections that are EMPTY at scan time but
   written at runtime (churn). No data migrates; the tables must exist so
   the live code paths work after cutover. RE-PROBE both at the freeze moment.

   NOTE: exact columns are inferred from code (collections were empty, so no
   live field sample). Confirm against the writers before go-live; the *_json
   catch-all preserves any field not promoted to a column.
   ===================================================================== */

/* ---------- graph_subscriptions  (MS-Graph warmup subscription lifecycle) ----------
   Writers/readers: TeamAndy-backend warmup graph_sync — full CRUD, .stream() to build
   an email->account_id map, where('email','=='), where('user_email','==').
   PK = the Graph subscription id (doc-id). */
CREATE TABLE teamandy.graph_subscriptions (
  subscription_id      NVARCHAR(128) NOT NULL,   -- MS Graph subscription id == Firestore doc-id
  email                NVARCHAR(320) NULL,        -- mailbox address (where('email','==') )
  user_email           NVARCHAR(320) NULL,        -- owning user (where('user_email','==') )
  account_id           NVARCHAR(128) NULL,        -- email account id resolved from the stream() map
  resource             NVARCHAR(512) NULL,        -- Graph resource path
  change_type          NVARCHAR(128) NULL,
  notification_url     NVARCHAR(1024) NULL,
  client_state         NVARCHAR(256) NULL,
  expiration_date_time DATETIME2(3)  NULL,
  status               NVARCHAR(32)  NULL,
  extra_json           NVARCHAR(MAX) NULL,
  created_at           DATETIME2(3)  NOT NULL CONSTRAINT DF_gs_created DEFAULT SYSUTCDATETIME(),
  updated_at           DATETIME2(3)  NOT NULL CONSTRAINT DF_gs_updated DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_graph_subscriptions PRIMARY KEY CLUSTERED (subscription_id)
);
GO
CREATE NONCLUSTERED INDEX IX_gs_email      ON teamandy.graph_subscriptions(email)      WHERE email IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_gs_user_email ON teamandy.graph_subscriptions(user_email) WHERE user_email IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_gs_expiration ON teamandy.graph_subscriptions(expiration_date_time);  -- renewal sweep
GO
