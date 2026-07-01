/* =====================================================================
   10_core_crm.sql  —  12 core CRM entities + child/junction tables
   Load order respects FK dependencies. Soft refs are indexed, not enforced.
   ===================================================================== */

/* ---------- 1. users  (root of all soft refs; uid = Firebase Auth uid) ---------- */
CREATE TABLE teamandy.users (
  uid                     NVARCHAR(128)  NOT NULL,
  uuid                    NVARCHAR(128)  NULL,
  email                   NVARCHAR(320)  NULL,    -- Firestore never enforced presence/uniqueness
  display_name            NVARCHAR(256)  NULL,
  photo_url               NVARCHAR(2048) NULL,
  role                    NVARCHAR(64)   NULL,
  is_admin                BIT            NOT NULL CONSTRAINT DF_users_is_admin DEFAULT(0),
  nexudus_connect         BIT            NOT NULL CONSTRAINT DF_users_nexudus_connect DEFAULT(0),
  hubspot_access_token    BIT            NULL,                 -- ground truth: bool flag, not the token
  session_token           NVARCHAR(MAX)  NULL,                 -- SECRET -> prefer Key Vault; null post-cutover
  session_expiry          DATETIME2(3)   NULL,
  last_login              DATETIME2(3)   NULL,
  last_activity           DATETIME2(3)   NULL,
  created_at              DATETIME2(3)   NULL,
  settings_json           NVARCHAR(MAX)  NULL,
  nexudus_account_json    NVARCHAR(MAX)  NULL, -- SECRET: tokens/password
  created_lead_lists_json NVARCHAR(MAX)  NULL,
  CONSTRAINT PK_users PRIMARY KEY CLUSTERED (uid)
);
GO
CREATE NONCLUSTERED INDEX IX_users_email ON teamandy.users(email) WHERE email IS NOT NULL;  -- non-unique: prod has empty/dup emails
GO
CREATE NONCLUSTERED INDEX IX_users_role ON teamandy.users(role) WHERE role IS NOT NULL;
GO

/* ---------- 2. lead_list_folders ---------- */
CREATE TABLE teamandy.lead_list_folders (
  uid         NVARCHAR(128) NOT NULL,
  name        NVARCHAR(256) NULL,
  created_by  NVARCHAR(128) NULL,   -- soft ref users.uid
  created_at  DATETIME2(3)  NULL,
  CONSTRAINT PK_lead_list_folders PRIMARY KEY CLUSTERED (uid)
);
GO
CREATE NONCLUSTERED INDEX IX_llf_name ON teamandy.lead_list_folders(name);
GO

/* ---------- 3. locations  (uid = numeric Nexudus id as string) ---------- */
CREATE TABLE teamandy.locations (
  uid                         NVARCHAR(128) NOT NULL,
  name                        NVARCHAR(512) NULL,
  address                     NVARCHAR(1024) NULL,
  city                        NVARCHAR(256) NULL,
  country                     NVARCHAR(128) NULL,
  postal_code                 NVARCHAR(32)  NULL,
  latitude                    FLOAT NULL,
  longitude                   FLOAT NULL,
  description                 NVARCHAR(MAX) NULL,
  ups                         NVARCHAR(256) NULL,
  nexudus_id                  NVARCHAR(128) NULL,
  hubspot_id                  NVARCHAR(128) NULL,
  total_size                  FLOAT NULL,
  total_size_measurement_unit NVARCHAR(32) NULL,
  total_workspaces            INT   NULL,
  occupied_workspaces         INT   NULL,
  avg_occupancy_rate          FLOAT NULL,
  image_blob_url              NVARCHAR(2048) NULL,   -- Azure Blob (container location-images)
  amenities_json              NVARCHAR(MAX) NULL,
  integrations_json           NVARCHAR(MAX) NULL,
  created_at                  DATETIME2(3) NULL,     -- ETL: ISO str -> DATETIME2
  updated_at                  DATETIME2(3) NULL,
  CONSTRAINT PK_locations PRIMARY KEY CLUSTERED (uid),
  CONSTRAINT CK_locations_uid_numeric CHECK (uid NOT LIKE '%[^0-9]%')
);
GO
CREATE NONCLUSTERED INDEX IX_locations_city    ON teamandy.locations(city);
GO
CREATE NONCLUSTERED INDEX IX_locations_country ON teamandy.locations(country);
GO
CREATE TABLE teamandy.location_workspaces (
  workspace_id            NVARCHAR(64)  NOT NULL,   -- workspaceId (uuid)
  location_uid            NVARCHAR(128) NOT NULL,
  workspace_nexudus_id    NVARCHAR(128) NULL,
  public_name             NVARCHAR(512) NULL,
  internal_code           NVARCHAR(128) NULL,
  type                    NVARCHAR(64)  NULL,
  floor                   NVARCHAR(64)  NULL,
  size_sq_m               FLOAT NULL,
  capacity                INT NULL,
  availability_status     NVARCHAR(32)  NULL,   -- Available|Occupied|Reserved
  status                  NVARCHAR(64)  NULL,
  exclude_from_scraping   BIT NOT NULL CONSTRAINT DF_lw_efs  DEFAULT(0),
  booking                 BIT NOT NULL CONSTRAINT DF_lw_book DEFAULT(0),
  start_value             NVARCHAR(64)  NULL,   -- Firestore 'start'
  end_value               NVARCHAR(64)  NULL,   -- Firestore 'end'
  coworker_company_name   NVARCHAR(512) NULL,
  coworker_name           NVARCHAR(256) NULL,
  coworker_price          FLOAT NULL,
  coworker_id             NVARCHAR(128) NULL,
  coworker_tariff_name    NVARCHAR(256) NULL,
  price_details_json      NVARCHAR(MAX) NULL,
  coworker_contracts_json NVARCHAR(MAX) NULL,
  properties_json         NVARCHAR(MAX) NULL,
  CONSTRAINT PK_location_workspaces PRIMARY KEY CLUSTERED (location_uid, workspace_id),
  CONSTRAINT FK_lw_loc FOREIGN KEY (location_uid) REFERENCES teamandy.locations(uid) ON DELETE CASCADE
);
GO
CREATE TABLE teamandy.location_active_lead_lists (
  location_uid  NVARCHAR(128) NOT NULL,
  lead_list_uid NVARCHAR(128) NOT NULL,           -- soft ref lead_lists.uid (not FK-enforced)
  CONSTRAINT PK_location_active_lead_lists PRIMARY KEY CLUSTERED (location_uid, lead_list_uid),
  CONSTRAINT FK_lall_loc FOREIGN KEY (location_uid) REFERENCES teamandy.locations(uid) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_lall_leadlist ON teamandy.location_active_lead_lists(lead_list_uid); -- array_contains(activeLeadLists)
GO

/* ---------- 4. lead_lists  (createdAt:int epoch -> DATETIME2) ---------- */
CREATE TABLE teamandy.lead_lists (
  uid                        NVARCHAR(128) NOT NULL,
  name                       NVARCHAR(512) NULL,
  status                     NVARCHAR(32)  NULL,  -- Active|Paused|Archived
  source                     NVARCHAR(128) NULL,
  scraping_type              NVARCHAR(32)  NULL,  -- Dynamic|Static
  scraping_frequency         INT           NULL,
  folder_id                  NVARCHAR(128) NULL,
  created_by                 NVARCHAR(128) NULL,  -- soft ref users.uid
  hubspot_id                 NVARCHAR(128) NULL,
  estimated_monthly_cost     FLOAT         NULL,
  total_actual_costs         FLOAT         NULL,
  created_at                 DATETIME2(3)  NULL,  -- ETL: epoch-int ms -> DATEADD
  updated_at                 DATETIME2(3)  NULL,  -- ETL: int|ts -> DATETIME2
  last_scraped_at            DATETIME2(3)  NULL,
  scraping_settings_json     NVARCHAR(MAX) NULL,
  lead_warming_settings_json NVARCHAR(MAX) NULL,
  statistics_json            NVARCHAR(MAX) NULL,
  CONSTRAINT PK_lead_lists PRIMARY KEY CLUSTERED (uid)
  -- folder_id is a SOFT ref (folders may be deleted); indexed, not FK-enforced
);
GO
CREATE NONCLUSTERED INDEX IX_ll_status_created ON teamandy.lead_lists(status, created_at DESC);
GO
CREATE NONCLUSTERED INDEX IX_ll_folder_id      ON teamandy.lead_lists(folder_id) WHERE folder_id IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_ll_name           ON teamandy.lead_lists(name);
GO
CREATE NONCLUSTERED INDEX IX_ll_updated_at     ON teamandy.lead_lists(updated_at DESC);
GO
/* counters that replace firestore.Increment(statistics.*) — atomic UPDATE under RCSI */
CREATE TABLE teamandy.lead_list_statistics (
  lead_list_uid             NVARCHAR(128) NOT NULL,
  warmup_exported           INT NOT NULL CONSTRAINT DF_lls_we  DEFAULT(0),
  synthetic_warmup_exported INT NOT NULL CONSTRAINT DF_lls_swe DEFAULT(0),
  hubspot_exported          INT NOT NULL CONSTRAINT DF_lls_he  DEFAULT(0),
  total_leads               INT NULL,
  last_updated              DATETIME2(3) NULL,
  CONSTRAINT PK_lead_list_statistics PRIMARY KEY CLUSTERED (lead_list_uid),
  CONSTRAINT FK_lls_ll FOREIGN KEY (lead_list_uid) REFERENCES teamandy.lead_lists(uid) ON DELETE CASCADE
);
GO
CREATE TABLE teamandy.lead_list_locations (
  lead_list_uid NVARCHAR(128) NOT NULL,
  location_uid  NVARCHAR(128) NOT NULL,   -- locationIds array element (soft ref locations.uid)
  CONSTRAINT PK_lead_list_locations PRIMARY KEY CLUSTERED (lead_list_uid, location_uid),
  CONSTRAINT FK_lll2_ll FOREIGN KEY (lead_list_uid) REFERENCES teamandy.lead_lists(uid) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_lll2_location ON teamandy.lead_list_locations(location_uid);
GO

/* ---------- 5. campaigns  (dates str|null -> DATETIME2) ---------- */
CREATE TABLE teamandy.campaigns (
  uid                  NVARCHAR(128) NOT NULL,
  name                 NVARCHAR(512) NULL,
  description          NVARCHAR(MAX) NULL,
  type                 NVARCHAR(64)  NULL,
  status               NVARCHAR(64)  NULL,
  lead_list_id         NVARCHAR(128) NULL,
  created_by           NVARCHAR(128) NULL,  -- soft ref users.uid
  start_date           DATETIME2(3)  NULL,
  end_date             DATETIME2(3)  NULL,
  created_at           DATETIME2(3)  NULL,
  updated_at           DATETIME2(3)  NULL,
  target_audience_json NVARCHAR(MAX) NULL,
  CONSTRAINT PK_campaigns PRIMARY KEY CLUSTERED (uid)
  -- lead_list_id is a SOFT ref (lists may be deleted); indexed, not FK-enforced
);
GO
CREATE NONCLUSTERED INDEX IX_campaigns_lead_list_id ON teamandy.campaigns(lead_list_id) WHERE lead_list_id IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_campaigns_status       ON teamandy.campaigns(status);
GO
CREATE TABLE teamandy.campaign_message_templates (
  template_id          NVARCHAR(128) NOT NULL,
  campaign_uid         NVARCHAR(128) NOT NULL,
  channel              NVARCHAR(32)  NULL,
  subject              NVARCHAR(1024) NULL,
  body                 NVARCHAR(MAX) NULL,
  template_order       INT NOT NULL CONSTRAINT DF_cmt_order DEFAULT(0),
  delay_after_previous INT NOT NULL CONSTRAINT DF_cmt_delay DEFAULT(0),
  CONSTRAINT PK_campaign_message_templates PRIMARY KEY CLUSTERED (campaign_uid, template_id),
  CONSTRAINT FK_cmt_campaign FOREIGN KEY (campaign_uid) REFERENCES teamandy.campaigns(uid) ON DELETE CASCADE
);
GO

/* ---------- 6. sequences ---------- */
CREATE TABLE teamandy.sequences (
  uid                           NVARCHAR(128) NOT NULL,
  name                          NVARCHAR(512) NULL,
  description                   NVARCHAR(MAX) NULL,
  status                        NVARCHAR(32)  NULL,
  language                      NVARCHAR(16)  NULL,
  sync_mode                     NVARCHAR(16)  NULL,
  owner_user_id                 NVARCHAR(128) NULL,  -- soft ref users.uid
  created_by                    NVARCHAR(128) NULL,  -- soft ref users.uid
  default_list_name             NVARCHAR(512) NULL,
  reply_io_sequence_id          INT           NULL,
  reply_io_fallback_sequence_id INT           NULL,
  reply_io_sync_status          NVARCHAR(32)  NULL,
  reply_io_sync_error           NVARCHAR(MAX) NULL,
  condition_branching_json      NVARCHAR(MAX) NULL,
  created_at                    DATETIME2(3)  NULL,
  updated_at                    DATETIME2(3)  NULL,
  CONSTRAINT PK_sequences PRIMARY KEY CLUSTERED (uid)
);
GO
CREATE NONCLUSTERED INDEX IX_sequences_owner_user_id ON teamandy.sequences(owner_user_id) WHERE owner_user_id IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_sequences_status        ON teamandy.sequences(status);
GO
CREATE TABLE teamandy.sequence_steps (
  uid                         NVARCHAR(128) NOT NULL,   -- step uid (uuid)
  sequence_uid                NVARCHAR(128) NOT NULL,
  step_order                  INT NOT NULL CONSTRAINT DF_ss_order DEFAULT(0),
  name                        NVARCHAR(512) NULL,
  channel                     NVARCHAR(32)  NULL,
  subject                     NVARCHAR(1024) NULL,
  body                        NVARCHAR(MAX) NULL,
  delay_days                  INT NOT NULL CONSTRAINT DF_ss_dd DEFAULT(0),
  delay_hours                 INT NOT NULL CONSTRAINT DF_ss_dh DEFAULT(0),
  reply_io_step_id            INT NULL,
  reply_io_variant_id         INT NULL,
  parent_condition_uid        NVARCHAR(128) NULL,
  branch                      NVARCHAR(16)  NULL,   -- positive|negative|NULL
  run_on_positive_branch_only BIT NOT NULL CONSTRAINT DF_ss_ropbo DEFAULT(0),
  custom_field_mapping_json   NVARCHAR(MAX) NULL,
  ai_hook_config_json         NVARCHAR(MAX) NULL,
  branching_json              NVARCHAR(MAX) NULL,
  CONSTRAINT PK_sequence_steps PRIMARY KEY CLUSTERED (sequence_uid, uid),
  CONSTRAINT FK_ss_seq FOREIGN KEY (sequence_uid) REFERENCES teamandy.sequences(uid) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_ss_seq_order ON teamandy.sequence_steps(sequence_uid, step_order);
GO
CREATE TABLE teamandy.sequence_conditions (
  uid             NVARCHAR(128) NOT NULL,
  sequence_uid    NVARCHAR(128) NOT NULL,
  anchor_step_uid    NVARCHAR(128) NULL,
  property           NVARCHAR(256) NULL,
  condition_operator NVARCHAR(32)  NULL,   -- 'operator' avoided (reserved-ish)
  condition_value    NVARCHAR(MAX) NULL,
  wait_minutes    INT NOT NULL CONSTRAINT DF_sc_wait DEFAULT(0),
  CONSTRAINT PK_sequence_conditions PRIMARY KEY CLUSTERED (sequence_uid, uid),
  CONSTRAINT FK_sc_seq FOREIGN KEY (sequence_uid) REFERENCES teamandy.sequences(uid) ON DELETE CASCADE
);
GO
CREATE TABLE teamandy.sequence_assigned_users (
  sequence_uid NVARCHAR(128) NOT NULL,
  user_uid     NVARCHAR(128) NOT NULL,   -- soft ref users.uid (not FK-enforced)
  CONSTRAINT PK_sequence_assigned_users PRIMARY KEY CLUSTERED (sequence_uid, user_uid),
  CONSTRAINT FK_sau_seq FOREIGN KEY (sequence_uid) REFERENCES teamandy.sequences(uid) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_sau_user ON teamandy.sequence_assigned_users(user_uid); -- array_contains(assignedUserIds)
GO

/* ---------- 7. leads  (the cost driver; nested arrays -> child tables) ---------- */
CREATE TABLE teamandy.leads (
  uid                          NVARCHAR(128)  NOT NULL,
  lead_list_id                 NVARCHAR(128)  NULL,   -- legacy scalar; soft ref lead_lists.uid
  company_name                 NVARCHAR(512)  NULL,
  domain                       NVARCHAR(512)  NULL,
  website                      NVARCHAR(1024) NULL,
  email                        NVARCHAR(320)  NULL,
  phone                        NVARCHAR(64)   NULL,
  linkedin_url                 NVARCHAR(1024) NULL,
  logo_url                     NVARCHAR(2048) NULL,
  industry                     NVARCHAR(256)  NULL,
  employee_count               INT            NULL,
  founding_year                INT            NULL,
  company_age                  INT            NULL,
  lead_score                   INT            NULL,
  lead_status                  NVARCHAR(64)   NULL,
  assigned_to                  NVARCHAR(128)  NULL,   -- soft ref users.uid
  campaign_id                  NVARCHAR(128)  NULL,   -- soft ref campaigns.uid
  current_office_status        NVARCHAR(128)  NULL,
  current_flex_office_provider NVARCHAR(256)  NULL,
  current_location             NVARCHAR(256)  NULL,
  language_preference          NVARCHAR(32)   NULL,
  source                       NVARCHAR(128)  NULL,
  buyer_person                 NVARCHAR(MAX)  NULL,
  description_of_company       NVARCHAR(MAX)  NULL,
  up_or_down_scaling_employees NVARCHAR(32)   NULL,
  talking_about_hybrid_remote_back_to_office BIT NULL,
  is_existing_customer         BIT NOT NULL CONSTRAINT DF_leads_iec  DEFAULT(0),
  excluded                     BIT NOT NULL CONSTRAINT DF_leads_excl DEFAULT(0),
  recent_funding               BIT NULL,
  rental_amount                FLOAT NULL,
  budget_indication            FLOAT NULL,
  hubspot_id                   NVARCHAR(128)  NULL,
  warm_up_date                 DATETIME2(3)   NULL,
  created_at                   DATETIME2(3)   NULL,   -- ETL: mixed int|ts -> DATETIME2
  updated_at                   DATETIME2(3)   NULL,
  last_enriched_at             DATETIME2(3)   NULL,
  industries_json              NVARCHAR(MAX)  NULL,
  countries_json               NVARCHAR(MAX)  NULL,
  cities_json                  NVARCHAR(MAX)  NULL,
  sourced_locations_json       NVARCHAR(MAX)  NULL,
  provider_data_json           NVARCHAR(MAX)  NULL,
  signals_json                 NVARCHAR(MAX)  NULL,
  kvk_data_json                NVARCHAR(MAX)  NULL,
  reviews_distribution_json    NVARCHAR(MAX)  NULL,
  about_social_media_json      NVARCHAR(MAX)  NULL,
  recent_funding_details_json  NVARCHAR(MAX)  NULL,
  interactions_json            NVARCHAR(MAX)  NULL,
  CONSTRAINT PK_leads PRIMARY KEY CLUSTERED (uid)
);
GO
CREATE NONCLUSTERED INDEX IX_leads_email        ON teamandy.leads(email)        WHERE email IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_leads_lead_list_id ON teamandy.leads(lead_list_id) WHERE lead_list_id IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_leads_created      ON teamandy.leads(created_at DESC, uid DESC);  -- keyset pagination
GO
CREATE NONCLUSTERED INDEX IX_leads_assigned_to  ON teamandy.leads(assigned_to) WHERE assigned_to IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_leads_campaign_id  ON teamandy.leads(campaign_id) WHERE campaign_id IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_leads_company_name ON teamandy.leads(company_name) INCLUDE (email, lead_list_id);  -- LIKE 'x%' prefix
GO
CREATE NONCLUSTERED INDEX IX_leads_domain       ON teamandy.leads(domain)  WHERE domain IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_leads_website      ON teamandy.leads(website) WHERE website IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_leads_lead_status  ON teamandy.leads(lead_status) WHERE lead_status IS NOT NULL;
GO
/* leadListIds array (array_contains) — dual with the scalar lead_list_id above */
CREATE TABLE teamandy.lead_lead_lists (
  lead_uid      NVARCHAR(128) NOT NULL,
  lead_list_uid NVARCHAR(128) NOT NULL,   -- NOT FK-enforced (orphan legacy list ids exist)
  CONSTRAINT PK_lead_lead_lists PRIMARY KEY CLUSTERED (lead_uid, lead_list_uid),
  CONSTRAINT FK_llls_lead FOREIGN KEY (lead_uid) REFERENCES teamandy.leads(uid) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_llls_leadlist ON teamandy.lead_lead_lists(lead_list_uid) INCLUDE (lead_uid);
GO
CREATE TABLE teamandy.lead_target_locations (
  lead_uid     NVARCHAR(128) NOT NULL,
  location_uid NVARCHAR(128) NOT NULL,   -- targetLocationIds array element (soft ref)
  CONSTRAINT PK_lead_target_locations PRIMARY KEY CLUSTERED (lead_uid, location_uid),
  CONSTRAINT FK_ltl_lead FOREIGN KEY (lead_uid) REFERENCES teamandy.leads(uid) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_ltl_location ON teamandy.lead_target_locations(location_uid);
GO
CREATE TABLE teamandy.lead_contact_persons (
  id                      BIGINT IDENTITY(1,1) NOT NULL,
  lead_uid                NVARCHAR(128) NOT NULL,
  source_array            NVARCHAR(24)  NOT NULL,  -- 'contact'|'enriched'|'selected'
  contact_id              NVARCHAR(128) NULL,
  first_name              NVARCHAR(256) NULL,
  last_name               NVARCHAR(256) NULL,
  job_title               NVARCHAR(512) NULL,
  normalized_job_title    NVARCHAR(512) NULL,
  email                   NVARCHAR(320) NULL,
  business_phone          NVARCHAR(64)  NULL,
  mobile_phone            NVARCHAR(64)  NULL,
  linkedin_profile        NVARCHAR(1024) NULL,
  decision_maker          BIT NULL,
  preferred_contact       NVARCHAR(32) NULL,
  recent_posts_json       NVARCHAR(MAX) NULL,
  interests_json          NVARCHAR(MAX) NULL,
  pain_points_json        NVARCHAR(MAX) NULL,
  personality_traits_json NVARCHAR(MAX) NULL,
  background_json         NVARCHAR(MAX) NULL,
  raw_json                NVARCHAR(MAX) NULL,  -- preserves basic_info/huspot_info shape reply+husport rely on
  CONSTRAINT PK_lead_contact_persons PRIMARY KEY CLUSTERED (id),
  CONSTRAINT FK_lcp_lead FOREIGN KEY (lead_uid) REFERENCES teamandy.leads(uid) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_lcp_lead  ON teamandy.lead_contact_persons(lead_uid);
GO
CREATE NONCLUSTERED INDEX IX_lcp_email ON teamandy.lead_contact_persons(email) WHERE email IS NOT NULL;
GO
CREATE TABLE teamandy.lead_notes (
  note_id    NVARCHAR(64)  NOT NULL,   -- noteId (uuid4) from Firestore
  lead_uid   NVARCHAR(128) NOT NULL,
  content    NVARCHAR(MAX) NULL,
  created_by NVARCHAR(128) NULL,       -- soft ref users.uid
  created_at DATETIME2(3)  NULL,
  CONSTRAINT PK_lead_notes PRIMARY KEY CLUSTERED (note_id),
  CONSTRAINT FK_ln_lead FOREIGN KEY (lead_uid) REFERENCES teamandy.leads(uid) ON DELETE CASCADE
);
GO
CREATE NONCLUSTERED INDEX IX_ln_lead ON teamandy.lead_notes(lead_uid);
GO

/* ---------- 8. contacts  (PK = lower(email)) ---------- */
CREATE TABLE teamandy.contacts (
  uid                   NVARCHAR(320) NOT NULL,  -- doc-id == lower(email)
  email                 NVARCHAR(320) NULL,
  email_lower           AS LOWER(email) PERSISTED,
  first_name            NVARCHAR(256) NULL,
  last_name             NVARCHAR(256) NULL,
  company               NVARCHAR(512) NULL,
  city                  NVARCHAR(256) NULL,
  source                NVARCHAR(128) NULL,
  phase                 NVARCHAR(64)  NULL,
  external_id           NVARCHAR(128) NULL,
  hubspot_deal_id       NVARCHAR(128) NULL,
  reply_io_contact_id   NVARCHAR(128) NULL,
  reply_count           INT NOT NULL CONSTRAINT DF_contacts_rc DEFAULT(0),
  forwarded_to_email    NVARCHAR(320) NULL,
  warmup_inbox_showing  BIT NULL,
  hubspot_insights_json NVARCHAR(MAX) NULL,
  created_at            DATETIME2(3) NULL,
  updated_at            DATETIME2(3) NULL,
  last_replied_at       DATETIME2(3) NULL,
  last_contacted_at     DATETIME2(3) NULL,
  date_to_reachout      DATETIME2(3) NULL,
  sent_to_warmup_at     DATETIME2(3) NULL,
  CONSTRAINT PK_contacts PRIMARY KEY CLUSTERED (uid)
);
GO
CREATE UNIQUE NONCLUSTERED INDEX UQ_contacts_email      ON teamandy.contacts(email_lower) WHERE email IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_contacts_phase             ON teamandy.contacts(phase) WHERE phase IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_contacts_reply_io_contact  ON teamandy.contacts(reply_io_contact_id) WHERE reply_io_contact_id IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_contacts_forwarded_to      ON teamandy.contacts(forwarded_to_email) WHERE forwarded_to_email IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_contacts_warmup_showing    ON teamandy.contacts(warmup_inbox_showing) WHERE warmup_inbox_showing = 1;
GO

/* ---------- 10. replies ---------- */
CREATE TABLE teamandy.replies (
  uid                      NVARCHAR(128) NOT NULL,
  contact_id               NVARCHAR(320) NULL,   -- soft ref contacts.uid
  sequence_id              NVARCHAR(128) NULL,   -- soft ref sequences.uid
  step_number              INT           NULL,
  channel                  NVARCHAR(32)  NULL,
  source                   NVARCHAR(64)  NULL,
  subject                  NVARCHAR(1024) NULL,
  reply_body               NVARCHAR(MAX) NULL,
  reply_body_html          NVARCHAR(MAX) NULL,
  original_body            NVARCHAR(MAX) NULL,
  sent_by                  NVARCHAR(320) NULL,
  sent_to_email            NVARCHAR(320) NULL,
  email                    NVARCHAR(320) NULL,
  conversation_id          NVARCHAR(256) NULL,
  sent_internet_message_id NVARCHAR(512) NULL,
  reply_message_id         NVARCHAR(512) NULL,
  actual_sender_email      NVARCHAR(320) NULL,
  actual_sender_name       NVARCHAR(256) NULL,
  linkedin_url             NVARCHAR(1024) NULL,
  replyio_event_id         NVARCHAR(128) NULL,
  email_account_id         NVARCHAR(128) NULL,   -- mixed int|str -> string
  raw_webhook_payload      NVARCHAR(MAX) NULL,
  reply_date               DATETIME2(3)  NULL,
  received_at              DATETIME2(3)  NULL,
  CONSTRAINT PK_replies PRIMARY KEY CLUSTERED (uid)
);
GO
CREATE NONCLUSTERED INDEX IX_replies_reply_message_id ON teamandy.replies(reply_message_id) WHERE reply_message_id IS NOT NULL;  -- non-unique: prod has dup message-ids
GO
CREATE NONCLUSTERED INDEX IX_replies_contact_id      ON teamandy.replies(contact_id) WHERE contact_id IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_replies_conversation_id ON teamandy.replies(conversation_id) WHERE conversation_id IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_replies_email           ON teamandy.replies(email) WHERE email IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_replies_received_at      ON teamandy.replies(received_at DESC);
GO
