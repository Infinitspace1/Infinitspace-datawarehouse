"""Declarative source->target mapping. The transform/load engines read this.
This is the executable form of ../manifest/field_mapping.md.

Coercion keys: 's' str, 'i' int, 'fl' float, 'b' bool, 'dt' datetime, 'js' json.
A column entry is "field" (default str) or ("field", "coerce").
PK source "__id" = the Firestore doc id.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Child:
    table: str
    source: str                       # parent array field name
    parent_fk: str                    # child column receiving the parent PK
    kind: str = "object"              # 'scalar_set' | 'object'
    element_col: Optional[str] = None # scalar_set: child col for the element value
    pk: Optional[tuple] = None        # object: (child_col, element_field) natural PK, else identity
    cols: dict = field(default_factory=dict)   # child col -> element field (or (field,coerce))

@dataclass
class Coll:
    name: str                         # Firestore collection
    target: str                       # 'sql'|'table'|'archive'|'drop'|'recompute'|'phantom'|'keyvault'
    table: Optional[str] = None
    pk: tuple = ("uid", "__id")       # (column, source-field)
    cols: dict = field(default_factory=dict)   # col -> field or (field, coerce)
    children: list = field(default_factory=list)
    subcollections: list = field(default_factory=list)  # names to walk during extract
    hook: Optional[str] = None        # transform.HOOKS key for irregular shapes
    note: str = ""

# convenience builders
def S(f): return (f, "s")
def I(f): return (f, "i")
def FL(f): return (f, "fl")
def B(f): return (f, "b")
def DT(f): return (f, "dt")
def JS(f): return (f, "js")

# =====================================================================
# CORE_CRM
# =====================================================================
COLLECTIONS = [
  Coll("users", "sql", "users",
    cols={"uuid": "uuid", "email": "email", "display_name": "displayName", "photo_url": "photoUrl",
          "role": "role", "is_admin": B("isAdmin"), "nexudus_connect": B("nexudusConnect"),
          "hubspot_access_token": B("hubspotAccessToken"), "session_token": "sessionToken",
          "session_expiry": DT("sessionExpiry"), "last_login": DT("lastLogin"),
          "last_activity": DT("lastActivity"), "created_at": DT("createdAt"),
          "settings_json": JS("settings"), "nexudus_account_json": JS("nexudusAccount"),
          "created_lead_lists_json": JS("createdLeadLists")}),

  Coll("lead_list_folders", "sql", "lead_list_folders",
    cols={"name": "name", "created_by": "createdBy", "created_at": DT("createdAt")}),

  Coll("locations", "sql", "locations",
    cols={"name": "name", "address": "address", "city": "city", "country": "country",
          "postal_code": "postalCode", "latitude": FL("latitude"), "longitude": FL("longitude"),
          "description": "description", "ups": "ups", "nexudus_id": "nexudusId", "hubspot_id": "hubSpotId",
          "total_size": FL("totalSize"), "total_size_measurement_unit": "totalSizeMeasurementUnit",
          "total_workspaces": I("totalWorkspaces"), "occupied_workspaces": I("occupiedWorkspaces"),
          "avg_occupancy_rate": FL("avg_occupancy_rate"), "amenities_json": JS("amenities"),
          "integrations_json": JS("integrations"), "created_at": DT("createdAt"), "updated_at": DT("updatedAt")},
    children=[
      Child("location_active_lead_lists", "activeLeadLists", "location_uid", "scalar_set", element_col="lead_list_uid"),
      Child("location_workspaces", "workspaces", "location_uid", "object", pk=("workspace_id", "workspaceId"),
            cols={"workspace_nexudus_id": "nexudusId", "public_name": "publicName", "internal_code": "internalCode",
                  "type": "type", "floor": "floor", "size_sq_m": FL("sizeSqM"), "capacity": I("capacity"),
                  "availability_status": "availabilityStatus", "status": "status",
                  "exclude_from_scraping": B("excludeFromScraping"), "booking": B("booking"),
                  "start_value": "start", "end_value": "end", "coworker_company_name": "coworkerCompanyName",
                  "coworker_name": "coworkerName", "coworker_price": FL("coworkerPrice"),
                  "coworker_id": "coworkerId", "coworker_tariff_name": "coworkerTariffName",
                  "price_details_json": JS("priceDetails"), "coworker_contracts_json": JS("coworkerContracts"),
                  "properties_json": JS("properties")}),
    ]),

  Coll("lead_lists", "sql", "lead_lists",
    cols={"name": "name", "status": "status", "source": "source", "scraping_type": "scrapingType",
          "scraping_frequency": I("scrapingFrequency"), "folder_id": "folderId", "created_by": "createdBy",
          "hubspot_id": "hubspotId", "estimated_monthly_cost": FL("estimatedMonthlyCost"),
          "total_actual_costs": FL("totalActualCosts"), "created_at": DT("createdAt"),
          "updated_at": DT("updatedAt"), "last_scraped_at": DT("lastScrapedAt"),
          "scraping_settings_json": JS("scrapingSettings"), "lead_warming_settings_json": JS("leadWarmingSettings"),
          "statistics_json": JS("statistics")},
    children=[
      Child("lead_list_locations", "locationIds", "lead_list_uid", "scalar_set", element_col="location_uid"),
    ],
    hook="lead_lists_statistics"),   # also emits teamandy.lead_list_statistics from statistics map

  Coll("campaigns", "sql", "campaigns",
    cols={"name": "name", "description": "description", "type": "type", "status": "status",
          "lead_list_id": "leadListId", "created_by": "createdBy", "start_date": DT("startDate"),
          "end_date": DT("endDate"), "created_at": DT("createdAt"), "updated_at": DT("updatedAt"),
          "target_audience_json": JS("targetAudience")},
    children=[
      Child("campaign_message_templates", "messageTemplates", "campaign_uid", "object",
            pk=("template_id", "templateId"),
            cols={"channel": "channel", "subject": "subject", "body": "body",
                  "template_order": I("order"), "delay_after_previous": I("delayAfterPrevious")}),
    ]),

  Coll("sequences", "sql", "sequences",
    cols={"name": "name", "description": "description", "status": "status", "language": "language",
          "sync_mode": "syncMode", "owner_user_id": "ownerUserId", "created_by": "createdBy",
          "default_list_name": "defaultListName", "reply_io_sequence_id": I("replyIoSequenceId"),
          "reply_io_fallback_sequence_id": I("replyIoFallbackSequenceId"),
          "reply_io_sync_status": "replyIoSyncStatus", "reply_io_sync_error": "replyIoSyncError",
          "condition_branching_json": JS("conditionBranching"), "created_at": DT("createdAt"),
          "updated_at": DT("updatedAt")},
    children=[
      Child("sequence_assigned_users", "assignedUserIds", "sequence_uid", "scalar_set", element_col="user_uid"),
      Child("sequence_steps", "steps", "sequence_uid", "object", pk=("uid", "uid"),
            cols={"step_order": I("order"), "name": "name", "channel": "channel", "subject": "subject",
                  "body": "body", "delay_days": I("delayDays"), "delay_hours": I("delayHours"),
                  "reply_io_step_id": I("replyIoStepId"), "reply_io_variant_id": I("replyIoVariantId"),
                  "parent_condition_uid": "parentConditionUid", "branch": "branch",
                  "run_on_positive_branch_only": B("runOnPositiveBranchOnly"),
                  "custom_field_mapping_json": JS("customFieldMapping"), "ai_hook_config_json": JS("aiHookConfig"),
                  "branching_json": JS("branching")}),
      Child("sequence_conditions", "conditions", "sequence_uid", "object", pk=("uid", "uid"),
            cols={"anchor_step_uid": "anchorStepUid", "property": "property",
                  "condition_operator": "operator", "condition_value": ("value", "s"),
                  "wait_minutes": I("waitMinutes")}),
    ]),

  Coll("leads", "sql", "leads",
    cols={"lead_list_id": "leadListId", "company_name": "companyName", "domain": "domain", "website": "website",
          "email": "email", "phone": "phone", "linkedin_url": "linkedinUrl", "logo_url": "logoUrl",
          "industry": "industry", "employee_count": I("employeeCount"), "founding_year": I("foundingYear"),
          "company_age": I("companyAge"), "lead_score": I("leadScore"), "lead_status": "leadStatus",
          "assigned_to": "assignedTo", "campaign_id": "campaignId", "current_office_status": "currentOfficeStatus",
          "current_flex_office_provider": "currentFlexOfficeProvider", "current_location": "currentLocation",
          "language_preference": "languagePreference", "source": "source", "buyer_person": "buyerPerson",
          "description_of_company": "descriptionOfCompany", "up_or_down_scaling_employees": "upOrDownScalingEmployees",
          "talking_about_hybrid_remote_back_to_office": B("talkingAboutHybridRemoteBackToOffice"),
          "is_existing_customer": B("isExistingCustomer"), "excluded": B("excluded"),
          "recent_funding": B("recentFunding"), "rental_amount": FL("rentalAmount"),
          "budget_indication": FL("budgetIndication"), "hubspot_id": "hubspotId", "warm_up_date": DT("warmUpDate"),
          "created_at": DT("createdAt"), "updated_at": DT("updatedAt"), "last_enriched_at": DT("lastEnrichedAt"),
          "industries_json": JS("industries"), "countries_json": JS("countries"), "cities_json": JS("cities"),
          "sourced_locations_json": JS("sourcedLocations"), "provider_data_json": JS("provider_data"),
          "signals_json": JS("signals"), "kvk_data_json": JS("kvk_data"),
          "reviews_distribution_json": JS("reviewsDistribution"), "about_social_media_json": JS("aboutSocialMedia"),
          "recent_funding_details_json": JS("recentFundingDetails"), "interactions_json": JS("interactions")},
    children=[
      Child("lead_lead_lists", "leadListIds", "lead_uid", "scalar_set", element_col="lead_list_uid"),
      Child("lead_target_locations", "targetLocationIds", "lead_uid", "scalar_set", element_col="location_uid"),
      Child("lead_notes", "notes", "lead_uid", "object", pk=("note_id", "noteId"),
            cols={"content": "content", "created_by": "createdBy", "created_at": DT("createdAt")}),
      # contactPersons / enrichedContactPersons / selectedContactPersons merged by the hook below
    ],
    hook="lead_contact_persons"),

  Coll("contacts", "sql", "contacts", pk=("uid", "__id"),
    cols={"email": "email", "first_name": "first_name", "last_name": "last_name", "company": "company",
          "city": "city", "source": "source", "phase": "phase", "external_id": "external_id",
          "hubspot_deal_id": "hubspot_deal_id", "reply_io_contact_id": "reply_io_contact_id",
          "reply_count": I("reply_count"), "forwarded_to_email": "forwarded_to_email",
          "warmup_inbox_showing": B("warmup_inbox_showing"), "hubspot_insights_json": JS("hubspot_insights"),
          "created_at": DT("created_at"), "updated_at": DT("updated_at"), "last_replied_at": DT("last_replied_at"),
          "last_contacted_at": DT("last_contacted_at"), "date_to_reachout": DT("date_to_reachout"),
          "sent_to_warmup_at": DT("sent_to_warmup_at")}),

  Coll("interactions", "sql", "interactions",
    cols={"lead_id": "leadId", "contact_id": "contactId", "campaign_id": "campaignId",
          "lead_list_id": "leadListId", "performed_by": "performedBy", "type": "type", "channel": "channel",
          "status": "status", "content": "content", "response": "response", "scheduled_at": DT("scheduledAt"),
          "completed_at": DT("completedAt"), "created_at": DT("createdAt"), "updated_at": DT("updatedAt")}),

  Coll("replies", "sql", "replies",
    cols={"contact_id": "contact_id", "sequence_id": "sequence_id", "step_number": I("step_number"),
          "channel": "channel", "source": "source", "subject": "subject", "reply_body": "reply_body",
          "reply_body_html": "reply_body_html", "original_body": "original_body", "sent_by": "sent_by",
          "sent_to_email": "sent_to_email", "email": "email", "conversation_id": "conversation_id",
          "sent_internet_message_id": "sent_internet_message_id", "reply_message_id": "reply_message_id",
          "actual_sender_email": "actual_sender_email", "actual_sender_name": "actual_sender_name",
          "linkedin_url": "linkedin_url", "replyio_event_id": "replyio_event_id",
          "email_account_id": ("email_account_id", "s"), "raw_webhook_payload": ("raw_webhook_payload", "s"),
          "reply_date": DT("reply_date"), "received_at": DT("received_at")}),

  Coll("tasks", "sql", "tasks",
    cols={"title": "title", "description": "description", "status": "status", "priority": "priority",
          "assigned_to": "assignedTo", "created_by": "createdBy", "related_lead_id": "relatedLeadId",
          "related_lead_list_id": "relatedLeadListId", "due_date": DT("dueDate"),
          "created_at": DT("createdAt"), "updated_at": DT("updatedAt")}),

  Coll("integrations", "sql", "integrations",
    cols={"integration_type": "integrationType", "status": "status", "error_log": "errorLog",
          "config_json": JS("config"), "field_mappings_json": JS("fieldMappings"),
          "last_sync_time": DT("lastSyncTime"), "created_at": DT("createdAt"), "updated_at": DT("updatedAt")}),

  # =====================================================================
  # SCRAPING_REF
  # =====================================================================
  Coll("organization_ids_apollo", "sql", "organization_ids_apollo", hook="org_ids_apollo_pivot"),

  Coll("job_title_mappings", "sql", "job_title_mappings", pk=("mapping_id", "__id"),
    cols={"original_job_title": "originalJobTitle", "synonyms_justification": "synonymsJustification",
          "synonyms_json": JS("synonyms")},
    children=[Child("job_title_synonyms", "synonyms", "mapping_id", "scalar_set", element_col="synonym")]),

  Coll("competence", "sql", "competence", hook="competence_legacy"),  # promote location.city/country

  Coll("competence_new", "sql", "competence_new",
    cols={"competitor_list_name": "competitor_list_name", "country": "country", "country_code": "country_code",
          "status": "status", "auto_managed": B("auto_managed"), "schema_version": I("schema_version"),
          "competitor_count": I("competitor_count"), "owner_user_uid": "uid",
          "apify_input_json": JS("apify_input"), "polygon_points_json": JS("polygon_points"),
          "competitors_legacy_json": JS("competitors_legacy"), "last_run_stats_json": JS("last_run_stats"),
          "last_error": ("last_error", "s"), "created_at": DT("created_at"), "updated_at": DT("updated_at"),
          "last_run_at": DT("last_run_at"), "migrated_at": DT("migrated_at")},
    subcollections=["competitors"]),   # competitors loaded via extract subcollection walk + competence_new_competitors map

  # competitors subcollection -> its own pseudo-collection record (see extract.py)
  Coll("__competitors", "sql", "competence_new_competitors", pk=("competitor_id", "__id"),
    cols={"list_uid": "__parent_id", "title": "title", "website": "website", "address": "address",
          "city": "city", "street": "street", "postal_code": "postalCode", "phone": "phone",
          "latitude": FL("latitude"), "longitude": FL("longitude"), "place_id": "placeId",
          "google_maps_url": "googleMapsUrl", "category_name": "categoryName",
          "created_at": DT("created_at"), "updated_at": DT("updated_at")}),

  Coll("scraping_jobs", "sql", "scraping_jobs",
    cols={"lead_list_id": "leadListId", "job_type": "type", "status": "status", "start_time": DT("startTime"),
          "end_time": DT("endTime"), "leads_generated": I("leadsGenerated"), "error_log": "errorLog",
          "config_snapshot_json": JS("configSnapshot"), "is_refresh": B("isRefresh"),
          "refresh_cleanup_status": "refreshCleanupStatus",
          "refresh_cleanup_snapshot_json": JS("refreshCleanupSnapshot"),
          "refresh_cleanup_at": DT("refreshCleanupAt"), "created_at": DT("createdAt"), "updated_at": DT("updatedAt")}),

  Coll("jobs_queue", "sql", "jobs_queue", pk=("lead_id", "__id"),
    cols={"status": "status", "error_message": "error_message",
          "created_at": DT("created_at"), "updated_at": DT("updated_at")}),

  Coll("deduplication_tracking", "sql", "deduplication_tracking", pk=("dedup_key", "__id"),
    cols={"place_id": "place_id", "lead_list_id": "leadListId", "deduped_to": "dedupedTo",
          "domain": "domain", "deduped_at": DT("dedupedAt")}),

  # =====================================================================
  # OPS_CONFIG
  # =====================================================================
  Coll("export_jobs", "sql", "export_jobs", pk=("job_id", "__id"),
    cols={"job_type": "job_type", "status": "status", "error_message": "error_message",
          "payload_json": JS("payload"), "progress_json": JS("progress"), "result_json": JS("result"),
          "created_at": DT("created_at"), "updated_at": DT("updated_at"), "completed_at": DT("completed_at")}),

  Coll("processed_webhook_events", "sql", "processed_webhook_events", pk=("event_id", "__id"),
    cols={"event_type": "event_type", "processed_at": DT("processed_at")}),

  Coll("warmup_handled_leads", "sql", "warmup_handled_leads", pk=("contact_id", "__id"),
    cols={"handled_by": "handledBy", "handled_at": DT("handledAt")}),

  Coll("warmup_reply_logs", "sql", "warmup_reply_logs", pk=("firestore_doc_id", "__id"),
    cols={"contact_id": "contactId", "contact_email": "contactEmail", "subject": "subject",
          "sent_by": "sentBy", "sent_via": "sentVia", "sent_internet_message_id": "sentInternetMessageId",
          "email_account_id": ("emailAccountId", "s"), "sent_at": DT("sentAt")}),

  Coll("settings", "sql", "settings", hook="settings_broker_firms"),
  Coll("users_with_access", "sql", "acl_users", hook="acl_users"),
  Coll("deleted", "sql", "deleted_tombstones", hook="deleted_tombstones"),

  Coll("analytics", "sql", "analytics_rollup", pk=("uid", "__id"),
    cols={"snapshot_date": DT("date"), "total_leads": I("totalLeads"), "new_leads_today": I("newLeadsToday"),
          "leads_scraped": I("leadsScraped"), "leads_converted_to_viewings": I("leadsConvertedToViewings"),
          "conversion_rate": FL("conversionRate"), "average_lead_score": FL("averageLeadScore"),
          "occupancy_rate": FL("occupancyRate"), "estimated_cost": FL("estimatedCost"),
          "revenue_generated": FL("revenueGenerated"), "top_performing_locations_json": JS("topPerformingLocations"),
          "leads_by_location_json": JS("leadsByLocation"), "leads_by_lead_list_json": JS("leadsByLeadList"),
          "top_lead_sources_json": JS("topLeadSources"), "metrics_json": JS("metrics")}),
  Coll("graph_subscription_monitor", "sql", "graph_subscription_monitor", pk=("doc_id", "__id"),
    cols={"overall_status": "overall_status", "checked_at": ("checked_at", "s"),
          "issue_signature": ("issue_signature", "s"), "summary_json": JS("summary"),
          "accounts_json": JS("accounts"), "issues_json": JS("issues"),
          "orphaned_subscriptions_json": JS("orphaned_subscriptions"), "reply_io_error": ("reply_io_error", "s"),
          "updated_at": DT("updated_at")}),

  Coll("company_index", "sql", "company_index", pk=("domain", "__id"),
    cols={"lead_uid": "leadUid", "website": "website", "status": "status", "company_name": "companyName",
          "created_at": DT("createdAt"), "updated_at": DT("updatedAt")},
    children=[
      Child("company_index_lead_list", "leadListIds", "domain", "scalar_set", element_col="lead_list_id"),
      Child("company_index_sourced_location", "sourcedLocations", "domain", "object",
            cols={"lead_list_id": "leadListId", "city": "city", "country": "country",
                  "place_id": "place_id", "maps_url": "maps_url"}),
    ]),

  # =====================================================================
  # CACHES -> Azure Table Storage (+ Blob spillover for >32KB bodies).
  # KEEP EVERYTHING (goal: fully close Firebase). Routing/keys = load_tables.CACHE_PLAN.
  # =====================================================================
  Coll("__apollo_neg", "table", "apolloNeg"),
  Coll("__lusha_neg", "table", "lushaNeg"),
  Coll("__lusha_quota", "table", "lushaQuota"),
  Coll("cache_scraping_settings", "table", "cacheScrapingSettings"),
  Coll("cache_unified_v2", "table", "cacheUnifiedV2"),
  Coll("company_enrichment_cache", "table", "companyEnrichmentCache"),
  Coll("cache_main_competence", "table", "cacheMainCompetence"),
  Coll("cache", "table", "cacheRoot", subcollections=["data"]),   # parent docs; data subcol -> cacheData
  Coll("__apollo", "table", "apollo"),
  Coll("__lusha", "table", "lusha"),
  Coll("__maps", "table", "maps"),
  Coll("cache_main", "table", "cacheMain"),
  Coll("test_leads_20250717_121935", "table", "testLeads"),   # test data — kept per 'import everything'
  Coll("cache_unified_workflow", "drop"),   # empty structural parent; its subcollections ARE migrated above
]

# Tables in FK-safe load order (parents before children/junctions).
TABLE_LOAD_ORDER = [
  "users", "lead_list_folders", "locations", "location_workspaces", "location_active_lead_lists",
  "lead_lists", "lead_list_statistics", "lead_list_locations",
  "campaigns", "campaign_message_templates",
  "sequences", "sequence_steps", "sequence_conditions", "sequence_assigned_users",
  "leads", "lead_lead_lists", "lead_target_locations", "lead_contact_persons", "lead_notes",
  "contacts", "interactions", "replies", "tasks", "integrations",
  "organization_ids_apollo", "job_title_mappings", "job_title_synonyms",
  "competence", "competence_new", "competence_new_competitors",
  "scraping_jobs", "jobs_queue", "deduplication_tracking",
  "export_jobs", "processed_webhook_events", "warmup_handled_leads", "warmup_reply_logs",
  "settings", "settings_broker_firms", "acl_users", "deleted_tombstones",
  "analytics_rollup", "graph_subscription_monitor",
  "company_index", "company_index_lead_list", "company_index_sourced_location",
]

# Exact ground-truth counts for validation gates.
GROUND_TRUTH_COUNTS = {
  "leads": 26437, "contacts": 4813, "replies": 971, "lead_lists": 110, "lead_list_folders": 9,
  "campaigns": 5, "sequences": 9, "interactions": 1, "tasks": 1, "users": 40, "locations": 9,
  "integrations": 1, "scraping_jobs": 288, "jobs_queue": 61, "export_jobs": 173,
  "processed_webhook_events": 78, "warmup_handled_leads": 192, "warmup_reply_logs": 110,
  "deduplication_tracking": 2805, "competence": 31, "competence_new": 30,
  "competence_new_competitors": 15012, "job_title_mappings": 13, "company_index": 20081,
  "analytics_rollup": 1, "graph_subscription_monitor": 1,
}

# ---------------------------------------------------------------------------
# Collections / tables the team decided NOT to migrate (confirmed unused — see the
# collection-usage audit). Applied centrally: extract/transform/load/validate skip these.
# ---------------------------------------------------------------------------
EXCLUDE_COLLECTIONS = {
    "interactions", "tasks", "integrations", "analytics", "deleted",      # SQL drops
    "cache_main", "cache_main_competence", "test_leads_20250717_121935",  # cache drops
}
EXCLUDE_TABLES = {
    "interactions", "tasks", "integrations", "analytics_rollup", "deleted_tombstones", "pre_leads",
}
EXCLUDE_CACHE_TABLES = {"cacheMain", "cacheMainCompetence", "testLeads"}

TABLE_LOAD_ORDER = [t for t in TABLE_LOAD_ORDER if t not in EXCLUDE_TABLES]
GROUND_TRUTH_COUNTS = {k: v for k, v in GROUND_TRUTH_COUNTS.items() if k not in EXCLUDE_TABLES}


def by_name(name):
    for c in COLLECTIONS:
        if c.name == name:
            return c
    return None
