"""
shared/azure_clients/silver_writer_hubspot.py

Reads bronze.hubspot_marketing_emails, transforms, and MERGEs into
silver.hubspot_marketing_emails (single table, fully flat — no JSON columns).
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.hubspot.transformers.marketing_emails import transform_marketing_email

logger = logging.getLogger(__name__)

_COLUMNS = (
    "bronze_id", "sync_run_id",
    "name", "subject", "state", "email_type", "language",
    "archived", "is_published",
    "campaign_id", "campaign_name",
    "from_name", "reply_to",
    "subject_preview_text", "body_html", "body_plain_text",
    "template_path",
    "content_widget_count", "content_widget_names",
    "content_primary_widget_id", "content_primary_widget_name",
    "content_primary_widget_type", "content_primary_widget_module_id",
    "content_primary_widget_body_module_id", "content_primary_widget_html",
    "web_version_url",
    "created_at", "updated_at", "published_at",
    "stat_sent", "stat_delivered", "stat_opens", "stat_clicks",
    "stat_bounces", "stat_unsubscribed", "stat_replies",
    "stat_spam_reports", "stat_dropped", "stat_selected",
    "stat_pending", "stat_suppressed", "stat_not_sent",
    "stat_hard_bounces", "stat_soft_bounces", "stat_contacts_lost",
    "open_rate", "click_rate", "click_through_rate",
    "delivered_rate", "bounce_rate", "unsubscribed_rate",
    "reply_rate", "spam_report_rate", "hard_bounce_rate",
    "soft_bounce_rate", "contacts_lost_rate", "pending_rate", "not_sent_rate",
    "opens_computer", "opens_mobile", "opens_unknown",
    "clicks_computer", "clicks_mobile", "clicks_unknown",
)

_UPDATE_SET = ",\n        ".join(f"{column} = ?" for column in _COLUMNS)
_INSERT_COLUMNS = ", ".join(("source_id", *_COLUMNS))
_INSERT_PLACEHOLDERS = ", ".join("?" for _ in ("source_id", *_COLUMNS))

_MERGE_SQL = f"""
    MERGE silver.hubspot_marketing_emails AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        {_UPDATE_SET},
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT ({_INSERT_COLUMNS})
    VALUES ({_INSERT_PLACEHOLDERS});
"""


class SilverHubspotMarketingEmailsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze HubSpot marketing email records")

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                em = transform_marketing_email(raw, row["id"], self.sync_run_id)
                if not em["source_id"]:
                    raise ValueError("missing email id")
                params_list.append(self._make_params(em))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver HubSpot marketing emails: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "marketing_emails": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.hubspot_marketing_emails",
            source_name="hubspot",
            entity="marketing_emails",
        )

    def _make_params(self, em: dict) -> tuple:
        vals = tuple(em[column] for column in _COLUMNS)
        return (em["source_id"], *vals, em["source_id"], *vals)
