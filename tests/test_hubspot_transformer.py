"""
tests/test_hubspot_transformer.py

Unit tests for the HubSpot marketing email transformer.

The stats/content shapes mirror REAL production payloads (hub 19741777,
captured 2026-06-10): counters use `hardbounced`/`softbounced`/`pending`/
`selected`, ratios are percentages, and drag-and-drop emails carry the
body in content.widgets[*].body.html (no plainTextVersion).
"""
import unittest
from datetime import datetime, timezone

from shared.hubspot.transformers.marketing_emails import transform_marketing_email


EMAIL = {
    "id": "123456789",
    "name": "June Newsletter",
    "subject": "What's new at InfinitSpace",
    "state": "PUBLISHED",
    "type": "BATCH_EMAIL",
    "language": "en",
    "archived": False,
    "isPublished": True,
    "campaign": "f1f1f1f1-aaaa-bbbb-cccc-123456789abc",
    "campaignName": "Newsletter 2026",
    "from": {"fromName": "InfinitSpace", "replyTo": "info@infinitspace.com"},
    "content": {
        "templatePath": "@hubspot/email/dnd/Start_from_scratch.html",
        "flexAreas": {"main": {"sections": []}},
        "styleSettings": {"backgroundColor": "#ffffff"},
        "widgets": {
            "module-0-0-0": {
                "id": "module-0-0-0",
                "order": 2,
                "type": "module",
                "body": {
                    "html": "<p>Hi {{ contact.firstname }},</p>\n<p>Here is <b>what's new</b> this month.</p>",
                    "path": "@hubspot/rich_text",
                },
            },
            "module-1-0-0": {
                "id": "module-1-0-0",
                "order": 5,
                "type": "module",
                "body": {"path": "@hubspot/email_footer", "align": "center"},
            },
            "module_extra": {
                "id": "module_extra",
                "order": 4,
                "type": "module",
                "body": {"html": "<p>PS: see you there!</p>", "path": "@hubspot/rich_text"},
            },
            "preview_text": {
                "id": "preview_text",
                "order": 0,
                "type": "text",
                "body": {"value": "A sneak peek inside"},
            },
            "primary_rich_text_module": {
                "id": "primary_rich_text_module",
                "module_id": 2285712,
                "name": "Primary Rich Text",
                "type": "module",
                "body": {
                    "html": "<p>Primary module HTML</p>",
                    "module_id": 2285712,
                },
            },
        },
    },
    "webversion": {"url": "https://hs.example.com/web-version"},
    "createdAt": "2026-06-01T08:00:00Z",
    "updatedAt": "2026-06-02T09:30:00Z",
    "publishedAt": "2026-06-02T10:00:00Z",
    "stats": {
        "counters": {
            "sent": 234, "open": 72, "delivered": 233, "bounce": 0,
            "unsubscribed": 1, "click": 5, "reply": 2, "dropped": 47,
            "selected": 281, "spamreport": 0, "suppressed": 0,
            "hardbounced": 3, "softbounced": 4, "pending": 1,
            "contactslost": 1, "notsent": 47,
        },
        "deviceBreakdown": {
            "open_device_type": {"computer": 143, "mobile": 16, "unknown": 0},
            "click_device_type": {"computer": 10, "mobile": 0, "unknown": 0},
        },
        "qualifierStats": {},
        "ratios": {
            "clickratio": 2.146, "clickthroughratio": 6.944,
            "deliveredratio": 99.573, "openratio": 30.901,
            "replyratio": 0.858, "unsubscribedratio": 0.429,
            "spamreportratio": 0.0, "bounceratio": 0.0,
            "hardbounceratio": 0.0, "softbounceratio": 0.0,
            "contactslostratio": 0.427, "pendingratio": 0.427,
            "notsentratio": 16.726,
        },
    },
}


class TestMarketingEmailTransformer(unittest.TestCase):
    def test_identity_fields(self):
        em = transform_marketing_email(EMAIL, bronze_id=11, sync_run_id="run-1")
        self.assertEqual(em["source_id"], "123456789")
        self.assertEqual(em["name"], "June Newsletter")
        self.assertEqual(em["subject"], "What's new at InfinitSpace")
        self.assertEqual(em["state"], "PUBLISHED")
        self.assertEqual(em["email_type"], "BATCH_EMAIL")
        self.assertEqual(em["archived"], 0)
        self.assertEqual(em["is_published"], 1)
        self.assertEqual(em["campaign_name"], "Newsletter 2026")

    def test_sender(self):
        em = transform_marketing_email(EMAIL, 1, "r")
        self.assertEqual(em["from_name"], "InfinitSpace")
        self.assertEqual(em["reply_to"], "info@infinitspace.com")
        self.assertEqual(em["web_version_url"], "https://hs.example.com/web-version")

    def test_body_html_concatenated_in_widget_order(self):
        em = transform_marketing_email(EMAIL, 1, "r")
        self.assertIn("what's new", em["body_html"])
        # order 2 module comes before order 4 module
        self.assertLess(
            em["body_html"].index("Hi {{ contact.firstname }}"),
            em["body_html"].index("PS: see you there!"),
        )
        # footer module has no html — must not break anything

    def test_body_plain_text_falls_back_to_stripped_html(self):
        em = transform_marketing_email(EMAIL, 1, "r")
        self.assertIn("what's new this month", em["body_plain_text"])
        self.assertNotIn("<p>", em["body_plain_text"])
        self.assertNotIn("<b>", em["body_plain_text"])

    def test_plain_text_version_preferred_when_present(self):
        raw = dict(EMAIL)
        raw["content"] = dict(EMAIL["content"], plainTextVersion="Plain version wins")
        em = transform_marketing_email(raw, 1, "r")
        self.assertEqual(em["body_plain_text"], "Plain version wins")

    def test_preview_and_template(self):
        em = transform_marketing_email(EMAIL, 1, "r")
        self.assertEqual(em["subject_preview_text"], "A sneak peek inside")
        self.assertEqual(em["template_path"], "@hubspot/email/dnd/Start_from_scratch.html")

    def test_content_widget_metadata_flattened(self):
        em = transform_marketing_email(EMAIL, 1, "r")
        self.assertEqual(em["content_widget_count"], 5)
        self.assertIn("Primary Rich Text", em["content_widget_names"])
        self.assertEqual(em["content_primary_widget_id"], "primary_rich_text_module")
        self.assertEqual(em["content_primary_widget_name"], "Primary Rich Text")
        self.assertEqual(em["content_primary_widget_type"], "module")
        self.assertEqual(em["content_primary_widget_module_id"], "2285712")
        self.assertEqual(em["content_primary_widget_body_module_id"], "2285712")
        self.assertEqual(em["content_primary_widget_html"], "<p>Primary module HTML</p>")

    def test_timestamps(self):
        em = transform_marketing_email(EMAIL, 1, "r")
        self.assertEqual(
            em["published_at"], datetime(2026, 6, 2, 10, 0, tzinfo=timezone.utc)
        )

    def test_epoch_millis_timestamp(self):
        raw = dict(EMAIL, createdAt=1750000000000)
        em = transform_marketing_email(raw, 1, "r")
        self.assertEqual(em["created_at"].year, 2025)

    def test_kpi_counters_real_keys(self):
        em = transform_marketing_email(EMAIL, 1, "r")
        self.assertEqual(em["stat_sent"], 234)
        self.assertEqual(em["stat_delivered"], 233)
        self.assertEqual(em["stat_opens"], 72)
        self.assertEqual(em["stat_clicks"], 5)
        self.assertEqual(em["stat_unsubscribed"], 1)
        self.assertEqual(em["stat_dropped"], 47)
        self.assertEqual(em["stat_selected"], 281)
        self.assertEqual(em["stat_pending"], 1)
        self.assertEqual(em["stat_suppressed"], 0)
        self.assertEqual(em["stat_not_sent"], 47)
        # production keys are hardbounced/softbounced (not hardbounces)
        self.assertEqual(em["stat_hard_bounces"], 3)
        self.assertEqual(em["stat_soft_bounces"], 4)
        self.assertEqual(em["stat_contacts_lost"], 1)

    def test_legacy_counter_aliases_still_work(self):
        raw = dict(
            EMAIL,
            stats={"counters": {"hardbounces": 7, "softBounces": 8, "notSent": 9}},
        )
        em = transform_marketing_email(raw, 1, "r")
        self.assertEqual(em["stat_hard_bounces"], 7)
        self.assertEqual(em["stat_soft_bounces"], 8)
        self.assertEqual(em["stat_not_sent"], 9)

    def test_kpi_ratios_are_percentages(self):
        em = transform_marketing_email(EMAIL, 1, "r")
        self.assertAlmostEqual(em["open_rate"], 30.901)
        self.assertAlmostEqual(em["click_rate"], 2.146)
        self.assertAlmostEqual(em["click_through_rate"], 6.944)
        self.assertAlmostEqual(em["delivered_rate"], 99.573)
        self.assertAlmostEqual(em["hard_bounce_rate"], 0.0)
        self.assertAlmostEqual(em["soft_bounce_rate"], 0.0)
        self.assertAlmostEqual(em["contacts_lost_rate"], 0.427)
        self.assertAlmostEqual(em["pending_rate"], 0.427)
        self.assertAlmostEqual(em["not_sent_rate"], 16.726)

    def test_device_breakdown(self):
        em = transform_marketing_email(EMAIL, 1, "r")
        self.assertEqual(em["opens_computer"], 143)
        self.assertEqual(em["opens_mobile"], 16)
        self.assertEqual(em["opens_unknown"], 0)
        self.assertEqual(em["clicks_computer"], 10)
        self.assertEqual(em["clicks_mobile"], 0)
        self.assertEqual(em["clicks_unknown"], 0)

    def test_draft_without_stats_or_content(self):
        raw = {"id": "9", "name": "Draft", "state": "DRAFT"}
        em = transform_marketing_email(raw, 1, "r")
        self.assertEqual(em["source_id"], "9")
        self.assertIsNone(em["stat_sent"])
        self.assertIsNone(em["open_rate"])
        self.assertIsNone(em["body_html"])
        self.assertIsNone(em["body_plain_text"])
        self.assertIsNone(em["opens_computer"])


if __name__ == "__main__":
    unittest.main()
