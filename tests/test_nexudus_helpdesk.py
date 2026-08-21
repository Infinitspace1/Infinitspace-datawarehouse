"""
tests/test_nexudus_helpdesk.py

Unit tests for the Nexudus help-desk ingestion: pure transformers, the
webhook routing/signature helpers, and MERGE placeholder parity.

No credentials, no network, no database.

    .\venv\Scripts\python.exe -m unittest tests.test_nexudus_helpdesk
"""
from __future__ import annotations

import re
import unittest
import unittest.mock
import uuid
from datetime import datetime, timezone

from shared.nexudus.helpdesk import (
    COMMENTS,
    DEPARTMENTS,
    ENTITIES,
    MESSAGES,
    WEBHOOK_ACTIONS,
    compute_signature,
    extract_source_id,
    incremental_params,
    resolve_entity,
    verify_signature,
)
from shared.nexudus.transformers.helpdesk_comments import transform_helpdesk_comment
from shared.nexudus.transformers.helpdesk_departments import transform_helpdesk_department
from shared.nexudus.transformers.helpdesk_messages import transform_helpdesk_message

RUN_ID = str(uuid.uuid4())

# Shapes copied from live payloads (2026-08-20).
RAW_MESSAGE = {
    "MinutesToClose": -41.65,
    "BusinessId": 1420976575,
    "CoworkerId": 1419993316,
    "CoworkerFullName": "Martha FELIX",
    "HelpDeskDepartmentId": 1414790129,
    "HelpDeskDepartmentName": "Air con queries",
    "Subject": "\U0001f976 We are freezing  \U0001f9ca",
    "MessageText": "AC is blowing a lot of chilly air at our office.",
    "Priority": 2,
    "AiProcessingResult": 0,
    "SupportIssueCategory": None,
    "Closed": True,
    "ClosedOn": "2026-08-04T15:28:18Z",
    "OwnerId": None,
    "OwnerFullName": None,
    "FirstResponseTimeInMinutes": 42,
    "AiChannelSessionId": None,
    "ImageFileName": None,
    "Id": 1416023860,
    "UpdatedOn": "2026-08-04T15:28:18Z",
    "CreatedOn": "2026-08-04T14:46:39Z",
    "UniqueId": "7f90bd59-e5da-4194-b15d-75985abb7b54",
    "UpdatedBy": "foxcourt.reception@wearebeyond.work",
    "IsNew": False,
    "SystemId": None,
    "ToStringText": "\U0001f976 We are freezing  \U0001f9ca",
    "LocalizationDetails": None,
    "CustomFields": None,
}

RAW_COMMENT = {
    "CoworkerFullName": "Lauren Ross",
    "CoworkerId": 1415651020,
    "CreatedOn": "2022-02-28T12:09:33Z",
    "HelpDeskMessageId": 1415051110,
    "Id": 1415087491,
    "ImageFileName": None,
    "Internal": False,
    "MessageText": "The invoice has been sent to Sofia.",
    "UniqueId": "bcb1615c-bf52-4f42-b122-88d0eecb7748",
    "UpdatedBy": "lauren.ross@infinitspace.com",
    "UpdatedOn": "2022-02-28T12:09:33Z",
}

RAW_DEPARTMENT = {
    "Active": True,
    "BusinessId": 1421021924,
    "CreatedOn": "2026-08-19T17:53:06Z",
    "Description": "Any spillages, milk needs topping up etc let us know here ",
    "Id": 1414804366,
    "Name": "Cleaning queries",
    "TaskListId": 0,
    "UniqueId": "e416b8d2-d464-4d5b-8eba-e7a67f68d37c",
    "UpdatedBy": "wilco.wijnbergen@infinitspace.com",
    "UpdatedOn": "2026-08-19T17:53:06Z",
}


class TestMessageTransformer(unittest.TestCase):

    def setUp(self):
        self.row = transform_helpdesk_message(RAW_MESSAGE, 99, RUN_ID)

    def test_core_field_mapping(self):
        self.assertEqual(self.row["source_id"], 1416023860)
        self.assertEqual(self.row["location_source_id"], 1420976575)
        self.assertEqual(self.row["coworker_source_id"], 1419993316)
        self.assertEqual(self.row["department_source_id"], 1414790129)
        self.assertEqual(self.row["department_name"], "Air con queries")
        self.assertEqual(self.row["first_response_minutes"], 42)
        self.assertEqual(self.row["bronze_id"], 99)

    def test_emoji_subject_survives(self):
        self.assertIn("freezing", self.row["subject"])

    def test_closed_flag_is_a_bit(self):
        self.assertEqual(self.row["is_closed"], 1)
        self.assertEqual(
            transform_helpdesk_message({**RAW_MESSAGE, "Closed": False}, 1, RUN_ID)["is_closed"],
            0,
        )

    def test_minutes_to_close_is_recomputed_not_copied(self):
        """The source MinutesToClose is -41.65; the real elapsed time is +41.65."""
        self.assertEqual(self.row["minutes_to_close"], 41.65)
        self.assertNotEqual(self.row["minutes_to_close"], RAW_MESSAGE["MinutesToClose"])

    def test_minutes_to_close_none_when_not_closed(self):
        raw = {**RAW_MESSAGE, "Closed": False, "ClosedOn": None}
        self.assertIsNone(transform_helpdesk_message(raw, 1, RUN_ID)["minutes_to_close"])

    def test_minutes_to_close_none_when_timestamps_inverted(self):
        """A closed-before-created record must not produce a negative average."""
        raw = {**RAW_MESSAGE, "ClosedOn": "2026-08-04T14:00:00Z"}
        self.assertIsNone(transform_helpdesk_message(raw, 1, RUN_ID)["minutes_to_close"])

    def test_closed_without_closed_on_is_tolerated(self):
        """~2% of closed tickets carry no ClosedOn."""
        raw = {**RAW_MESSAGE, "ClosedOn": None}
        row = transform_helpdesk_message(raw, 1, RUN_ID)
        self.assertEqual(row["is_closed"], 1)
        self.assertIsNone(row["closed_on"])
        self.assertIsNone(row["minutes_to_close"])

    def test_timestamps_parsed_to_aware_datetimes(self):
        self.assertIsInstance(self.row["created_on"], datetime)
        self.assertEqual(self.row["created_on"].tzinfo, timezone.utc)

    def test_null_owner_and_category(self):
        self.assertIsNone(self.row["owner_source_id"])
        self.assertIsNone(self.row["support_issue_category"])

    def test_missing_department_is_allowed(self):
        raw = {**RAW_MESSAGE, "HelpDeskDepartmentId": None, "HelpDeskDepartmentName": None}
        row = transform_helpdesk_message(raw, 1, RUN_ID)
        self.assertIsNone(row["department_source_id"])
        self.assertIsNone(row["department_name"])


class TestCommentTransformer(unittest.TestCase):

    def test_field_mapping_and_inherited_location(self):
        row = transform_helpdesk_comment(RAW_COMMENT, 7, RUN_ID, location_source_id=1376491118)
        self.assertEqual(row["source_id"], 1415087491)
        self.assertEqual(row["helpdesk_message_source_id"], 1415051110)
        self.assertEqual(row["location_source_id"], 1376491118)
        self.assertEqual(row["coworker_source_id"], 1415651020)
        self.assertEqual(row["is_internal"], 0)

    def test_location_optional(self):
        """An unresolved parent must not break the row."""
        row = transform_helpdesk_comment(RAW_COMMENT, 7, RUN_ID)
        self.assertIsNone(row["location_source_id"])

    def test_internal_flag(self):
        row = transform_helpdesk_comment({**RAW_COMMENT, "Internal": True}, 7, RUN_ID)
        self.assertEqual(row["is_internal"], 1)


class TestDepartmentTransformer(unittest.TestCase):

    def test_field_mapping(self):
        row = transform_helpdesk_department(RAW_DEPARTMENT, 3, RUN_ID)
        self.assertEqual(row["source_id"], 1414804366)
        self.assertEqual(row["location_source_id"], 1421021924)
        self.assertEqual(row["name"], "Cleaning queries")
        self.assertEqual(row["is_active"], 1)
        self.assertEqual(row["task_list_id"], 0)

    def test_description_is_stripped(self):
        row = transform_helpdesk_department(RAW_DEPARTMENT, 3, RUN_ID)
        self.assertFalse(row["description"].endswith(" "))


class TestWebhookRouting(unittest.TestCase):

    def test_action_codes_match_the_nexudus_enum(self):
        self.assertEqual(WEBHOOK_ACTIONS[45], MESSAGES)
        self.assertEqual(WEBHOOK_ACTIONS[46], COMMENTS)

    def test_resource_query_param_wins(self):
        self.assertEqual(resolve_entity({}, resource="HelpDeskMessage"), MESSAGES)
        self.assertEqual(resolve_entity({}, resource="HelpDeskComment"), COMMENTS)
        self.assertEqual(resolve_entity({}, resource="helpdeskcomments"), COMMENTS)

    def test_action_name_with_nexudus_typo(self):
        """Nexudus spells action 45 'HelDeskMessageCreated' — one 'p' missing."""
        self.assertEqual(resolve_entity({}, action="HelDeskMessageCreated"), MESSAGES)
        self.assertEqual(resolve_entity({}, action="HelpDeskMessageCreated"), MESSAGES)
        self.assertEqual(resolve_entity({}, action="HelpDeskCommentCreated"), COMMENTS)

    def test_numeric_action(self):
        self.assertEqual(resolve_entity({}, action="45"), MESSAGES)
        self.assertEqual(resolve_entity({}, action="46"), COMMENTS)

    def test_payload_shape_fallback(self):
        self.assertEqual(resolve_entity(RAW_COMMENT), COMMENTS)
        self.assertEqual(resolve_entity(RAW_MESSAGE), MESSAGES)

    def test_unroutable_returns_none(self):
        self.assertIsNone(resolve_entity({"Foo": 1}))

    def test_extract_id_flat(self):
        self.assertEqual(extract_source_id(RAW_MESSAGE), 1416023860)

    def test_extract_id_from_envelope(self):
        self.assertEqual(extract_source_id({"Value": {"Id": 42}}), 42)
        self.assertEqual(extract_source_id({"Data": {"Record": {"Id": 43}}}), 43)

    def test_extract_id_from_list(self):
        self.assertEqual(extract_source_id([{"Id": 44}]), 44)

    def test_extract_id_string_coerced(self):
        self.assertEqual(extract_source_id({"Id": "45"}), 45)

    def test_extract_id_missing(self):
        self.assertIsNone(extract_source_id({"Subject": "no id"}))
        self.assertIsNone(extract_source_id(None))


class TestSignatureVerification(unittest.TestCase):

    BODY = b'{"Id":1416023860}'
    SECRET = "s3cr3t"

    def _env(self, **kwargs):
        return unittest.mock.patch.dict("os.environ", kwargs, clear=False)

    def test_accepts_when_no_secret_configured(self):
        with unittest.mock.patch.dict("os.environ", {}, clear=True):
            ok, reason = verify_signature(self.BODY, None)
        self.assertTrue(ok)
        self.assertIn("function key", reason)

    def test_hex_signature_matches(self):
        hex_sig, _ = compute_signature(self.BODY, self.SECRET)
        with self._env(NEXUDUS_WEBHOOK_SECRET=self.SECRET,
                       NEXUDUS_WEBHOOK_SIGNATURE_MODE="enforce"):
            ok, reason = verify_signature(self.BODY, hex_sig)
        self.assertTrue(ok)
        self.assertEqual(reason, "signature verified")

    def test_base64_signature_matches(self):
        _, b64_sig = compute_signature(self.BODY, self.SECRET)
        with self._env(NEXUDUS_WEBHOOK_SECRET=self.SECRET,
                       NEXUDUS_WEBHOOK_SIGNATURE_MODE="enforce"):
            ok, _ = verify_signature(self.BODY, b64_sig)
        self.assertTrue(ok)

    def test_enforce_rejects_bad_signature(self):
        with self._env(NEXUDUS_WEBHOOK_SECRET=self.SECRET,
                       NEXUDUS_WEBHOOK_SIGNATURE_MODE="enforce"):
            ok, reason = verify_signature(self.BODY, "deadbeef")
        self.assertFalse(ok)
        self.assertEqual(reason, "signature mismatch")

    def test_warn_mode_accepts_bad_signature_but_says_so(self):
        with self._env(NEXUDUS_WEBHOOK_SECRET=self.SECRET,
                       NEXUDUS_WEBHOOK_SIGNATURE_MODE="warn"):
            ok, reason = verify_signature(self.BODY, "deadbeef")
        self.assertTrue(ok)
        self.assertIn("mismatch", reason)

    def test_off_mode_skips_entirely(self):
        with self._env(NEXUDUS_WEBHOOK_SECRET=self.SECRET,
                       NEXUDUS_WEBHOOK_SIGNATURE_MODE="off"):
            ok, _ = verify_signature(self.BODY, None)
        self.assertTrue(ok)

    def test_body_tampering_changes_the_digest(self):
        a, _ = compute_signature(self.BODY, self.SECRET)
        b, _ = compute_signature(b'{"Id":9}', self.SECRET)
        self.assertNotEqual(a, b)


class TestIncrementalParams(unittest.TestCase):

    def test_first_run_is_a_full_fetch(self):
        self.assertEqual(incremental_params(ENTITIES[MESSAGES], has_previous_run=False), {})

    def test_subsequent_runs_use_the_nexudus_native_filter(self):
        params = incremental_params(ENTITIES[MESSAGES], has_previous_run=True)
        self.assertIn("from_HelpDeskMessage_UpdatedOn", params)
        # UpdatedSince is silently ignored by these endpoints — never send it.
        self.assertNotIn("UpdatedSince", params)

    def test_every_entity_has_a_distinct_filter_and_endpoint(self):
        filters = {e.updated_filter for e in ENTITIES.values()}
        endpoints = {e.endpoint for e in ENTITIES.values()}
        self.assertEqual(len(filters), 3)
        self.assertEqual(len(endpoints), 3)
        for entity in ENTITIES.values():
            self.assertTrue(entity.endpoint.startswith("support/"))


class TestMergeParity(unittest.TestCase):
    """A placeholder/param mismatch in a MERGE fails at runtime, not import."""

    def _check(self, module, cls_name, row):
        merge = module._MERGE_SQL
        params = getattr(module, cls_name)._make_params(None, row)
        self.assertEqual(
            merge.count("?"), len(params),
            f"{cls_name}: {merge.count('?')} placeholders vs {len(params)} params",
        )
        insert_cols = re.search(r"INSERT \(([^)]*)\)", merge, re.S).group(1)
        cols = [c.strip() for c in insert_cols.replace("\n", " ").split(",") if c.strip()]
        self.assertEqual(len(cols), len(params) // 2)

    def test_messages(self):
        from shared.azure_clients import silver_writer_helpdesk_messages as m
        self._check(m, "SilverHelpdeskMessagesWriter",
                    transform_helpdesk_message(RAW_MESSAGE, 1, RUN_ID))

    def test_comments(self):
        from shared.azure_clients import silver_writer_helpdesk_comments as m
        self._check(m, "SilverHelpdeskCommentsWriter",
                    transform_helpdesk_comment(RAW_COMMENT, 1, RUN_ID, location_source_id=1))

    def test_departments(self):
        from shared.azure_clients import silver_writer_helpdesk_departments as m
        self._check(m, "SilverHelpdeskDepartmentsWriter",
                    transform_helpdesk_department(RAW_DEPARTMENT, 1, RUN_ID))


class TestEntityRegistration(unittest.TestCase):
    """The three entities must be wired into every place that drives them."""

    def test_registered_in_silver_worker(self):
        from functions.silver_worker import _ENTITY_MAP
        for key in (MESSAGES, COMMENTS, DEPARTMENTS):
            self.assertIn(key, _ENTITY_MAP)

    def test_registered_in_nightly_fanout(self):
        from functions.silver_nexudus import ENTITIES as FANOUT
        for key in (MESSAGES, COMMENTS, DEPARTMENTS):
            self.assertIn(key, FANOUT)

    def test_registered_in_weekly_reconcile(self):
        from functions.nexudus_silver_reconcile import ENTITIES as RECONCILE
        names = {c.entity for c in RECONCILE}
        for key in (MESSAGES, COMMENTS, DEPARTMENTS):
            self.assertIn(key, names)

    def test_reconcile_floors_match_the_registry(self):
        from functions.nexudus_silver_reconcile import ENTITIES as RECONCILE
        by_name = {c.entity: c for c in RECONCILE}
        for key, entity in ENTITIES.items():
            self.assertEqual(by_name[key].min_ids, entity.min_ids)
            self.assertEqual(by_name[key].endpoint, entity.endpoint)

    def test_sync_order_writes_messages_before_comments(self):
        """Comments inherit their location from the parent message's bronze row."""
        from functions.nexudus_helpdesk_sync import SYNC_ORDER
        self.assertLess(SYNC_ORDER.index(MESSAGES), SYNC_ORDER.index(COMMENTS))


if __name__ == "__main__":
    unittest.main()
