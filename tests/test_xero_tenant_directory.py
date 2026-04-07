import unittest
from datetime import datetime, timedelta, timezone

from shared.xero.store import StoredXeroConnection
from shared.xero.tenant_directory import XeroTenantDirectoryService


LOCATIONS = [
    {
        "source_id": 2,
        "nexudus_uuid": "uuid-c29",
        "name": "Berlin - Mitte - Chausseestrasse 29",
        "web_address": "chausseestrasse",
        "address": "Chausseestrasse 29",
        "postal_code": "10115",
        "city": "Berlin",
        "state": None,
        "country_name": "Germany",
        "currency_code": "EUR",
        "email": "chausseestrasse.reception@wearebeyond.work",
        "web_contact": "https://www.wearebeyond.work/beyond-chausseestrasse",
    },
    {
        "source_id": 3,
        "nexudus_uuid": "uuid-heidestrasse",
        "name": "Berlin - Mitte - Heidestrasse 34",
        "web_address": "qh",
        "address": "Heidestrasse 34",
        "postal_code": "10557",
        "city": "Berlin",
        "state": "Berlin",
        "country_name": "Germany",
        "currency_code": "EUR",
        "email": "qh.reception@wearebeyond.work",
        "web_contact": "https://www.wearebeyond.work/beyond-quartier-heidestrasse",
    },
    {
        "source_id": 4,
        "nexudus_uuid": "uuid-herengracht",
        "name": "Amsterdam - Center - Herengracht 471",
        "web_address": "beyondgoudenbocht",
        "address": "Herengracht 471",
        "postal_code": "1017 BS",
        "city": "Amsterdam",
        "state": "Noord-Holland",
        "country_name": "Netherlands",
        "currency_code": "EUR",
        "email": "goudenbocht.reception@wearebeyond.work",
        "web_contact": "https://www.wearebeyond.work/beyond-gouden-bocht",
    },
    {
        "source_id": 5,
        "nexudus_uuid": "uuid-bower",
        "name": "London - Old Street - 207 Old Street",
        "web_address": "beyondthebower",
        "address": "207 Old Street",
        "postal_code": "EC1V 9NR",
        "city": "London",
        "state": "England",
        "country_name": "United Kingdom",
        "currency_code": "GBP",
        "email": "thebower.reception@wearebeyond.work",
        "web_contact": None,
    },
    {
        "source_id": 8,
        "nexudus_uuid": "uuid-holborn",
        "name": "London - Holborn - 229-231 High Holborn",
        "web_address": "beyondkingsbournehouse",
        "address": "229-231 High Holborn",
        "postal_code": "WC1V 7DN",
        "city": "London",
        "state": "England",
        "country_name": "United Kingdom",
        "currency_code": "GBP",
        "email": "kingsbournehouse.reception@wearebeyond.work",
        "web_contact": "https://www.wearebeyond.work/beyond-kingsbourne-house",
    },
    {
        "source_id": 9,
        "nexudus_uuid": "uuid-aldgate",
        "name": "London - Aldgate - 2 Leman Street",
        "web_address": "beyondaldgatetower",
        "address": "2 Leman Street",
        "postal_code": "E1 8FA",
        "city": "London",
        "state": "England",
        "country_name": "United Kingdom",
        "currency_code": "GBP",
        "email": "aldgatetower.reception@wearebeyond.work",
        "web_contact": "https://www.wearebeyond.work/beyond-aldgate-tower",
    },
]

TENANTS = [
    {
        "id": 11,
        "xero_connection_id": 1,
        "xero_tenant_id": "tenant-bower",
        "xero_tenant_type": "ORGANISATION",
        "tenant_name": "beyond The Bower",
        "connection_id": "conn-bower",
        "auth_event_id": "auth-bower",
        "raw_payload": "{}",
    },
    {
        "id": 12,
        "xero_connection_id": 1,
        "xero_tenant_id": "tenant-heidestrasse",
        "xero_tenant_type": "ORGANISATION",
        "tenant_name": "beyond Quartier Heidestrasse GmbH",
        "connection_id": "conn-heidestrasse",
        "auth_event_id": "auth-heidestrasse",
        "raw_payload": "{}",
    },
    {
        "id": 13,
        "xero_connection_id": 1,
        "xero_tenant_id": "tenant-aldgate",
        "xero_tenant_type": "ORGANISATION",
        "tenant_name": "infinitSpace Aldgate UK Ltd",
        "connection_id": "conn-aldgate",
        "auth_event_id": "auth-aldgate",
        "raw_payload": "{}",
    },
    {
        "id": 14,
        "xero_connection_id": 1,
        "xero_tenant_id": "tenant-c29",
        "xero_tenant_type": "ORGANISATION",
        "tenant_name": "infinitSpace Germany Two GmbH (C29)",
        "connection_id": "conn-c29",
        "auth_event_id": "auth-c29",
        "raw_payload": "{}",
    },
    {
        "id": 15,
        "xero_connection_id": 1,
        "xero_tenant_id": "tenant-herengracht",
        "xero_tenant_type": "ORGANISATION",
        "tenant_name": "InfinitSpace Herengracht 471 BV",
        "connection_id": "conn-herengracht",
        "auth_event_id": "auth-herengracht",
        "raw_payload": "{}",
    },
]


class FakeDirectorySQLClient:
    def __init__(self, locations):
        self.locations = locations
        self.non_queries = []

    def execute_query(self, query, params=None):
        if "FROM silver.nexudus_locations" in query:
            return list(self.locations)
        raise AssertionError(f"Unexpected query: {query}")

    def execute_non_query(self, query, params=None):
        self.non_queries.append((query, params))
        return 1


class FakeDirectoryStore:
    def __init__(self, tenants):
        self.tenants = tenants
        self.connection = StoredXeroConnection(
            id=1,
            owner_type="workspace",
            owner_id="default",
            xero_user_id="xero-user-1",
            access_token="access-ok",
            refresh_token="refresh-ok",
            id_token=None,
            scope="offline_access accounting.invoices",
            token_type="Bearer",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
            selected_xero_tenant_id="tenant-bower",
            is_connected=True,
            last_error=None,
        )

    def get_connection(self, owner_type=None, owner_id=None, connection_id=None):
        return self.connection

    def list_tenants(self, owner_type=None, owner_id=None, connection_id=None):
        return list(self.tenants)


class TestXeroTenantDirectory(unittest.TestCase):
    def test_build_directory_rows_matches_current_known_tenants(self):
        service = XeroTenantDirectoryService(
            sql_client=FakeDirectorySQLClient(LOCATIONS),
            store=FakeDirectoryStore(TENANTS),
        )

        rows = service.build_directory_rows(TENANTS, LOCATIONS)
        matched = {row["tenant_name"]: row["location_source_id"] for row in rows}

        self.assertEqual(matched["beyond The Bower"], 5)
        self.assertEqual(matched["beyond Quartier Heidestrasse GmbH"], 3)
        self.assertEqual(matched["infinitSpace Aldgate UK Ltd"], 9)
        self.assertEqual(matched["infinitSpace Germany Two GmbH (C29)"], 2)
        self.assertEqual(matched["InfinitSpace Herengracht 471 BV"], 4)

    def test_build_directory_rows_leaves_unknown_tenant_unmatched(self):
        service = XeroTenantDirectoryService(
            sql_client=FakeDirectorySQLClient(LOCATIONS),
            store=FakeDirectoryStore([]),
        )

        rows = service.build_directory_rows(
            [
                {
                    "xero_connection_id": 1,
                    "xero_tenant_id": "tenant-unknown",
                    "xero_tenant_type": "ORGANISATION",
                    "tenant_name": "InfinitSpace Madrid One SL",
                    "connection_id": "conn-madrid",
                    "auth_event_id": "auth-madrid",
                    "raw_payload": "{}",
                }
            ],
            LOCATIONS,
        )

        self.assertIsNone(rows[0]["location_source_id"])
        self.assertEqual(rows[0]["location_match_rule"], "unmatched")

    def test_refresh_tenants_upserts_rows_and_preserves_manual_community_manager(self):
        sql = FakeDirectorySQLClient(LOCATIONS)
        service = XeroTenantDirectoryService(
            sql_client=sql,
            store=FakeDirectoryStore(TENANTS),
        )

        stats = service.refresh_tenants(connection_id=1)

        self.assertEqual(stats["tenant_count"], 5)
        self.assertEqual(stats["matched_count"], 5)
        merge_queries = [query for query, _ in sql.non_queries if "MERGE silver.xero_tenants" in query]
        self.assertEqual(len(merge_queries), 5)
        self.assertIn("community_manager_name = target.community_manager_name", merge_queries[0])
        self.assertTrue(
            any("DELETE FROM silver.xero_tenants" in query for query, _ in sql.non_queries)
        )


if __name__ == "__main__":
    unittest.main()
