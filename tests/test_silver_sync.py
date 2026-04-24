import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from shared.azure_clients import silver_sync


class TestSilverSync(unittest.TestCase):

    @patch("shared.azure_clients.silver_sync.get_sql_client")
    def test_get_last_successful_run_started_at_queries_meta_sync_runs(self, mock_get_sql_client):
        sql = MagicMock()
        started_at = datetime(2026, 4, 24, 5, 30, tzinfo=timezone.utc)
        sql.execute_scalar.return_value = started_at
        mock_get_sql_client.return_value = sql

        result = silver_sync.get_last_successful_run_started_at("nexudus", "products")

        self.assertEqual(result, started_at)
        query, params = sql.execute_scalar.call_args.args
        self.assertIn("FROM meta.sync_runs", query)
        self.assertIn("MAX(started_at)", query)
        self.assertEqual(params, ("nexudus", "products", "silver"))

    @patch("shared.azure_clients.silver_sync.get_last_successful_run_started_at")
    @patch("shared.azure_clients.silver_sync.get_sql_client")
    def test_load_latest_bronze_rows_applies_watermark_when_present(
        self,
        mock_get_sql_client,
        mock_get_last_successful_run_started_at,
    ):
        sql = MagicMock()
        rows = [{"id": 1, "raw_json": "{}"}]
        sql.execute_query.return_value = rows
        mock_get_sql_client.return_value = sql

        watermark = datetime(2026, 4, 24, 2, 30, tzinfo=timezone.utc)
        mock_get_last_successful_run_started_at.return_value = watermark

        result = silver_sync.load_latest_bronze_rows(
            "bronze.nexudus_products",
            source_name="nexudus",
            entity="products",
        )

        self.assertEqual(result, rows)
        query, params = sql.execute_query.call_args.args
        self.assertIn("FROM bronze.nexudus_products", query)
        self.assertIn("WHERE b.synced_at >= ?", query)
        self.assertEqual(params, (watermark,))

    @patch("shared.azure_clients.silver_sync.get_last_successful_run_started_at")
    @patch("shared.azure_clients.silver_sync.get_sql_client")
    def test_load_latest_bronze_rows_falls_back_to_full_latest_snapshot_when_no_watermark(
        self,
        mock_get_sql_client,
        mock_get_last_successful_run_started_at,
    ):
        sql = MagicMock()
        sql.execute_query.return_value = []
        mock_get_sql_client.return_value = sql
        mock_get_last_successful_run_started_at.return_value = None

        silver_sync.load_latest_bronze_rows(
            "bronze.nexudus_contracts",
            source_name="nexudus",
            entity="contracts",
        )

        query, params = sql.execute_query.call_args.args
        self.assertIn("FROM bronze.nexudus_contracts", query)
        self.assertNotIn("WHERE b.synced_at >= ?", query)
        self.assertIsNone(params)


if __name__ == "__main__":
    unittest.main()
