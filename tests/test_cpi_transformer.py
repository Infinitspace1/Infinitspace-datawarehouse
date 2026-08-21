"""
tests/test_cpi_transformer.py

Pure unit tests for the CPI transform and the client's period/status parsing.
No network, no database - the live end-to-end check lives in
scripts/python_scripts/test_cpi_sync.py.

Run:  .\\venv\\Scripts\\python.exe -m unittest tests.test_cpi_transformer
"""
import unittest
from datetime import date

from shared.cpi.client import _cbs_status, cutoff_period
from shared.cpi.transformers.series import transform_observation

RAW = {
    "source_id": "ons:UK:2026-07",
    "provider": "ons",
    "geo": "UK",
    "index_code": "CPI",
    "index_name": "CPI all items (ONS, UK)",
    "base_year": "2015",
    "period": "2026-07",
    "index_level": 142.9,
    "annual_rate_pct": 2.9,
    "status": "definitive",
    "source_url": "https://www.ons.gov.uk/x/timeseries/d7g7/mm23/data",
    "published_at": "2026-08-18T23:00:00.000Z",
}


class TransformTests(unittest.TestCase):
    def test_maps_every_field(self):
        out = transform_observation(RAW, bronze_id=42, sync_run_id="run-1")
        self.assertEqual(out["source_id"], "ons:UK:2026-07")
        self.assertEqual(out["bronze_id"], 42)
        self.assertEqual(out["sync_run_id"], "run-1")
        self.assertEqual(out["provider"], "ons")
        self.assertEqual(out["geo"], "UK")
        self.assertEqual(out["index_code"], "CPI")
        self.assertEqual(out["base_year"], "2015")
        self.assertEqual(out["period"], "2026-07")
        self.assertEqual(out["index_level"], 142.9)
        self.assertEqual(out["annual_rate_pct"], 2.9)
        self.assertEqual(out["status"], "definitive")

    def test_germany_keeps_its_hicp_label(self):
        # A German agreement names the Destatis VPI. This is the Eurostat HICP,
        # a different index, and index_code must keep saying so.
        out = transform_observation({**RAW, "source_id": "eurostat:DE:2026-07",
                                     "provider": "eurostat", "geo": "DE",
                                     "index_code": "HICP"}, 1, "r")
        self.assertEqual(out["index_code"], "HICP")

    def test_a_level_without_a_rate_is_kept(self):
        out = transform_observation({**RAW, "annual_rate_pct": None}, 1, "r")
        self.assertEqual(out["index_level"], 142.9)
        self.assertIsNone(out["annual_rate_pct"])

    def test_a_rate_without_a_level_is_kept(self):
        out = transform_observation({**RAW, "index_level": None}, 1, "r")
        self.assertIsNone(out["index_level"])
        self.assertEqual(out["annual_rate_pct"], 2.9)

    def test_an_observation_with_neither_figure_is_rejected(self):
        # Not a zero - an empty observation. The silver writer counts it as a
        # per-record error rather than writing 0.0 into a money calculation.
        with self.assertRaises(ValueError):
            transform_observation({**RAW, "index_level": None, "annual_rate_pct": None}, 1, "r")

    def test_a_missing_source_id_is_rejected(self):
        with self.assertRaises(ValueError):
            transform_observation({**RAW, "source_id": ""}, 1, "r")

    def test_a_malformed_period_is_rejected(self):
        for bad in ("2026-7", "2026/07", "202607", ""):
            with self.assertRaises(ValueError):
                transform_observation({**RAW, "period": bad}, 1, "r")

    def test_an_unknown_status_is_rejected(self):
        with self.assertRaises(ValueError):
            transform_observation({**RAW, "status": "flash"}, 1, "r")

    def test_blank_optional_text_becomes_null_not_empty_string(self):
        out = transform_observation({**RAW, "index_name": "  ", "source_url": "",
                                     "published_at": None}, 1, "r")
        self.assertIsNone(out["index_name"])
        self.assertIsNone(out["source_url"])
        self.assertIsNone(out["published_at"])


class CbsStatusTests(unittest.TestCase):
    def test_the_plain_none_marker_is_definitive(self):
        self.assertEqual(_cbs_status("None"), "definitive")
        self.assertEqual(_cbs_status(""), "definitive")

    def test_anything_else_is_provisional(self):
        # CBS states its provisional figures are "niet geschikt om te gebruiken
        # voor indexering", so anything carrying an attribute is held back.
        for marker in ("Voorlopig", "Nader voorlopig", "voorlopige cijfers"):
            self.assertEqual(_cbs_status(marker), "provisional")


class WindowTests(unittest.TestCase):
    def test_the_window_is_inclusive_of_the_current_month(self):
        self.assertEqual(cutoff_period(1, date(2026, 8, 20)), "2026-08")
        self.assertEqual(cutoff_period(2, date(2026, 8, 20)), "2026-07")

    def test_the_window_crosses_the_year_boundary(self):
        self.assertEqual(cutoff_period(3, date(2026, 1, 15)), "2025-11")
        self.assertEqual(cutoff_period(18, date(2026, 8, 20)), "2025-03")


if __name__ == "__main__":
    unittest.main()
