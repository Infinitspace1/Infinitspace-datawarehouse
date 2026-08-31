"""Tests for the LoopNet broker email tail-enrichment (Lusha -> directory)."""
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.location_scraper.activities import enrich as enrich_act


def test_enrich_loopnet_brokers_noop_without_api_key(monkeypatch):
    """No LUSHA_API_KEY -> return immediately without hitting Lusha.

    setenv("") not delenv(): shared.azure_clients.sql_client calls load_dotenv()
    at import, which would otherwise repopulate the real key from .env mid-test.
    An empty value is falsy, so the guard still trips.
    """
    monkeypatch.setenv("LUSHA_API_KEY", "")

    def _boom(*_a, **_k):
        raise AssertionError("Lusha must not be called when the key is absent")

    monkeypatch.setattr(enrich_act.lusha_client, "search_individual", _boom)

    result = enrich_act.enrich_loopnet_brokers(
        {"run_id": "weekly-london-2026-W36", "source": "loopnet", "city": "london"}
    )
    assert result["skipped_no_key"] is True
    assert result["looked_up"] == 0
    assert result["written"] == 0


def test_lookup_cap_default_and_override(monkeypatch):
    monkeypatch.delenv("LOOPNET_LUSHA_MAX_LOOKUPS", raising=False)
    assert enrich_act._loopnet_lookup_cap() == 150
    monkeypatch.setenv("LOOPNET_LUSHA_MAX_LOOKUPS", "25")
    assert enrich_act._loopnet_lookup_cap() == 25
    monkeypatch.setenv("LOOPNET_LUSHA_MAX_LOOKUPS", "0")  # invalid -> default
    assert enrich_act._loopnet_lookup_cap() == 150
