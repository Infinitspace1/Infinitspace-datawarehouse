"""Tests for the Lusha client's person lookup (GET /v2/person, header auth)."""
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.location_scraper.clients import lusha as L


class _FakeResp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            err = requests.HTTPError(f"{self.status_code}")
            err.response = self
            raise err

    def json(self):
        return self._payload


_PERSON_ENVELOPE = {
    "contact": {
        "error": None,
        "isCreditCharged": True,
        "data": {
            "fullName": "Lizzie Boswell",
            "jobTitle": {"title": "Associate Director"},
            "emailAddresses": [
                {"email": "lizzie.boswell@cbre.com", "emailType": "work", "emailConfidence": "A+"}
            ],
        },
    }
}


def test_search_individual_uses_get_v2_person_with_header_auth(monkeypatch):
    captured = {}

    def fake_get(url, params=None, headers=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        captured["headers"] = headers
        return _FakeResp(200, _PERSON_ENVELOPE)

    monkeypatch.setattr(L.requests, "get", fake_get)
    person = L.search_individual("Lizzie", "Boswell", "CBRE")

    assert captured["url"] == "https://api.lusha.com/v2/person"
    assert captured["params"] == {"firstName": "Lizzie", "lastName": "Boswell", "companyName": "CBRE"}
    # Auth must be the header, not a query param (Lusha 401s on ?api_key= for GET).
    assert "api_key" in captured["headers"]
    assert "api_key" not in captured["params"]

    assert person["fullName"] == "Lizzie Boswell"
    email, conf = L.extract_best_email(person)
    assert email == "lizzie.boswell@cbre.com"
    assert conf == "A+"


def test_search_individual_none_on_404(monkeypatch):
    monkeypatch.setattr(L.requests, "get", lambda *a, **k: _FakeResp(404, {}))
    assert L.search_individual("No", "Body", "Nowhere Ltd") is None


def test_search_individual_none_when_data_empty(monkeypatch):
    envelope = {"contact": {"error": None, "data": None}}
    monkeypatch.setattr(L.requests, "get", lambda *a, **k: _FakeResp(200, envelope))
    assert L.search_individual("No", "Body", "Nowhere Ltd") is None
