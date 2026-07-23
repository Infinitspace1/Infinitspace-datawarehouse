"""Tests for the LoopNet broker directory (pure logic — no SQL required)."""
from __future__ import annotations

from shared.location_scraper.broker_directory import (
    extract_broker_records,
    normalize_broker_name,
    resolve_email,
)
from shared.location_scraper.activities.materialize_globe import _loopnet_broker_contacts


# --- normalize_broker_name ---

def test_normalize_basic():
    assert normalize_broker_name("  John   SMITH ") == "john smith"


def test_normalize_empty_and_none():
    assert normalize_broker_name(None) is None
    assert normalize_broker_name("") is None
    assert normalize_broker_name("   ") is None


# --- extract_broker_records ---

def test_extract_flat_fields():
    payload = {
        "brokerName": "Jane Broker",
        "brokerEmail": "Jane@Savills.com",
        "brokerCompany": "Savills",
        "brokerPhone": "020 7000 0000",
    }
    records = extract_broker_records(payload)
    assert len(records) == 1
    rec = records[0]
    assert rec["name_normalized"] == "jane broker"
    assert rec["email"] == "jane@savills.com"  # lowercased
    assert rec["company"] == "Savills"
    assert rec["phone"] == "020 7000 0000"


def test_extract_brokers_list_and_dedup():
    payload = {
        "brokerName": "Jane Broker",
        "brokerEmail": "jane@savills.com",
        "brokers": [
            {"name": "Jane Broker", "email": "jane@savills.com"},  # duplicate of flat
            {"name": "Jim Allison", "email": "jim@jll.com"},
            {"name": "No Email"},  # not directory-worthy
        ],
    }
    records = extract_broker_records(payload)
    keys = {(r["name_normalized"], r["email"]) for r in records}
    assert keys == {("jane broker", "jane@savills.com"), ("jim allison", "jim@jll.com")}


def test_extract_requires_name_and_valid_email():
    assert extract_broker_records({"brokerEmail": "x@y.com"}) == []
    assert extract_broker_records({"brokerName": "Jane"}) == []
    assert extract_broker_records({"brokerName": "Jane", "brokerEmail": "not-an-email"}) == []


# --- resolve_email ---

_DIRECTORY = {
    "john bundy": [
        {"name_normalized": "john bundy", "email": "jbundy@d2000.com", "company": "Don Quick & Associates"},
    ],
    "john smith": [
        {"name_normalized": "john smith", "email": "js@cbre.com", "company": "CBRE"},
        {"name_normalized": "john smith", "email": "john@jll.com", "company": "JLL"},
    ],
}


def test_resolve_single_email():
    hit = resolve_email(_DIRECTORY, "John Bundy")
    assert hit is not None
    assert hit["email"] == "jbundy@d2000.com"


def test_resolve_unknown_name():
    assert resolve_email(_DIRECTORY, "Nobody Here") is None
    assert resolve_email(_DIRECTORY, None) is None


def test_resolve_ambiguous_without_company_returns_none():
    assert resolve_email(_DIRECTORY, "John Smith") is None


def test_resolve_ambiguous_with_company_tiebreak():
    hit = resolve_email(_DIRECTORY, "John Smith", "CBRE")
    assert hit is not None
    assert hit["email"] == "js@cbre.com"


def test_resolve_ambiguous_with_unknown_company_returns_none():
    assert resolve_email(_DIRECTORY, "John Smith", "Colliers") is None


# --- _loopnet_broker_contacts with directory fallback ---

def test_contacts_payload_email_wins_no_directory_needed():
    payload = {
        "brokerName": "Jane Broker",
        "brokerCompany": "Savills",
        "brokerEmail": "jane@savills.com",
    }
    contacts = _loopnet_broker_contacts(payload)
    assert contacts[0]["email"] == "jane@savills.com"
    assert contacts[0]["name"] == "Jane Broker"
    assert contacts[0]["title"] == "Broker — Savills"


def test_contacts_cobroker_email_from_brokers_list():
    payload = {
        "brokerName": "Jane Broker",
        "brokerEmail": "jane@savills.com",
        "brokers": [
            {"name": "Jane Broker", "email": "jane@savills.com"},
            {"name": "Jim Allison", "email": "jim@jll.com"},
        ],
    }
    contacts = _loopnet_broker_contacts(payload)
    emails = [c["email"] for c in contacts]
    assert emails == ["jane@savills.com", "jim@jll.com"]
    assert contacts[1]["name"] == "Jim Allison"


def test_contacts_directory_fills_primary_missing_email():
    payload = {"brokerName": "John Bundy", "brokerCompany": "Don Quick & Associates"}
    contacts = _loopnet_broker_contacts(payload, _DIRECTORY)
    assert len(contacts) == 1
    assert contacts[0]["email"] == "jbundy@d2000.com"
    assert contacts[0]["name"] == "John Bundy"
    assert "(directory)" in contacts[0]["title"]


def test_contacts_directory_fills_cobrokers():
    payload = {
        "brokerName": "Jane Broker",
        "brokerEmail": "jane@savills.com",
        "brokers": [
            {"name": "Jane Broker", "email": "jane@savills.com"},
            {"name": "John Bundy"},  # email-less co-broker, known in directory
        ],
    }
    contacts = _loopnet_broker_contacts(payload, _DIRECTORY)
    emails = [c["email"] for c in contacts]
    assert emails == ["jane@savills.com", "jbundy@d2000.com"]


def test_contacts_ambiguous_directory_name_not_guessed():
    payload = {"brokerName": "John Smith"}  # 2 emails in directory, no company
    assert _loopnet_broker_contacts(payload, _DIRECTORY) == []


def test_contacts_cap_at_three():
    payload = {
        "brokerName": "A",
        "brokerEmail": "a@x.com",
        "brokerEmails": ["a@x.com", "b@x.com", "c@x.com", "d@x.com"],
    }
    contacts = _loopnet_broker_contacts(payload)
    assert len(contacts) == 3


def test_contacts_empty_payload_no_directory():
    assert _loopnet_broker_contacts({}) == []
    assert _loopnet_broker_contacts({"brokerName": "Ghost"}, {}) == []
