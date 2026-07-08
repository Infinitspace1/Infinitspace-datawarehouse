"""
Transforms raw bronze.nexudus_coworkers JSON into a typed dict
for silver.nexudus_coworkers.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional


def _str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


# The internal "HubSpot Deal URL" custom field lives in the coworker payload's
# CustomFields.Data array as a {Name, Value} entry keyed by the field's Nexudus
# id (see the field's API panel: record.CustomFields.Data[f.Name=='1414370977'].
# Value). We match that id primarily, and fall back to any entry whose value
# looks like a HubSpot deal URL (so a recreated field with a new id still works).
_HUBSPOT_DEAL_FIELD_ID = "1414370977"
_HUBSPOT_DEAL_URL_RE = re.compile(r"(?:record/0-3/|/deals?/)(\d{4,})")


def _extract_hubspot_deal_url(raw: dict) -> Optional[str]:
    data = (raw.get("CustomFields") or {}).get("Data")
    if not isinstance(data, list):
        return None
    by_id = fallback = None
    for item in data:
        if not isinstance(item, dict):
            continue
        value = _str(item.get("Value"))
        if not value:
            continue
        if _HUBSPOT_DEAL_FIELD_ID in str(item.get("Name") or ""):
            by_id = value
        elif "hubspot" in value.lower() and _HUBSPOT_DEAL_URL_RE.search(value):
            fallback = value
    return by_id or fallback


def _int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bit(value: Any) -> Optional[bool]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("true", "1", "yes"):
        return True
    if text in ("false", "0", "no"):
        return False
    return None


def _parse_dt(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def transform_coworker(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    businesses = raw.get("Businesses")
    teams = raw.get("Teams")

    return {
        "source_id": _int(raw.get("Id")),
        "unique_id": _str(raw.get("UniqueId")),
        "bronze_id": bronze_id,
        "sync_run_id": sync_run_id,
        "coworker_type": _int(raw.get("CoworkerType")),
        "full_name": _str(raw.get("FullName")),
        "email": _str(raw.get("Email")),
        "billing_email": _str(raw.get("BillingEmail")),
        "billing_name": _str(raw.get("BillingName")),
        "company_name": _str(raw.get("CompanyName")),
        "team_name": _str(raw.get("TeamName")),
        "team_names": _str(raw.get("TeamNames")),
        "team_ids": _str(raw.get("TeamIds")) or (
            ",".join(str(team_id) for team_id in teams) if isinstance(teams, list) else None
        ),
        "business_ids": ",".join(str(business_id) for business_id in businesses)
        if isinstance(businesses, list) and businesses
        else None,
        "location_source_id": _int(raw.get("InvoicingBusinessId")),
        "location_name": _str(raw.get("InvoicingBusinessName")),
        "mobile_phone": _str(raw.get("MobilePhone")),
        "land_line": _str(raw.get("LandLine")),
        "address": _str(raw.get("Address")),
        "post_code": _str(raw.get("PostCode")),
        "city_name": _str(raw.get("CityName")),
        "state": _str(raw.get("State")),
        "billing_address": _str(raw.get("BillingAddress")),
        "billing_post_code": _str(raw.get("BillingPostCode")),
        "billing_city_name": _str(raw.get("BillingCityName")),
        "billing_state": _str(raw.get("BillingState")),
        "tax_id_number": _str(raw.get("TaxIDNumber")),
        "billing_day": _int(raw.get("BillingDay")),
        "tariff_id": _int(raw.get("TariffId")),
        "tariff_name": _str(raw.get("TariffName")),
        "next_tariff_id": _int(raw.get("NextTariffId")),
        "next_tariff_name": _str(raw.get("NextTariffName")),
        "coworker_contract_ids": _str(raw.get("CoworkerContractIds")),
        "coworker_contract_tariff_names": _str(raw.get("CoworkerContractTariffNames")),
        "active": _bit(raw.get("Active")),
        "archived": _bit(raw.get("Archived")),
        "user_active": _bit(raw.get("UserActive")),
        "notify_on_new_invoice": _bit(raw.get("NotifyOnNewInvoice")),
        "notify_on_new_payment": _bit(raw.get("NotifyOnNewPayment")),
        "notify_on_failed_payment": _bit(raw.get("NotifyOnFailedPayment")),
        "do_not_process_invoices_automatically": _bit(raw.get("DoNotProcessInvoicesAutomatically")),
        "user_last_access": _parse_dt(raw.get("UserLastAccess")),
        "registration_date": _parse_dt(raw.get("RegistrationDate")),
        "renewal_date": _parse_dt(raw.get("RenewalDate")),
        "start_date": _parse_dt(raw.get("StartDate")),
        "cancellation_date": _parse_dt(raw.get("CancellationDate")),
        "created_on": _parse_dt(raw.get("CreatedOn")),
        "updated_on": _parse_dt(raw.get("UpdatedOn")),
        "hubspot_deal_url": _extract_hubspot_deal_url(raw),
    }
