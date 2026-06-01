"""
shared/nexudus/transformers/financial_accounts.py

Transforms a raw Nexudus FinancialAccount JSON record into a typed dict for
silver.nexudus_financial_accounts.

Phase 2 of the landlord dashboard rework uses `name` as the filter target
(case-insensitive LIKE '%membership fee%'), so it's marked NOT NULL in
silver and we fall back to ToStringText / Code if Name is missing.
"""
from datetime import datetime
from typing import Optional


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _bit(value) -> int:
    return 1 if value else 0


def _int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _str(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def transform_financial_account(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """One raw Nexudus FinancialAccount record → silver row dict."""
    return {
        # Source
        "source_id":          raw["Id"],
        "unique_id":          _str(raw.get("UniqueId")),
        "bronze_id":          bronze_id,
        "sync_run_id":        sync_run_id,

        # Identity (Phase 2 filter target)
        "name":               (
            _str(raw.get("Name"))
            or _str(raw.get("ToStringText"))
            or _str(raw.get("Code"))
            or ""
        ),
        "code":               _str(raw.get("Code")),
        "description":        _str(raw.get("Description")),
        "location_source_id": _int(raw.get("BusinessId")),

        # Classification
        "account_type":       _str(raw.get("AccountType")) or _str(raw.get("Type")),
        "currency_code":      _str(raw.get("CurrencyCode")),

        # Flags
        "active":             _bit(raw.get("Active", True)),
        "is_deleted":         _bit(raw.get("Deleted") or raw.get("IsDeleted")),

        # Audit
        "updated_by":         _str(raw.get("UpdatedBy")),
        "created_on":         _parse_dt(raw.get("CreatedOn")),
        "updated_on":         _parse_dt(raw.get("UpdatedOn")),
    }
