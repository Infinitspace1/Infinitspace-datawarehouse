"""
shared/nexudus/transformers/tariffs.py

Transforms a raw Nexudus Tariff JSON record into a typed dict for
silver.nexudus_tariffs.

The raw shape is best-effort based on the surrounding Nexudus API
conventions (camel-case keys mirroring the SQL columns; CreatedOn/UpdatedOn
as ISO strings; nullable price fields). If a field is named differently in
the actual response, this transformer will return None for it and the
silver row will store NULL — surface those as data-quality issues rather
than crashing the sync.
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


def _decimal(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _str(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def transform_tariff(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """One raw Nexudus Tariff record → silver row dict."""
    return {
        # Source
        "source_id":              raw["Id"],
        "unique_id":              _str(raw.get("UniqueId")),
        "bronze_id":              bronze_id,
        "sync_run_id":            sync_run_id,

        # Identity
        "name":                   _str(raw.get("Name")) or _str(raw.get("ToStringText")) or "",
        "description":            _str(raw.get("Description")),
        "location_source_id":     _int(raw.get("BusinessId")),

        # Pricing
        "price":                  _decimal(raw.get("Price")),
        "currency_code":          _str(raw.get("CurrencyCode")),
        "signup_fee":             _decimal(raw.get("SignupFee")),
        "deposit":                _decimal(raw.get("Deposit")),
        "included_credit_amount": _decimal(raw.get("IncludedCreditAmount")),
        "time_credit_minutes":    _int(raw.get("TimeCredit")),

        # Billing cadence
        "charge_period":          _int(raw.get("ChargePeriod")),
        "billing_day":            _int(raw.get("BillingDay")),
        "term_duration_months":   _int(raw.get("TermDurationInMonths"))
                                  or _int(raw.get("TermDuration")),
        "notice_period_days":     _int(raw.get("NoticePeriodInDays"))
                                  or _int(raw.get("CancellationLimitDays")),

        # Financial account link — the key join for Phase 2's revenue filter
        "financial_account_id":   _int(raw.get("FinancialAccountId")),

        # Flags
        "active":                 _bit(raw.get("Active", True)),
        "visible":                _bit(raw.get("Visible")),
        "is_team_plan":           _bit(raw.get("IsTeamPlan")),
        "is_default":             _bit(raw.get("IsDefault")),
        "apply_pro_rating":       _bit(raw.get("ApplyProRating")),
        "pro_rate_cancellation":  _bit(raw.get("ProRateCancellation")),
        "is_deleted":             _bit(raw.get("Deleted") or raw.get("IsDeleted")),

        # Audit
        "updated_by":             _str(raw.get("UpdatedBy")),
        "created_on":             _parse_dt(raw.get("CreatedOn")),
        "updated_on":             _parse_dt(raw.get("UpdatedOn")),
    }
