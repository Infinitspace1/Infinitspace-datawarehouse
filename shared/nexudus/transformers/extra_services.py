"""
shared/nexudus/transformers/extra_services.py

Transforms raw bronze.nexudus_extra_services JSON into a typed dict
for silver.nexudus_extra_services.

Fields deliberately excluded (always null, always same value, or low value):
  - AddedResourceTypes, AddedTariffs, AddedTeams       → always null
  - RemovedResourceTypes, RemovedTariffs, RemovedTeams → always null
  - AppliedDiscountAmount, AppliedDiscountCode          → always null
  - AvailableCredit, UsedCredit, UsedTimeCredit         → always null
  - CoworkerDiscountUniqueId                            → always null
  - CustomFields, LocalizationDetails                   → always null
  - Demand, DynamicPriceAdjustment                      → always null
  - FromTime, ToTime, TimeSlots                         → always null
  - InvoiceLineDisplayAs                                → always null
  - LastMinutePeriodMinutes, LastMinutePriceAdjustment  → always null
  - PriceFactor* (5 fields)                             → always null
  - ResourceTypes, Tariffs, Teams                       → always null (ResourceTypeNames kept)
  - SystemId                                            → mostly null, internal
  - DisplayOrder                                        → always 0
  - IsBookingCredit                                     → always False
  - IsNew                                               → always False
  - OnlyWithinAvailableTimes                            → always False
  - Visible                                             → always False
  - ToStringText                                        → duplicate of Name
  - CurrencyId                                          → CurrencyCode is more useful
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


def transform_extra_service(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Transform one raw Nexudus ExtraService record into a silver row dict."""
    return {
        # Source
        "source_id":                    raw["Id"],
        "unique_id":                    _str(raw.get("UniqueId")),
        "bronze_id":                    bronze_id,
        "sync_run_id":                  sync_run_id,

        # Location
        "location_source_id":           raw["BusinessId"],

        # Identity
        "name":                         _str(raw.get("Name")) or _str(raw.get("ToStringText")) or "",
        "description":                  _str(raw.get("Description")),

        # Pricing
        "price":                        _decimal(raw.get("Price")) or 0.0,
        "currency_code":                _str(raw.get("CurrencyCode")),
        "charge_period":                _int(raw.get("ChargePeriod")),
        "credit_price":                 _decimal(raw.get("CreditPrice")),
        "fixed_cost_price":             _decimal(raw.get("FixedCostPrice")),
        "fixed_cost_length_minutes":    _int(raw.get("FixedCostLength")),
        "maximum_price":                _decimal(raw.get("MaximumPrice")),
        "min_length_minutes":           _int(raw.get("MinLength")),
        "max_length_minutes":           _int(raw.get("MaxLength")),

        # Flags
        "is_default_price":             _bit(raw.get("IsDefaultPrice")),
        "is_printing_credit":           _bit(raw.get("IsPrintingCredit")),
        "only_for_contacts":            _bit(raw.get("OnlyForContacts")),
        "only_for_members":             _bit(raw.get("OnlyForMembers")),
        "apply_charge_to_visitors":     _bit(raw.get("ApplyChargeToVisitors")),
        "use_per_night_pricing":        _bit(raw.get("UsePerNightPricing")),

        # Dynamic pricing
        "last_minute_adjustment_type":  _int(raw.get("LastMinuteAdjustmentType")),

        # Availability window
        "apply_from":                   _parse_dt(raw.get("ApplyFrom")),
        "apply_to":                     _parse_dt(raw.get("ApplyTo")),

        # Resource type link (soft → silver.nexudus_products.resource_type_name)
        "resource_type_names":          _str(raw.get("ResourceTypeNames")),

        # Financial
        "tax_rate_id":                  _int(raw.get("TaxRateId")),
        "reduced_tax_rate_id":          _int(raw.get("ReducedTaxRateId")),
        "exempt_tax_rate_id":           _int(raw.get("ExemptTaxRateId")),
        "financial_account_id":         _int(raw.get("FinancialAccountId")),

        # Audit
        "updated_by":                   _str(raw.get("UpdatedBy")),

        # Timestamps
        "created_on":                   _parse_dt(raw.get("CreatedOn")),
        "updated_on":                   _parse_dt(raw.get("UpdatedOn")),
    }
