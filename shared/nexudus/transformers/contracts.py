"""
shared/nexudus/transformers/contracts.py

Transforms raw bronze.nexudus_contracts JSON into a typed dict
for silver.nexudus_contracts.

Fields deliberately excluded (always null, always same value, or derivable):
  - DurationInDays/Weeks/Months, TermDurationInDays/Weeks  → derivable from dates
  - DeskCapacity/Price/Size                               → always 0.0
  - CancelTeamContracts, InvoiceAdvancedCycles, IsNew     → always False
  - TariffInvoiceEvery (always 1), TariffInvoiceEveryWeeks (always 0)
  - TotalPrice, PriceWithProductsAndDeposits              → duplicate of PriceWithProducts
  - All deposit totals, DeliveryHandling*, Proposal*,
    CourseMemberUniqueId, SystemId, etc.                  → always null
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


def _bit(value) -> Optional[int]:
    if value is None:
        return None
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


def transform_contract(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Transform one raw Nexudus CoworkerContract record into a silver row dict."""
    return {
        # Source
        "source_id":                raw["Id"],
        "unique_id":                _str(raw.get("UniqueId")),
        "bronze_id":                bronze_id,
        "sync_run_id":              sync_run_id,

        # Status
        "active":                   1 if raw.get("Active") else 0,
        "cancelled":                1 if raw.get("Cancelled") else 0,
        "main_contract":            1 if raw.get("MainContract") else 0,
        "in_paused_period":         1 if raw.get("InPausedPeriod") else 0,

        # Coworker
        "coworker_id":              raw.get("CoworkerId"),
        "coworker_name":            _str(raw.get("CoworkerFullName")),
        "coworker_email":           _str(raw.get("CoworkerEmail")),
        "coworker_company":         _str(raw.get("CoworkerCompanyName")),
        "coworker_billing_name":    _str(raw.get("CoworkerBillingName")),
        "coworker_type":            _int(raw.get("CoworkerCoworkerType")),
        "coworker_active":          _bit(raw.get("CoworkerActive")),

        # Issuing location (IssuedById = location source_id)
        "location_source_id":       raw.get("IssuedById"),
        "location_name":            _str(raw.get("IssuedByName")),

        # Tariff / plan
        "tariff_id":                raw.get("TariffId"),
        "tariff_name":              _str(raw.get("TariffName")),
        "tariff_price":             _decimal(raw.get("TariffPrice")),
        "currency_code":            _str(raw.get("TariffCurrencyCode")),
        "next_tariff_id":           raw.get("NextTariffId"),
        "next_tariff_name":         _str(raw.get("NextTariffName")),

        # Linked products
        "floor_plan_desk_ids":      _str(raw.get("FloorPlanDeskIds")),
        "floor_plan_desk_names":    _str(raw.get("FloorPlanDeskNames")),

        # Pricing
        "price":                    _decimal(raw.get("Price")),
        "price_with_products":      _decimal(raw.get("PriceWithProducts")),
        "unit_price":               _decimal(raw.get("UnitPrice")),
        "quantity":                 _int(raw.get("Quantity")),
        "billing_day":              _int(raw.get("BillingDay")),

        # Billing flags
        "apply_pro_rating":         _bit(raw.get("ApplyProRating")),
        "pro_rate_cancellation":    _bit(raw.get("ProRateCancellation")),
        "include_signup_fee":       _bit(raw.get("IncludeSignupFee")),
        "cancellation_limit_days":  _int(raw.get("CancellationLimitDays")),

        # Key dates
        "start_date":               _parse_dt(raw.get("StartDate")),
        "contract_term":            _parse_dt(raw.get("ContractTerm")),
        "renewal_date":             _parse_dt(raw.get("RenewalDate")),
        "cancellation_date":        _parse_dt(raw.get("CancellationDate")),
        "invoiced_period":          _parse_dt(raw.get("InvoicedPeriod")),

        # Duration
        "term_duration_months":     _int(raw.get("TermDurationInMonths")),

        # Audit
        "notes":                    _str(raw.get("Notes")),
        "updated_by":               _str(raw.get("UpdatedBy")),

        # Timestamps
        "created_on":               _parse_dt(raw.get("CreatedOn")),
        "updated_on":               _parse_dt(raw.get("UpdatedOn")),
    }
