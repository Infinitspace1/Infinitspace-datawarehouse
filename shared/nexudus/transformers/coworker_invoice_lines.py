"""
Transforms raw bronze.nexudus_coworker_invoice_lines JSON into a typed dict
for silver.nexudus_coworker_invoice_lines.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


def _str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dec(value: Any, scale: str = "0.01") -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal(scale))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _dec4(value: Any) -> Optional[Decimal]:
    return _dec(value, scale="0.0001")


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


def transform_coworker_invoice_line(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    return {
        "source_id": _int(raw.get("Id")),
        "unique_id": _str(raw.get("UniqueId")),
        "bronze_id": bronze_id,
        "sync_run_id": sync_run_id,
        "invoice_source_id": _int(raw.get("CoworkerInvoiceId")),
        "coworker_id": _int(raw.get("CoworkerId")),
        "location_source_id": _int(raw.get("BusinessId")),
        "description": _str(raw.get("Description")),
        "financial_account_code": _str(raw.get("FinancialAccountCode")),
        "financial_account_name": _str(raw.get("FinancialAccountName")),
        "quantity": _dec4(raw.get("Quantity")),
        "unit_price": _dec(raw.get("UnitPrice")),
        "sub_total": _dec(raw.get("SubTotal")),
        "tax_amount": _dec(raw.get("TaxAmount")),
        "total_amount": _dec(raw.get("TotalAmount")),
        "currency_code": _str(raw.get("CurrencyCode")),
        "tax_category_name": _str(raw.get("TaxCategoryName")),
        "product_name": _str(raw.get("ProductName")),
        "created_on": _parse_dt(raw.get("CreatedOn")),
        "updated_on": _parse_dt(raw.get("UpdatedOn")),
    }
