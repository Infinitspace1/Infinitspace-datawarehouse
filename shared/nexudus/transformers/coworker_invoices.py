"""
Transforms raw bronze.nexudus_coworker_invoices JSON into a typed dict
for silver.nexudus_coworker_invoices.
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


def _dec(value: Any, scale: str = "0.01") -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal(scale))
    except (InvalidOperation, TypeError, ValueError):
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


def transform_coworker_invoice(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    return {
        "source_id": _int(raw.get("Id")),
        "unique_id": _str(raw.get("UniqueId")),
        "bronze_id": bronze_id,
        "sync_run_id": sync_run_id,
        "coworker_id": _int(raw.get("CoworkerId")),
        "coworker_name": _str(raw.get("CoworkerFullName")),
        "coworker_billing_email": _str(raw.get("CoworkerBillingEmail")),
        "coworker_company_name": _str(raw.get("CoworkerCompanyName")),
        "coworker_team_names": _str(raw.get("CoworkerTeamNames")),
        "location_source_id": _int(raw.get("BusinessId")),
        "location_name": _str(raw.get("BusinessName")),
        "invoice_number": _str(raw.get("InvoiceNumber")),
        "payment_reference": _str(raw.get("PaymentReference")),
        "bill_to_name": _str(raw.get("BillToName")),
        "bill_to_address": _str(raw.get("BillToAddress")),
        "bill_to_city": _str(raw.get("BillToCity")),
        "bill_to_post_code": _str(raw.get("BillToPostCode")),
        "bill_to_state": _str(raw.get("BillToState")),
        "bill_to_country_name": _str(raw.get("BillToCountryName")),
        "bill_to_tax_id_number": _str(raw.get("BillToTaxIDNumber")),
        "description": _str(raw.get("Description")),
        "currency_code": _str(raw.get("CurrencyCode")),
        "due_date": _parse_dt(raw.get("DueDate")),
        "invoice_from_date": _parse_dt(raw.get("InvoiceFromDate")),
        "invoice_to_date": _parse_dt(raw.get("InvoiceToDate")),
        "sent_on": _parse_dt(raw.get("SentOn")),
        "paid_on": _parse_dt(raw.get("PaidOn")),
        "refunded_on": _parse_dt(raw.get("RefundedOn")),
        "total_amount": _dec(raw.get("TotalAmount")),
        "paid_amount": _dec(raw.get("PaidAmount")),
        "due_amount": _dec(raw.get("DueAmount")),
        "received_amount": _dec(raw.get("ReceivedAmount")),
        "credited_amount": _dec(raw.get("CreditedAmount")),
        "refunded_amount": _dec(raw.get("RefundedAmount")),
        "tax_amount": _dec(raw.get("TaxAmount")),
        "draft": _bit(raw.get("Draft")),
        "void": _bit(raw.get("Void")),
        "paid": _bit(raw.get("Paid")),
        "sent": _bit(raw.get("Sent")),
        "refunded": _bit(raw.get("Refunded")),
        "credit_note": _bit(raw.get("CreditNote")),
        "is_due": _bit(raw.get("IsDue")),
        "xero_invoice_transferred": _bit(raw.get("XeroInvoiceTransfered")),
        "xero_payment_transferred": _bit(raw.get("XeroPaymentTransfered")),
        "created_on": _parse_dt(raw.get("CreatedOn")),
        "updated_on": _parse_dt(raw.get("UpdatedOn")),
    }
