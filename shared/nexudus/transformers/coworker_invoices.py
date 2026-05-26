"""
Transforms raw bronze.nexudus_coworker_invoices JSON into a typed dict
for silver.nexudus_coworker_invoices.
"""
from __future__ import annotations

import os
from datetime import datetime, tzinfo
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from dateutil import tz


_DEFAULT_INVOICE_TIMEZONE = "Europe/Amsterdam"
_STATUS_KEYS = (
    "Status",
    "InvoiceStatus",
    "PaymentStatus",
    "State",
    "InvoiceState",
)
_PROCESSING_KEYS = (
    "Processing",
    "IsProcessing",
    "PaymentProcessing",
    "PaymentIsProcessing",
    "IsPaymentProcessing",
    "DirectDebitProcessing",
    "DirectDebitPaymentProcessing",
    "PaymentInProgress",
    "InProgress",
)
_AWAITING_PAYMENT_PREFIXES = ("AWAITING", "PENDING", "PROCESSING", "SUBMITTED")
_FAILED_PAYMENT_PREFIXES = (
    "FAILED",
    "FAILURE",
    "ERROR",
    "REJECTED",
    "REFUSED",
    "CANCELLED",
    "CANCELED",
    "CHARGED_BACK",
    "CHARGEBACK",
    "REVERSED",
)
_FAILED_PAYMENT_TOKENS = (
    "INSUFFICIENT",
    "FAILED",
    "FAILURE",
    "ERROR",
    "REJECTED",
    "REFUSED",
    "CANCELLED",
    "CANCELED",
    "CHARGED BACK",
    "CHARGEBACK",
)


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


def _invoice_timezone() -> tzinfo:
    name = os.getenv("NEXUDUS_INVOICE_TIMEZONE", _DEFAULT_INVOICE_TIMEZONE)
    return tz.gettz(name) or tz.gettz(_DEFAULT_INVOICE_TIMEZONE) or tz.UTC


def _parse_invoice_date_dt(value: Any) -> Optional[datetime]:
    """Convert Nexudus UTC invoice date-times into local business date-times.

    Nexudus date fields that represent a local business date can arrive as a
    UTC timestamp on the previous evening. SQL stores DATETIME2 without offset,
    so normalize before writing to silver to preserve the displayed due date.
    """
    dt = _parse_dt(value)
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(_invoice_timezone()).replace(tzinfo=None)


def _history_dt(history: dict) -> datetime:
    return (
        _parse_dt(history.get("CreatedOn"))
        or _parse_dt(history.get("UpdatedOn"))
        or datetime.min
    )


def _payment_result_histories(histories: Optional[list[dict]]) -> list[dict]:
    if not histories:
        return []
    return [
        h for h in histories
        if (_str(h.get("Name")) or "").strip().lower() == "payment result"
    ]


def _history_description(history: dict) -> str:
    return (_str(history.get("Description")) or "").strip()


def _is_awaiting_payment_result(history: dict) -> bool:
    text = _history_description(history).upper()
    return any(text.startswith(f"{prefix}:") or text == prefix for prefix in _AWAITING_PAYMENT_PREFIXES)


def _is_failed_payment_result(history: dict) -> bool:
    text = _history_description(history).upper()
    if _bit(history.get("IsProblem")):
        return True
    return (
        any(text.startswith(f"{prefix}:") or text == prefix for prefix in _FAILED_PAYMENT_PREFIXES)
        or any(token in text for token in _FAILED_PAYMENT_TOKENS)
    )


def _payment_history_summary(histories: Optional[list[dict]]) -> dict[str, Any]:
    # processing=True only while a DD attempt is in flight (latest Payment
    # Result is AWAITING/pending). Any other terminal state — including
    # mandate-revoked failures — leaves the invoice visible to the
    # dashboard so reminders can fire.
    payment_results = sorted(_payment_result_histories(histories), key=_history_dt)
    failures = [h for h in payment_results if _is_failed_payment_result(h)]
    latest = payment_results[-1] if payment_results else None
    failure_count = len(failures)

    if latest and _is_awaiting_payment_result(latest):
        return {
            "invoice_status": "Processing",
            "processing": True,
            "payment_failure_count": failure_count,
        }
    if latest and _is_failed_payment_result(latest):
        return {
            "invoice_status": "Payment Failed",
            "processing": False,
            "payment_failure_count": failure_count,
        }
    return {
        "invoice_status": None,
        "processing": None,
        "payment_failure_count": failure_count if histories is not None else None,
    }


def _invoice_status(raw: dict, histories: Optional[list[dict]] = None) -> Optional[str]:
    history_status = _payment_history_summary(histories).get("invoice_status")
    if history_status:
        return history_status

    for key in _STATUS_KEYS:
        value = _str(raw.get(key))
        if value:
            return value
    if _invoice_processing(raw, histories):
        return "Processing"
    if _bit(raw.get("Paid")):
        return "Paid"
    if _bit(raw.get("Void")):
        return "Void"
    if _bit(raw.get("Draft")):
        return "Draft"
    if _bit(raw.get("IsDue")):
        return "Due"
    return None


def _invoice_processing(raw: dict, histories: Optional[list[dict]] = None) -> Optional[bool]:
    history_processing = _payment_history_summary(histories).get("processing")
    if history_processing is not None:
        return history_processing

    for key in _PROCESSING_KEYS:
        value = _bit(raw.get(key))
        if value is not None:
            return value

    status = next((_str(raw.get(key)) for key in _STATUS_KEYS if _str(raw.get(key))), None)
    if status and "processing" in status.strip().lower():
        return True
    return None


def transform_coworker_invoice(
    raw: dict,
    bronze_id: int,
    sync_run_id: str,
    histories: Optional[list[dict]] = None,
) -> dict:
    history_summary = _payment_history_summary(histories)
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
        "invoice_status": _invoice_status(raw, histories),
        "processing": _invoice_processing(raw, histories),
        "payment_failure_count": history_summary.get("payment_failure_count"),
        "due_date": _parse_invoice_date_dt(raw.get("DueDate")),
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
