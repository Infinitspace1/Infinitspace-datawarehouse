"""
Deterministic linking helpers for Xero overdue invoices and Nexudus
coworker invoice/customer records.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


def _norm(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _coalesce_email(coworker: Optional[dict], nexudus_invoice: Optional[dict]) -> Optional[str]:
    if coworker:
        for key in ("billing_email", "email"):
            value = coworker.get(key)
            if value:
                return value
    if nexudus_invoice:
        return nexudus_invoice.get("coworker_billing_email")
    return None


@dataclass
class LinkResult:
    xero_invoice_number: Optional[str]
    matched: bool
    match_reason: str
    nexudus_invoice_id: Optional[int]
    nexudus_coworker_id: Optional[int]
    recipient_email: Optional[str]
    recipient_name: Optional[str]
    recipient_billing_name: Optional[str]


class XeroNexudusOverdueLinker:
    def link_invoices(
        self,
        xero_invoices: list[dict],
        nexudus_invoices: list[dict],
        coworkers: list[dict],
    ) -> list[dict]:
        coworkers_by_id = {row.get("source_id"): row for row in coworkers}
        results = []
        for xero_invoice in xero_invoices:
            link = self._link_single(xero_invoice, nexudus_invoices, coworkers_by_id)
            results.append({
                **xero_invoice,
                "link_matched": link.matched,
                "link_match_reason": link.match_reason,
                "nexudus_invoice_id": link.nexudus_invoice_id,
                "nexudus_coworker_id": link.nexudus_coworker_id,
                "recipient_email": link.recipient_email,
                "recipient_name": link.recipient_name,
                "recipient_billing_name": link.recipient_billing_name,
            })
        return results

    def _link_single(
        self,
        xero_invoice: dict,
        nexudus_invoices: list[dict],
        coworkers_by_id: dict[int, dict],
    ) -> LinkResult:
        invoice_number = _norm(xero_invoice.get("invoice_number"))
        reference = _norm(xero_invoice.get("reference"))
        location_source_id = xero_invoice.get("location_source_id")

        candidates = []
        for nexudus_invoice in nexudus_invoices:
            candidate_invoice_number = _norm(nexudus_invoice.get("invoice_number"))
            payment_reference = _norm(nexudus_invoice.get("payment_reference"))

            match_reason = None
            if invoice_number and candidate_invoice_number == invoice_number:
                match_reason = "invoice_number"
            elif invoice_number and payment_reference == invoice_number:
                match_reason = "payment_reference"
            elif reference and candidate_invoice_number == reference:
                match_reason = "xero_reference"
            elif reference and payment_reference == reference:
                match_reason = "xero_reference_payment_reference"

            if match_reason:
                candidates.append((match_reason, nexudus_invoice))

        if not candidates:
            return LinkResult(
                xero_invoice_number=xero_invoice.get("invoice_number"),
                matched=False,
                match_reason="unmatched",
                nexudus_invoice_id=None,
                nexudus_coworker_id=None,
                recipient_email=None,
                recipient_name=None,
                recipient_billing_name=None,
            )

        def candidate_rank(item: tuple[str, dict]) -> tuple[int, int]:
            reason, row = item
            same_location = row.get("location_source_id") == location_source_id
            reason_rank = {
                "invoice_number": 0,
                "payment_reference": 1,
                "xero_reference": 2,
                "xero_reference_payment_reference": 3,
            }.get(reason, 9)
            return (0 if same_location else 1, reason_rank)

        match_reason, best_invoice = sorted(candidates, key=candidate_rank)[0]
        coworker = coworkers_by_id.get(best_invoice.get("coworker_id"))

        return LinkResult(
            xero_invoice_number=xero_invoice.get("invoice_number"),
            matched=True,
            match_reason=match_reason,
            nexudus_invoice_id=best_invoice.get("source_id"),
            nexudus_coworker_id=best_invoice.get("coworker_id"),
            recipient_email=_coalesce_email(coworker, best_invoice),
            recipient_name=(coworker or {}).get("full_name") or best_invoice.get("coworker_name"),
            recipient_billing_name=(coworker or {}).get("billing_name") or best_invoice.get("bill_to_name"),
        )
