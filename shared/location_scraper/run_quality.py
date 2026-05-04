"""
Aggregated contact / extract quality metrics for bronze.location_scraper_run_quality.

Pure functions — safe to call from the Durable orchestrator (deterministic replay).
"""
from __future__ import annotations

import json
from typing import Any


def compute_run_quality_payload(
    *,
    raw_item_count: int,
    listings: list[dict],
    bundles: list[dict],
    enrichment_diag: dict[str, Any],
) -> dict[str, Any]:
    """
    Returns flat fields to merge into stats / persist in bronze.location_scraper_run_quality.

    listings: normalized Listing.to_dict() rows (post ls_normalize).
    bundles: ContactBundle.to_dict() rows (post consolidate_contacts).
    """
    normalized_count = len(listings)

    with_coords = 0
    with_phone = 0
    with_name = 0

    for L in listings:
        if L.get("latitude") is not None and L.get("longitude") is not None:
            with_coords += 1
        phone = (L.get("phone") or "").strip()
        if phone:
            with_phone += 1
        cn = (L.get("contact_name") or "").strip()
        co = (L.get("company_name") or "").strip()
        if cn or co:
            with_name += 1

    lusha_slots = 0
    for b in bundles:
        for key in ("email_1", "email_2", "email_3"):
            if (b.get(key) or "").strip():
                lusha_slots += 1

    return {
        "raw_item_count": raw_item_count,
        "normalized_count": normalized_count,
        "with_coords_count": with_coords,
        "with_phone_count": with_phone,
        "with_name_or_company_count": with_name,
        "lusha_email_slots": lusha_slots,
        "enrichment_diagnostics_json": json.dumps(enrichment_diag, ensure_ascii=False),
        "agencies_total": int(enrichment_diag.get("agencies_total") or 0),
        "agencies_with_contacts": int(enrichment_diag.get("agencies_with_contacts") or 0),
    }
