from shared.location_scraper.run_quality import compute_run_quality_payload


def test_compute_run_quality_payload():
    listings = [
        {
            "latitude": 1.0,
            "longitude": 2.0,
            "phone": "+1",
            "contact_name": "A",
            "company_name": "",
        },
        {"latitude": None, "longitude": None, "phone": "", "contact_name": "", "company_name": "Co"},
    ]
    bundles = [{"email_1": "a@x.com", "email_2": "", "email_3": "c@x.com"}]
    diag = {"agencies_total": 5, "agencies_with_contacts": 2, "reasons": {}}

    out = compute_run_quality_payload(
        raw_item_count=10,
        listings=listings,
        bundles=bundles,
        enrichment_diag=diag,
    )

    assert out["raw_item_count"] == 10
    assert out["normalized_count"] == 2
    assert out["with_coords_count"] == 1
    assert out["with_phone_count"] == 1
    assert out["with_name_or_company_count"] == 2
    assert out["lusha_email_slots"] == 2
    assert out["agencies_total"] == 5
    assert out["agencies_with_contacts"] == 2
    assert "enrichment_diagnostics_json" in out
