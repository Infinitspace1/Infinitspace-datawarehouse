from shared.nexudus.exclusions import is_excluded_location_source_id
from shared.nexudus.transformers.locations import transform_location, transform_location_hours


def test_kingsbourne_house_is_excluded_location_source_id():
    assert is_excluded_location_source_id(1414964752)
    assert is_excluded_location_source_id("1414964752")


def test_excluded_location_is_not_transformed_to_silver():
    raw = {
        "Id": 1414964752,
        "Name": "London - Holborn - 229-231 High Holborn",
    }

    assert transform_location(raw, bronze_id=1, sync_run_id="sync-1") is None
    assert transform_location_hours(raw) is None

