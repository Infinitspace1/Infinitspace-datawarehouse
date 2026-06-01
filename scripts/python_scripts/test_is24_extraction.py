"""Smoke-test the IS24 extraction logic on the saved raw payloads."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from shared.location_scraper.activities.materialize_globe import (
    _is24_attr_decimal,
    _parse_eu_number,
    _pick_decimal,
    _pick_str,
)


def main() -> int:
    files = sorted(ROOT.glob("tmp_is24_raw_*.json"))
    if not files:
        print("No tmp_is24_raw_*.json files found.")
        return 1
    print(f"Found {len(files)} payload(s).\n")
    for f in files:
        payload = json.loads(f.read_text(encoding="utf-8"))
        price_monthly = _pick_decimal(
            payload,
            "price",
            "priceInfo.amount",
            "basicInfo.price",
            "normalized.price.amount",
            "adTargetingParameters.obj_rentPerMonth",
            "obj_totalRent",
        )
        price_per_m2 = _pick_decimal(
            payload,
            "pricePerM2",
            "priceByArea",
            "adTargetingParameters.obj_rentPerSqM",
            "obj_baseRent",
            "basicInfo.priceByArea",
        )
        surface_m2 = _pick_decimal(
            payload,
            "area",
            "moreCharacteristics.constructedArea",
            "basicInfo.size",
            "adTargetingParameters.obj_mainFloorSpace",
            "normalized.area.livingSpace",
            "obj_netFloorSpace",
        )
        if price_per_m2 is None:
            price_per_m2 = _is24_attr_decimal(payload, "miete/m", "monatl. miete pro m")
        nebenkosten = _is24_attr_decimal(payload, "nebenkosten/m", "nebenkosten")
        teilbar = _is24_attr_decimal(payload, "teilbar ab", "fläche teilbar ab")
        price_kind = _pick_str(payload, "normalized.price.kind")

        total_per_m2 = None
        if price_per_m2 is not None and nebenkosten is not None:
            total_per_m2 = price_per_m2 + nebenkosten
        elif price_per_m2 is not None:
            total_per_m2 = price_per_m2

        estimated = 0
        if price_monthly is None and price_per_m2 is not None and surface_m2 is not None:
            price_monthly = price_per_m2 * surface_m2
            estimated = 1

        print(f"--- {f.name} ---")
        print(f"  price_monthly             = {price_monthly}  (estimated={estimated})")
        print(f"  price_per_m2              = {price_per_m2}")
        print(f"  additional_costs_per_m2   = {nebenkosten}")
        print(f"  total_price_per_m2        = {total_per_m2}")
        print(f"  surface_m2                = {surface_m2}")
        print(f"  divisible_from_m2         = {teilbar}")
        print(f"  price_kind                = {price_kind}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
