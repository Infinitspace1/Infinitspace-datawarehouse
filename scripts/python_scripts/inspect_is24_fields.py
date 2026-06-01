"""Dump price-related fields from a saved IS24 raw payload."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
path = ROOT / "tmp_is24_raw_197.json"
p = json.loads(path.read_text(encoding="utf-8"))

print("--- normalized ---")
print(json.dumps(p.get("normalized"), indent=2, ensure_ascii=False))
print()

print("--- adTargetingParameters (price/area-related) ---")
ap = p.get("adTargetingParameters") or {}
hints = ("rent", "price", "cost", "sqm", "calc", "interval", "base", "neben", "warm", "kalt", "floor", "space", "total", "from", "to", "area", "nutz", "haupt", "size", "scout")
for k in sorted(ap):
    if any(h in k.lower() for h in hints):
        print(f"  {k} = {ap[k]}")
print()

print("--- sections (types only) ---")
for i, s in enumerate(p.get("sections") or []):
    print(f"  [{i}] type={s.get('type')} keys={sorted(s.keys())}")
print()

print("--- TOP_ATTRIBUTES sections ---")
for s in p.get("sections") or []:
    if s.get("type") == "TOP_ATTRIBUTES":
        print(json.dumps(s, indent=2, ensure_ascii=False))
        print()

print("--- ATTRIBUTE_LIST sections (first 3) ---")
count = 0
for s in p.get("sections") or []:
    if s.get("type") in ("ATTRIBUTE_LIST", "ATTRIBUTES", "CRITERIA"):
        print(json.dumps(s, indent=2, ensure_ascii=False))
        count += 1
        if count >= 3:
            break
print()

print("--- basicInfo ---")
print(json.dumps(p.get("basicInfo"), indent=2, ensure_ascii=False))
