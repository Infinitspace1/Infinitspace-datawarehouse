"""Read-only display of ava.location_plans after a refresh."""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")
from shared.azure_clients.sql_client import get_sql_client  # noqa: E402

sql = get_sql_client()

total = sql.execute_scalar("SELECT COUNT(*) FROM ava.location_plans")
print(f"TOTAL PLANS: {total}\n")

print("PER LOCATION:")
for r in sql.execute_query("""
    SELECT location_name,
           COUNT(*)           AS plans,
           MIN(price)         AS min_price,
           MAX(price)         AS max_price,
           MAX(currency_code) AS ccy
    FROM ava.location_plans
    GROUP BY location_name
    ORDER BY location_name
"""):
    print(f"  {r['location_name']:<42} {r['plans']:>3} plans  "
          f"{r['ccy']} {float(r['min_price']):>7.2f}–{float(r['max_price']):.2f}")

print("\nBY TYPE LABEL:")
for r in sql.execute_query("""
    SELECT system_tariff_type, system_tariff_type_label, COUNT(*) AS n
    FROM ava.location_plans
    GROUP BY system_tariff_type, system_tariff_type_label
    ORDER BY n DESC
"""):
    print(f"  type={r['system_tariff_type']:<4} {r['system_tariff_type_label']:<18} {r['n']}")

print("\nFULL CONTENTS (every row):")
print(f"  {'location':<40} {'plan':<48} {'price':>9} {'ccy':<4} {'fin. account':<32}")
print("  " + "-" * 135)
for r in sql.execute_query("""
    SELECT location_name, plan_name, price, currency_code,
           financial_account_name, system_tariff_type
    FROM ava.location_plans
    ORDER BY location_name, price DESC
"""):
    loc = (r['location_name'] or '')[:39]
    plan = (r['plan_name'] or '')[:47]
    fa = (r['financial_account_name'] or '')[:31]
    price = float(r['price']) if r['price'] is not None else 0.0
    print(f"  {loc:<40} {plan:<48} {price:>9.2f} {r['currency_code'] or '':<4} {fa:<32}")
