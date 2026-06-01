from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.location_scraper.activities.materialize_globe import _pick_currency


def test_otodom_currency_uses_source_value_when_present():
    assert _pick_currency({"priceCurrency": "EUR"}, "otodom") == "EUR"
    assert _pick_currency({"priceCurrency": "PLN"}, "otodom") == "PLN"


def test_otodom_currency_defaults_to_pln():
    assert _pick_currency({}, "otodom") == "PLN"


def test_euro_sources_default_to_eur():
    assert _pick_currency({}, "idealista") == "EUR"
    assert _pick_currency({}, "immobilienscout") == "EUR"
