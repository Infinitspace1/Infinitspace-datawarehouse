"""
shared/cpi/client.py

National consumer price indices for the three countries the estate operates in,
from each country's official statistics API. No API key is needed anywhere.

WHY NATIONAL AND NOT PER CITY
-----------------------------
The buildings sit in London, Berlin, Amsterdam and Hoofddorp, but a monthly
CITY-level CPI does not exist for three of those four:
  * ONS publishes CPI/CPIH/RPI for the UK only. Its own machine-readable CPIH
    dataset declares its geography dimension as literally `uk-only`. The
    sub-national "consumption segment indices" it began publishing in 2026 are
    item-level NUTS1 research output that ONS itself labels "not accredited
    official statistics".
  * CBS table 86141NED has exactly two dimensions - expenditure category and
    period. There is no regional axis to filter on, so no Amsterdam figure.
  * Berlin is the sole exception (a city-state with its own statistical office),
    but that series is published as PDF/XLSX behind hashed URLs with no API.
Escalation clauses in all three countries name a NATIONAL index anyway, so this
is the number a contract actually references.

SERIES CHOICE
-------------
  UK  ONS      CPI    D7G7 (annual rate %) + D7BT (index, 2015=100)
  NL  CBS      CPI    M000238 (Jaarmutatie) + M000215 (index, 2025=100),
                      expenditure category T001112 = alle huishoudens
  DE  Eurostat HICP   prc_hicp_minr RCH_A (annual rate %) + I25 (index, 2025=100)

Germany is deliberately the HARMONISED index, not the domestic VPI. Destatis
GENESIS requires a registered account (its data endpoint answers HTTP 401
anonymously, including with the commonly cited GAST guest login), and the team
chose Eurostat over creating one. HICP and VPI are NOT the same index - they
happened to both read 2.8% in July 2026, which is coincidence. `index_code` is
recorded as "HICP" for Germany so nothing downstream can mistake it for the VPI
a German agreement would name.

BOTH LEVEL AND RATE ARE STORED
------------------------------
Every escalation clause in these three countries computes a RATIO of two index
levels at named reference months. Storing only the 12-month rate makes that
calculation impossible to reproduce, and the published rate carries one decimal
where the level carries two.

TRAPS THIS CODE AVOIDS (all verified live, 2026-08-20)
------------------------------------------------------
  * CBS 83131NED is DISCONTINUED (superseded by 86141NED on a 2025=100 base).
  * Eurostat prc_hicp_manr is DISCONTINUED (superseded by prc_hicp_minr, whose
    COICOP dimension is renamed `coicop18` and whose total code is `TOTAL`,
    not `CP00`).
Both retired endpoints still answer HTTP 200 with an EMPTY value set rather than
erroring, so a job pointed at either would silently freeze at last year's number
instead of failing. `fetch_series` raises when a provider yields no observations
for exactly that reason - see also the freshness assertion in functions/cpi_sync.py.

Fair use: these are public, unauthenticated, low-volume endpoints. We send an
identifying User-Agent and fetch a rolling window once a day.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date

import requests

logger = logging.getLogger(__name__)

ONS_URL = os.getenv("CPI_ONS_URL", "https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries")
CBS_URL = os.getenv("CPI_CBS_URL", "https://datasets.cbs.nl/odata/v1/CBS/86141NED/Observations")
EUROSTAT_URL = os.getenv(
    "CPI_EUROSTAT_URL",
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_minr",
)
USER_AGENT = os.getenv(
    "CPI_USER_AGENT",
    "infinitspace-datawarehouse/1.0 (data-engineering@infinitspace.com)",
)

REQUEST_TIMEOUT = 60
MAX_RETRIES = 5
MIN_INTERVAL_SECONDS = 1.0  # be a good citizen on public statistics endpoints

# How far back to re-fetch on every run. Long enough that a missed day (or a
# week of failures) self-heals without a backfill, and long enough to pick up
# the back-series revisions these offices publish.
DEFAULT_MONTHS = int(os.getenv("CPI_WINDOW_MONTHS", "18"))

_ONS_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# One entry per country. `rate` and `level` are the provider's own series ids.
SERIES: tuple[dict, ...] = (
    {
        "provider": "ons", "geo": "UK", "index_code": "CPI",
        "index_name": "CPI all items (ONS, UK)", "base_year": "2015",
        "rate": "d7g7", "level": "d7bt",
    },
    {
        "provider": "cbs", "geo": "NL", "index_code": "CPI",
        "index_name": "Consumentenprijsindex alle huishoudens (CBS, NL)", "base_year": "2025",
        "rate": "M000238", "level": "M000215",
    },
    {
        "provider": "eurostat", "geo": "DE", "index_code": "HICP",
        "index_name": "HICP all items (Eurostat, DE) - NOT the Destatis VPI",
        "base_year": "2025", "rate": "RCH_A", "level": "I25",
    },
)


def _get(url: str, params: dict | None = None) -> dict:
    """GET with the house retry contract: honour Retry-After on 429, back off on 5xx."""
    last: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                url, params=params,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                logger.warning("CPI: 429 from %s, sleeping %ss", url, wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 500 and attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:  # noqa: PERF203
            last = exc
            if attempt == MAX_RETRIES - 1:
                break
            time.sleep(2 ** attempt)
    raise RuntimeError(f"CPI fetch failed after {MAX_RETRIES} retries: {url}") from last


def cutoff_period(months: int, today: date | None = None) -> str:
    """Oldest period ("YYYY-MM") to keep, `months` back from today inclusive."""
    d = today or date.today()
    idx = d.year * 12 + (d.month - 1) - (months - 1)
    return f"{idx // 12:04d}-{idx % 12 + 1:02d}"


# ── Per-provider readers: each returns {period: value} ───────────────────────

def _read_ons(series_id: str, since: str) -> tuple[dict[str, float], dict[str, str]]:
    """(values by period, published_at by period) for one ONS CDID."""
    payload = _get(f"{ONS_URL}/{series_id}/mm23/data")
    values: dict[str, float] = {}
    published: dict[str, str] = {}
    for row in payload.get("months") or []:
        # "2026 JUL" -> "2026-07". Anything unparseable is skipped rather than
        # guessed at: a mis-parsed period would silently overwrite another month.
        parts = str(row.get("date", "")).split()
        if len(parts) != 2 or parts[1].upper() not in _ONS_MONTHS:
            continue
        period = f"{parts[0]}-{_ONS_MONTHS[parts[1].upper()]:02d}"
        if period < since:
            continue
        raw = row.get("value")
        if raw in (None, ""):
            continue
        values[period] = float(raw)          # ONS returns values as STRINGS
        published[period] = row.get("updateDate") or ""
    return values, published


def _read_cbs(measure: str, since: str) -> tuple[dict[str, float], dict[str, str]]:
    """(values by period, ValueAttribute by period) for one CBS measure."""
    payload = _get(CBS_URL, {
        "$filter": f"Measure eq '{measure}' and Bestedingscategorieen eq 'T001112'",
        "$orderby": "Id desc",
        "$top": "400",
    })
    values: dict[str, float] = {}
    attrs: dict[str, str] = {}
    for row in payload.get("value") or []:
        # Perioden mixes months (2026MM07), quarters (2026KW03) and years
        # (2026JJ00) in one dimension - keep only the monthly rows.
        code = str(row.get("Perioden", ""))
        if "MM" not in code:
            continue
        year, month = code.split("MM")
        period = f"{year}-{int(month):02d}"
        if period < since or row.get("Value") is None:
            continue
        values[period] = float(row["Value"])
        attrs[period] = str(row.get("ValueAttribute") or "")
    return values, attrs


def _read_eurostat(geo: str, unit: str, since: str) -> dict[str, float]:
    """Values by period for one Eurostat unit. JSON-stat: `value` is keyed by the
    FLAT index, which equals the time index only because every other dimension
    is pinned to a single code here."""
    payload = _get(EUROSTAT_URL, {
        "format": "JSON", "lang": "EN", "coicop18": "TOTAL",
        "unit": unit, "geo": geo, "sinceTimePeriod": since,
    })
    index = (payload.get("dimension", {}).get("time", {}).get("category", {}).get("index")) or {}
    raw = payload.get("value") or {}
    out: dict[str, float] = {}
    for period, pos in index.items():
        value = raw.get(str(pos))
        if value is None or period < since:
            continue
        out[period] = float(value)
    return out


def _cbs_status(attribute: str) -> str:
    """CBS marks not-yet-final figures with a ValueAttribute (e.g. 'Voorlopig').

    This matters: CBS states outright that its provisional figures 'zijn niet
    geschikt om te gebruiken voor indexering'. Anything that is not the plain
    'None' marker is treated as provisional.
    """
    return "definitive" if attribute.strip().lower() in ("", "none") else "provisional"


def fetch_series(months: int | None = None, today: date | None = None) -> list[dict]:
    """Every country's observations over the rolling window, one row per month.

    Each row carries BOTH the index level and the annual rate, plus the status
    and the URL it came from. Raises if a provider returns nothing at all - a
    retired endpoint answers 200 with an empty body, and silently writing zero
    rows would freeze the figure the tool shows.
    """
    months = months or DEFAULT_MONTHS
    since = cutoff_period(months, today)
    out: list[dict] = []

    for spec in SERIES:
        provider, geo = spec["provider"], spec["geo"]
        published: dict[str, str] = {}
        attrs: dict[str, str] = {}

        if provider == "ons":
            rates, published = _read_ons(spec["rate"], since)
            levels, _ = _read_ons(spec["level"], since)
            url = f"{ONS_URL}/{spec['rate']}/mm23/data"
        elif provider == "cbs":
            rates, attrs = _read_cbs(spec["rate"], since)
            levels, _ = _read_cbs(spec["level"], since)
            url = CBS_URL
        else:
            rates = _read_eurostat(geo, spec["rate"], since)
            levels = _read_eurostat(geo, spec["level"], since)
            url = EUROSTAT_URL

        if not rates and not levels:
            raise RuntimeError(
                f"CPI: {provider} returned no observations since {since}. "
                "A discontinued endpoint answers 200 with an empty value set - "
                "check the series ids before assuming an outage."
            )

        for period in sorted(set(rates) | set(levels)):
            out.append({
                "source_id": f"{provider}:{geo}:{period}",
                "provider": provider,
                "geo": geo,
                "index_code": spec["index_code"],
                "index_name": spec["index_name"],
                "base_year": spec["base_year"],
                "period": period,
                "index_level": levels.get(period),
                "annual_rate_pct": rates.get(period),
                "status": _cbs_status(attrs.get(period, "")) if provider == "cbs" else "definitive",
                "source_url": url,
                "published_at": published.get(period) or None,
            })

        logger.info("CPI %s/%s: %s periods since %s", provider, geo, len(out), since)
        time.sleep(MIN_INTERVAL_SECONDS)

    return out
