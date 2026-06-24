"""
shared/competence/classifier_service.py

Tier-2 (I/O) of the competitor flex-classifier: the Anthropic pass over the competitors the
free rules (shared.competence.classification) leave undecided, plus the DB read/write. The
pure rule logic and the prompt/parse live in `classification`; this module owns the side
effects (LLM calls, homepage fetches, SQL upserts) and the cost controls.

Why these cost levers (the scrape's category_name is only ever 3 flex-ish values, so it can't
filter — the website is the real signal, over ~15k rows):
  - DEDUP BY DOMAIN: classify one verdict per operator (website host) and apply it to every one
    of that operator's locations, so a 200-site chain is ONE LLM call, not 200. Main lever.
  - METADATA FIRST, ESCALATE: classify from name + Google category + domain in batches; fetch
    the homepage only for the units the model is unsure about, then re-ask with the page text.
  - HARD CAPS: ``max_ai_units`` per run + a sliding-window rate limit, so a run can't run away.

Verdicts are written per place_id into silver.competence_competitor_classification (see
scripts/sql_scripts/competence_classification.sql). Drive it via the nightly function
(functions/competence_classification.py) or the one-off backfill
(scripts/python_scripts/backfill_competitor_classification.py).
"""
from __future__ import annotations

import html
import logging
import os
import re
import time
from collections import OrderedDict, defaultdict
from dataclasses import asdict, dataclass, field
from typing import Iterable, Optional

import anthropic
import requests
import urllib3

from shared.azure_clients.sql_client import get_sql_client
from shared.competence.classification import (
    MAX_WEBSITE_EXCERPT,
    Verdict,
    build_classification_messages,
    classification_input_hash,
    classify_by_rules,
    domain_of,
    parse_classification_response,
)

logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_MODEL = os.getenv("COMPETENCE_CLASSIFIER_MODEL", "claude-haiku-4-5-20251001")
DEFAULT_BATCH_SIZE = int(os.getenv("COMPETENCE_CLASSIFIER_BATCH", "20"))
DEFAULT_RPM = int(os.getenv("COMPETENCE_CLASSIFIER_RPM", "45"))
MAX_TOKENS = 2000
FETCH_TIMEOUT = 8
_UA = "Mozilla/5.0 (compatible; InfinitSpaceBot/1.0; +https://infinitspace.com)"

_SCRIPT_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

_UPSERT_SQL = """
MERGE silver.competence_competitor_classification AS t
USING (SELECT ? AS place_id) AS s ON t.place_id = s.place_id
WHEN MATCHED THEN UPDATE SET
    is_flex = ?, confidence = ?, method = ?, model = ?, category_name = ?,
    input_hash = ?, reasoning = ?, classified_at = GETUTCDATE()
WHEN NOT MATCHED THEN INSERT
    (place_id, is_flex, confidence, method, model, category_name, input_hash, reasoning)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
"""


def _chunks(seq: list, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


class _RateLimiter:
    """Minimal sliding-window requests-per-minute limiter (mirrors the real_estate one)."""

    def __init__(self, rpm: int) -> None:
        self.rpm = max(1, rpm)
        self._calls: list[float] = []

    def wait(self) -> None:
        now = time.monotonic()
        self._calls = [t for t in self._calls if now - t < 60]
        if len(self._calls) >= self.rpm:
            time.sleep(max(0.0, 60 - (now - self._calls[0])) + 0.01)
        self._calls.append(time.monotonic())


@dataclass
class ClassifyReport:
    rows_loaded: int = 0
    units: int = 0
    rules_keep: int = 0
    rules_drop: int = 0
    ai_keep: int = 0
    ai_drop: int = 0
    ai_unsure: int = 0
    ai_calls: int = 0
    escalated: int = 0
    ai_skipped_cap: int = 0
    rows_written: int = 0
    by_category: dict = field(default_factory=dict)
    samples: list = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)


class CompetitorClassifier:
    """Classifies competitors as flexible-workspace operators (keep) or not, deduped by domain.

    ``trust_coworking_category`` lets the free tier auto-keep the "Coworking space" category
    (enable only once a sample shows it's clean). ``max_ai_units`` caps the LLM calls per run;
    ``fetch_websites`` toggles the escalation pass.
    """

    def __init__(
        self,
        *,
        model: str = DEFAULT_MODEL,
        batch_size: int = DEFAULT_BATCH_SIZE,
        rpm: int = DEFAULT_RPM,
        trust_coworking_category: bool = False,
        max_ai_units: Optional[int] = None,
        fetch_websites: bool = True,
    ) -> None:
        self.model = model
        self.batch_size = max(1, batch_size)
        self.trust = trust_coworking_category
        self.max_ai_units = max_ai_units
        self.fetch_websites = fetch_websites
        self._rl = _RateLimiter(rpm)
        self._client: Optional[anthropic.Anthropic] = None

    @property
    def client(self) -> anthropic.Anthropic:
        if self._client is None:
            self._client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        return self._client

    # ── DB read ──────────────────────────────────────────────────────────────
    def load_candidates(self, sql, *, limit: Optional[int] = None) -> list[dict]:
        """Active, geocoded-or-not competitors with a place_id that aren't classified yet."""
        top = f"TOP {int(limit)}" if limit else ""
        return sql.execute_query(
            f"""
            SELECT {top} c.place_id, c.title, c.category_name, c.website
            FROM silver.competence_competitors c
            LEFT JOIN silver.competence_competitor_classification k ON k.place_id = c.place_id
            WHERE c.is_deleted = 0 AND c.place_id IS NOT NULL AND k.place_id IS NULL
            ORDER BY c.place_id
            """
        )

    def build_units(self, rows: Iterable[dict]) -> list[dict]:
        """Collapse rows into classification UNITS: one per website domain (the operator),
        plus a singleton per row that has no usable website. The verdict for a domain applies
        to all of that operator's locations."""
        by_domain: "OrderedDict[str, dict]" = OrderedDict()
        singletons: list[dict] = []
        for r in rows:
            row = {"place_id": r["place_id"], "title": r["title"],
                   "category_name": r["category_name"], "website": r["website"]}
            dom = domain_of(r["website"])
            if dom:
                u = by_domain.get(dom)
                if u is None:
                    by_domain[dom] = {"key": f"dom:{dom}", "domain": dom, "website": r["website"],
                                      "title": r["title"], "category_name": r["category_name"], "rows": [row]}
                else:
                    u["rows"].append(row)
                    if not u["title"] and r["title"]:
                        u["title"] = r["title"]
                    if not u["category_name"] and r["category_name"]:
                        u["category_name"] = r["category_name"]
            else:
                singletons.append({"key": f"pid:{r['place_id']}", "domain": None, "website": None,
                                   "title": r["title"], "category_name": r["category_name"], "rows": [row]})
        return list(by_domain.values()) + singletons

    # ── orchestration ────────────────────────────────────────────────────────
    def classify(
        self, *, limit: Optional[int] = None, sample_per_category: Optional[int] = None,
        dry_run: bool = False, rules_only: bool = False,
    ) -> ClassifyReport:
        sql = get_sql_client()
        rows = self.load_candidates(sql, limit=limit)
        units = self.build_units(rows)
        if sample_per_category:
            units = self._sample(units, sample_per_category)

        report = ClassifyReport(rows_loaded=len(rows), units=len(units))
        verdicts: dict[str, Verdict] = {}
        undecided: list[dict] = []
        for u in units:
            v = classify_by_rules(u["title"], u["category_name"], trust_coworking_category=self.trust)
            if v.decided:
                verdicts[u["key"]] = v
                if v.is_flex:
                    report.rules_keep += 1
                else:
                    report.rules_drop += 1
            else:
                undecided.append(u)

        if not rules_only and undecided:
            ai_units = undecided if self.max_ai_units is None else undecided[: self.max_ai_units]
            report.ai_skipped_cap = len(undecided) - len(ai_units)
            self._classify_with_ai(ai_units, verdicts, report)

        if not dry_run:
            report.rows_written = self._persist(sql, units, verdicts)
        self._finalize(report, units, verdicts)
        return report

    def _classify_with_ai(self, units: list[dict], verdicts: dict, report: ClassifyReport) -> None:
        unsure: list[dict] = []
        for batch in _chunks(units, self.batch_size):
            res = self._ai_batch(batch, with_website=False, report=report)
            for u in batch:
                v = res.get(u["key"])
                if v is not None and v.is_flex is not None:
                    verdicts[u["key"]] = v
                    report.ai_keep += int(v.is_flex)
                    report.ai_drop += int(not v.is_flex)
                else:
                    unsure.append(u)

        if not unsure:
            return
        if not self.fetch_websites:
            for u in unsure:
                verdicts[u["key"]] = Verdict(None, "ai:meta", None, "unsure (website pass disabled)")
                report.ai_unsure += 1
            return

        for batch in _chunks(unsure, self.batch_size):
            for u in batch:
                u["website_excerpt"] = self._fetch_excerpt(u.get("website")) if u.get("domain") else None
                if u.get("website_excerpt"):
                    report.escalated += 1
            res = self._ai_batch(batch, with_website=True, report=report)
            for u in batch:
                v = res.get(u["key"])
                if v is not None and v.is_flex is not None:
                    verdicts[u["key"]] = v
                    report.ai_keep += int(v.is_flex)
                    report.ai_drop += int(not v.is_flex)
                else:
                    verdicts[u["key"]] = Verdict(None, "ai:web", None, "unsure after website")
                    report.ai_unsure += 1

    def _ai_batch(self, units: list[dict], *, with_website: bool, report: ClassifyReport) -> dict[str, Verdict]:
        items = [{"id": u["key"], "title": u["title"], "category_name": u["category_name"],
                  "domain": u.get("domain") or "", "website_excerpt": u.get("website_excerpt")} for u in units]
        system, user = build_classification_messages(items, with_website=with_website)
        method = "ai:web" if with_website else "ai:meta"
        self._rl.wait()
        report.ai_calls += 1
        try:
            resp = self.client.messages.create(
                model=self.model, max_tokens=MAX_TOKENS, temperature=0, system=system,
                messages=[{"role": "user", "content": user}],
            )
            text = "".join(getattr(b, "text", "") for b in resp.content)
            return parse_classification_response(text, [it["id"] for it in items], method=method)
        except Exception as exc:  # noqa: BLE001 — one bad batch must not abort the run
            logger.warning("AI classify batch failed (%s); %d units left unsure", exc, len(items))
            return {}

    def _fetch_excerpt(self, website: Optional[str]) -> Optional[str]:
        if not website:
            return None
        url = website if "://" in website else "https://" + website
        try:
            r = requests.get(url, timeout=FETCH_TIMEOUT, headers={"User-Agent": _UA},
                             verify=False, allow_redirects=True)
            if r.status_code >= 400 or not r.text:
                return None
            text = _SCRIPT_RE.sub(" ", html.unescape(r.text))
            text = _WS_RE.sub(" ", _TAG_RE.sub(" ", text)).strip()
            return text[:MAX_WEBSITE_EXCERPT] or None
        except Exception as exc:  # noqa: BLE001 — a dead site is just a metadata-only classify
            logger.debug("homepage fetch failed for %s: %s", url, exc)
            return None

    # ── DB write ─────────────────────────────────────────────────────────────
    def _persist(self, sql, units: list[dict], verdicts: dict) -> int:
        params: list[tuple] = []
        for u in units:
            v = verdicts.get(u["key"])
            if v is None:  # skipped by the AI cap — leave unclassified so a later run retries
                continue
            model = self.model if (v.method or "").startswith("ai") else None
            is_flex = None if v.is_flex is None else (1 if v.is_flex else 0)
            conf = round(float(v.confidence), 3) if isinstance(v.confidence, (int, float)) else None
            reasoning = (v.reasoning or "")[:500] or None
            for row in u["rows"]:
                ihash = classification_input_hash(row["title"], row["category_name"], row["website"])
                vals = (is_flex, conf, v.method, model, row["category_name"], ihash, reasoning)
                params.append((row["place_id"], *vals, row["place_id"], *vals))
        written = 0
        for batch in _chunks(params, 500):
            sql.execute_many(_UPSERT_SQL, batch)
            written += len(batch)
        return written

    # ── reporting helpers ────────────────────────────────────────────────────
    def _sample(self, units: list[dict], n: int) -> list[dict]:
        buckets: dict[str, list[dict]] = defaultdict(list)
        for u in units:
            buckets[u["category_name"] or "∅"].append(u)
        out: list[dict] = []
        for us in buckets.values():
            out.extend(us[:n])
        return out

    def _finalize(self, report: ClassifyReport, units: list[dict], verdicts: dict) -> None:
        bycat: dict[str, dict] = {}
        for u in units:
            cat = u["category_name"] or "∅"
            b = bycat.setdefault(cat, {"units": 0, "keep": 0, "drop": 0, "unsure": 0, "undecided": 0})
            b["units"] += 1
            v = verdicts.get(u["key"])
            if v is None:
                b["undecided"] += 1
            elif v.is_flex is True:
                b["keep"] += 1
            elif v.is_flex is False:
                b["drop"] += 1
            else:
                b["unsure"] += 1
            if len(report.samples) < 30 and v is not None and (v.method or "").startswith("ai"):
                report.samples.append({
                    "title": u["title"], "category": u["category_name"], "domain": u.get("domain"),
                    "is_flex": v.is_flex, "method": v.method, "confidence": v.confidence,
                })
        report.by_category = bycat
