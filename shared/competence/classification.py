"""
shared/competence/classification.py

Pure logic for classifying a scraped competitor as a flexible-workspace operator
(coworking / serviced & managed offices / business centres / flex space — anywhere a
company could realistically rent desks or private offices) vs. anything else. No I/O.

Two tiers (the I/O service in this package drives them):
  - Tier 1 — `classify_by_rules`: free, deterministic. Decides the easy majority from the
    Google-Places `category_name` (+ `title`): clear flex -> keep, clearly-unrelated
    consumer categories (restaurant/hotel/gym/...) -> drop. Returns an UNDECIDED verdict
    for the ambiguous middle (generic "office", real-estate agents, empty category, ...).
  - Tier 2 — `build_classification_messages` / `parse_classification_response`: builds the
    batched LLM prompt for the ambiguous middle and parses the JSON reply. Metadata-only
    first (name + Google category + domain); the service escalates to a website excerpt
    only when the model is unsure.

`classification_input_hash` lets the service re-classify only when the inputs that drive a
verdict change (title / category / domain), so the daily run is near-free in steady state.

The KEEP/DROP vocabularies below are intentionally easy to tune once we see the real
`category_name` distribution (`SELECT category_name, COUNT(*) ... GROUP BY category_name`).
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import urlsplit

# The single shared definition of "keep", reused by the LLM prompt so the AI tier and the
# rule tier agree on the boundary.
FLEX_DEFINITION = (
    "A flexible-workspace operator runs physical space where a company can rent desks or "
    "private offices on flexible terms: coworking spaces, serviced or managed offices, "
    "business centres, and hybrid/flex office providers. Keep only operators that run the "
    "space themselves.\n"
    "NOT flexible workspace: commercial real-estate brokers, agents or marketplaces that "
    "merely list or broker space for others (Google often tags these 'Office space rental "
    "agency'); traditional landlords leasing whole buildings; virtual-office / mailbox-only "
    "services with no physical workspace to sit in; conference or event venues; and unrelated "
    "businesses (restaurants, hotels, gyms, shops, etc.)."
)

# Tier-1 vocabularies — matched case-insensitively as substrings of `category_name`.
#
# With the current APIFY scrape the Google `category_name` is only ever one of three flex-ish
# values ("Coworking space", "Office space rental agency", "Business center"), because those
# were the search seeds. So a category match canNOT by itself prove a real operator — brokers,
# virtual-office services and event venues all wear those same labels. The discriminator is the
# actual business (its website). The rule tier therefore only AUTO-DROPS clearly-unrelated
# categories (future-proofing other scrapes) and, when explicitly trusted, auto-keeps the most
# specific "coworking" category; everything else goes to the AI tier (deduped by domain).
COWORKING_CATEGORY_TERMS: tuple[str, ...] = ("coworking", "co-working")

DROP_CATEGORY_TERMS: tuple[str, ...] = (
    "restaurant", "cafe", "café", "coffee", "bar", "pub", "brewery", "hotel", "hostel",
    "motel", "resort", "gym", "fitness", "yoga", "pilates", "spa", "massage", "salon",
    "barber", "hairdresser", "beauty", "nail", "tattoo", "bank", "atm", "insurance",
    "pharmacy", "drugstore", "supermarket", "grocery", "convenience store", "clothing",
    "furniture", "electronics", "hardware store", "book store", "bookstore", "jewelry",
    "jeweler", "florist", "bakery", "butcher", "school", "college", "university",
    "kindergarten", "hospital", "clinic", "dentist", "dental", "doctor", "physician",
    "veterinar", "church", "mosque", "temple", "synagogue", "museum", "art gallery",
    "theater", "theatre", "cinema", "movie", "amusement", "park", "playground",
    "parking", "gas station", "petrol", "car repair", "car dealer", "auto repair",
    "tire shop", "laundry", "dry clean", "courier", "post office", "travel agency",
    "tourist", "night club", "nightclub", "casino", "stadium", "warehouse", "storage",
)

def _compile_terms(terms: tuple[str, ...]) -> "re.Pattern[str]":
    """Whole-word, case-insensitive matcher for a term list. Word boundaries matter:
    a naive substring check would match 'spa' inside 'coworking space'."""
    return re.compile(r"\b(?:" + "|".join(re.escape(t) for t in terms) + r")\b", re.IGNORECASE)


_DROP_RE = _compile_terms(DROP_CATEGORY_TERMS)
_COWORKING_RE = _compile_terms(COWORKING_CATEGORY_TERMS)

# How much homepage text to hand the model on the escalation pass.
MAX_WEBSITE_EXCERPT = 2500

_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class Verdict:
    """A classification outcome.

    ``is_flex``: True = keep, False = drop, None = undecided (ambiguous -> needs the AI tier,
    or the AI was itself unsure). ``method`` records how it was decided
    ('rule:category' / 'rule:title' / 'ai:meta' / 'ai:web'); ``confidence`` is 0..1 or None.
    """
    is_flex: Optional[bool]
    method: Optional[str] = None
    confidence: Optional[float] = None
    reasoning: Optional[str] = None

    @property
    def decided(self) -> bool:
        return self.is_flex is not None


UNDECIDED = Verdict(None)


def classify_by_rules(
    title: Optional[str], category_name: Optional[str], *, trust_coworking_category: bool = False
) -> Verdict:
    """Tier-1 free classification.

    Auto-DROPS clearly-unrelated categories. When ``trust_coworking_category`` is set it also
    auto-KEEPS the specific "coworking" category — a cost lever to enable only once a sample
    confirms that category is clean. Everything else returns UNDECIDED: the ambiguous middle the
    AI tier resolves from the website. ``title`` is accepted for forward-compatibility (future
    name-based rules) and currently unused.
    """
    cat = (category_name or "").strip()
    if cat and _DROP_RE.search(cat):
        return Verdict(False, "rule:category", 0.95, f"category={category_name!r}")
    if trust_coworking_category and cat and _COWORKING_RE.search(cat):
        return Verdict(True, "rule:category", 0.85, f"trusted coworking category={category_name!r}")
    return UNDECIDED


def domain_of(website: Optional[str]) -> Optional[str]:
    """The bare registrable host of a website ('https://www.Spaces.com/x' -> 'spaces.com'),
    or None. Used both as a cheap LLM signal and as part of the input hash."""
    s = (website or "").strip()
    if not s:
        return None
    if "//" not in s:
        s = "//" + s
    host = urlsplit(s).netloc.lower().split("@")[-1].split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    return host or None


def classification_input_hash(
    title: Optional[str], category_name: Optional[str], website: Optional[str]
) -> str:
    """SHA-256 of the inputs that drive a verdict (title, category, domain). The service
    re-classifies a place only when this changes, so steady-state cost stays near zero."""
    basis = "".join([
        (title or "").strip().lower(),
        (category_name or "").strip().lower(),
        domain_of(website) or "",
    ])
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def build_classification_messages(
    items: list[dict], *, with_website: bool = False
) -> tuple[str, str]:
    """Build the (system, user) messages to classify a BATCH of ambiguous competitors.

    Each item: ``{"id", "title", "category_name", "domain", optional "website_excerpt"}``.
    ``with_website`` includes the homepage excerpt (the escalation pass). The model is asked
    for a JSON array of ``{"id", "verdict": yes|no|unsure, "confidence": 0..1}``.
    """
    entries = []
    for it in items:
        entry = {
            "id": str(it["id"]),
            "name": (it.get("title") or "")[:200],
            "google_category": (it.get("category_name") or "")[:200],
            "domain": it.get("domain") or "",
        }
        if with_website and it.get("website_excerpt"):
            entry["website_excerpt"] = str(it["website_excerpt"])[:MAX_WEBSITE_EXCERPT]
        entries.append(entry)

    unsure_clause = (
        "Use \"unsure\" only if the name, category, domain AND the website excerpt still "
        "don't let you decide."
        if with_website else
        "Use \"unsure\" if the name, category and domain don't let you decide (a website "
        "check could still resolve it)."
    )
    system = (
        "You classify businesses as flexible-workspace operators for a brokerage's "
        "competitor map. Be precise and decisive; reply with JSON only, no prose."
    )
    user = (
        f"{FLEX_DEFINITION}\n\n"
        "For EACH item, return one JSON object: "
        "{\"id\": <id>, \"verdict\": \"yes\"|\"no\"|\"unsure\", \"confidence\": <0..1>}. "
        "\"yes\" = a flexible-workspace operator, \"no\" = not one. "
        f"{unsure_clause}\n"
        "Return ONLY a JSON array of those objects, nothing else.\n\n"
        f"Items:\n{json.dumps(entries, ensure_ascii=False)}"
    )
    return system, user


def _strip_fences(text: str) -> str:
    s = (text or "").strip()
    # Pull out the outermost JSON array if the model wrapped it in prose/fences.
    start, end = s.find("["), s.rfind("]")
    if start != -1 and end != -1 and end > start:
        return s[start:end + 1]
    return _FENCE_RE.sub("", s)


def parse_classification_response(
    text: str, valid_ids: Iterable[str], *, method: str = "ai:meta"
) -> dict[str, Verdict]:
    """Parse the model's JSON array into ``{id: Verdict}``, keeping only known ids.
    A 'yes'/'no' is decided; 'unsure' (or anything unrecognised) yields an UNDECIDED
    verdict so the caller can escalate (fetch the website) or leave it unresolved.
    Tolerant of code-fences / surrounding prose; raises ValueError on non-JSON."""
    ids = {str(i) for i in valid_ids}
    data = json.loads(_strip_fences(text))
    if isinstance(data, dict):
        data = [data]
    out: dict[str, Verdict] = {}
    for obj in data:
        if not isinstance(obj, dict):
            continue
        oid = str(obj.get("id"))
        if oid not in ids:
            continue
        verdict = str(obj.get("verdict", "")).strip().lower()
        raw_conf = obj.get("confidence")
        conf = float(raw_conf) if isinstance(raw_conf, (int, float)) else None
        if verdict in ("yes", "y", "true"):
            is_flex: Optional[bool] = True
        elif verdict in ("no", "n", "false"):
            is_flex = False
        else:
            is_flex = None
        out[oid] = Verdict(is_flex, method if is_flex is not None else None, conf)
    return out
