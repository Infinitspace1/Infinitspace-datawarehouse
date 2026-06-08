"""
shared/firebase/competence.py

Read layer for the Firestore `competence_new` collection (TeamAndy lead-gen).

Each parent document is a per-country competitor list; the competitor records
live in a `competitors` subcollection (schema v2), with a legacy fallback to an
in-document `competitors` array (schema v1) — exactly like
`_read_competitors_for_list` in the AI-teamandy frontend routes.

Because the same physical competitor (placeId) can appear under more than one
parent list (overlapping polygons / a manual list overlapping an auto one), the
bronze key for a competitor is **composite**: `{list_id}::{competitor_doc_id}`.
That keeps every (list, competitor) pair distinct and preserves which list each
competitor belongs to.

Pure reads only — no transformation, no SQL.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from urllib.parse import urlparse

from google.cloud.firestore_v1.base_query import FieldFilter

logger = logging.getLogger(__name__)

COMPETENCE_NEW_COLLECTION = "competence_new"
COMPETITORS_SUBCOLLECTION = "competitors"

# Max length of the composite competitor source_id, kept within SQL Server's
# 900-byte (450-char NVARCHAR) index-key limit.
_MAX_SOURCE_ID_LEN = 450

# Firestore doc ids may only contain these characters; everything else -> "_".
# (Ported from AI-teamandy main.py `_competitor_doc_id`.)
_FIRESTORE_DOC_ID_RE = re.compile(r"[^A-Za-z0-9_\-]")


# ── Competitor identity (ported from AI-teamandy main.py) ────────────────────

def _extract_domain(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def _unique_key(comp: dict) -> str:
    """Stable identity for a competitor (ported from AI-teamandy main.py)."""
    place_id = (comp.get("placeId") or "").strip()
    if place_id:
        return f"placeId:{place_id}"
    domain = _extract_domain(comp.get("website", "") or "")
    if domain:
        return f"domain:{domain}"
    title = (comp.get("title") or "").strip().lower()
    address = (comp.get("address") or "").strip().lower()
    if title or address:
        return f"title_address:{title}|{address}"
    # Last resort. The AI-teamandy original uses id(comp), but bronze needs an
    # id that is stable across runs, so hash the record deterministically.
    return "fallback:" + hashlib.sha1(
        json.dumps(comp, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def competitor_doc_id(comp: dict) -> str:
    """
    Firestore-safe document id for a competitor (ported from AI-teamandy
    main.py `_competitor_doc_id`). Only used to key legacy v1 in-doc array
    records; v2 subcollection records already carry a real document id.
    """
    place_id = (comp.get("placeId") or "").strip()
    if place_id:
        return _FIRESTORE_DOC_ID_RE.sub("_", place_id)[:1500] or "unknown"
    return "k_" + hashlib.sha1(_unique_key(comp).encode("utf-8")).hexdigest()


def compose_competitor_source_id(list_id: str, comp_doc_id: str) -> str:
    """Composite bronze key for a (list, competitor) pair, bounded to 450 chars."""
    composite = f"{list_id}::{comp_doc_id}"
    if len(composite) <= _MAX_SOURCE_ID_LEN:
        return composite
    # Pathologically long competitor doc id: keep the readable list prefix and
    # hash the tail so the key stays unique and within the index-key limit.
    return f"{list_id}::h_{hashlib.sha1(comp_doc_id.encode('utf-8')).hexdigest()}"[:_MAX_SOURCE_ID_LEN]


# ── Incremental readers ──────────────────────────────────────────────────────

def read_competence_lists(db) -> list[tuple[str, dict]]:
    """Read every competence_new parent doc as (list_id, data). Cheap (~tens of
    docs) so it runs in full on every sync, even incremental ones."""
    out = [(doc.id, doc.to_dict() or {}) for doc in db.collection(COMPETENCE_NEW_COLLECTION).stream()]
    logger.info("competence_new: read %s lists", len(out))
    return out


def read_competitors_since(db, since) -> list[tuple[str, str, dict]]:
    """Read only competitor docs changed since `since`, via a collection-group
    query on `updated_at`.

    Returns [(competitor_source_id, list_id, competitor_dict), ...] for docs under
    competence_new only. Requires a Firestore COLLECTION_GROUP index on
    `competitors.updated_at` — Firestore returns a creation link the first time
    this query runs, and raises google.api_core.exceptions.FailedPrecondition
    until the index exists. Note: only v2 subcollection competitors are covered;
    legacy v1 in-doc arrays are handled by the caller via read_competence_lists.
    """
    out: list[tuple[str, str, dict]] = []
    query = db.collection_group(COMPETITORS_SUBCOLLECTION).where(
        filter=FieldFilter("updated_at", ">", since)
    )
    for snap in query.stream():
        ref = snap.reference
        # Path is competence_new/{list_id}/competitors/{doc_id}; ignore any other
        # `competitors` subcollections that might exist elsewhere in the database.
        if ref.parent.parent.parent.id != COMPETENCE_NEW_COLLECTION:
            continue
        list_id = ref.parent.parent.id
        out.append(
            (compose_competitor_source_id(list_id, snap.id), list_id, snap.to_dict() or {})
        )
    logger.info("competence_new: read %s competitors changed since %s", len(out), since)
    return out


# ── Full reader ──────────────────────────────────────────────────────────────

def read_competence(db) -> list[dict]:
    """
    Read every competence_new list and its competitors.

    Returns a list of dicts, one per parent document:
        {
            "list_id":     <Firestore parent doc id>,
            "data":        <parent doc dict>,
            "competitors": [(competitor_source_id, competitor_dict), ...],
        }

    Competitors come from the `competitors` subcollection (schema v2) when it
    has any documents, otherwise from the in-doc `competitors` array (schema v1).
    """
    out: list[dict] = []
    for doc in db.collection(COMPETENCE_NEW_COLLECTION).stream():
        data = doc.to_dict() or {}
        competitors = _read_competitors(doc, data)
        out.append({"list_id": doc.id, "data": data, "competitors": competitors})

    logger.info(
        "competence_new: read %s lists, %s competitors",
        len(out), sum(len(r["competitors"]) for r in out),
    )
    return out


def _read_competitors(doc, data: dict) -> list[tuple[str, dict]]:
    list_id = doc.id
    sub = list(doc.reference.collection(COMPETITORS_SUBCOLLECTION).stream())
    if sub:
        return [
            (compose_competitor_source_id(list_id, snap.id), snap.to_dict() or {})
            for snap in sub
        ]
    # Legacy v1: competitors stored as an array on the parent doc. Synthesize a
    # stable id for each so bronze upserts stay idempotent across runs.
    return [
        (compose_competitor_source_id(list_id, competitor_doc_id(c)), c)
        for c in (data.get("competitors") or [])
    ]
