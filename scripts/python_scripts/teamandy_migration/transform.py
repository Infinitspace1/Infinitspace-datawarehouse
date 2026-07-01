"""Phase 2 — TRANSFORM. NDJSON -> per-table row files (_work/sql/<table>.ndjson).
Manifest-driven, with named hooks for irregular shapes. Coercion failures are
logged to rejects and NULLed; a bad row never aborts the batch.

  python -m migration.etl.transform
"""
from __future__ import annotations
import sys, pathlib
from . import common, manifest

SQL_DIR = common.WORK / "sql"
SQL_DIR.mkdir(parents=True, exist_ok=True)

COERCE = {"s": common.to_str, "i": common.to_int, "fl": common.to_float,
          "b": common.to_bool, "dt": common.to_dt, "js": common.to_json}


class Emitter:
    def __init__(self):
        self._files = {}
        self._counts = {}

    def emit(self, table, row):
        f = self._files.get(table)
        if f is None:
            f = open(SQL_DIR / f"{table}.ndjson", "w", encoding="utf-8")
            self._files[table] = f
            self._counts[table] = 0
        f.write(common.dumps(row) + "\n")
        self._counts[table] += 1

    def close(self):
        for f in self._files.values():
            f.close()
        return self._counts


def coerce(spec, doc, table):
    field, c = spec if isinstance(spec, tuple) else (spec, "s")
    val = doc.get(field)
    try:
        return COERCE[c](val)
    except Exception as e:                       # only to_dt raises
        common.reject(table, {field: val}, f"{c}_coerce_failed: {e}")
        return None


def build_row(coll: manifest.Coll, doc):
    pk_col, pk_src = coll.pk
    pk_val = doc.get("__id") if pk_src == "__id" else doc.get(pk_src)
    row = {pk_col: pk_val}
    for col, spec in coll.cols.items():
        row[col] = coerce(spec, doc, coll.table)
    return pk_val, row


def emit_children(coll: manifest.Coll, doc, pk_val, em: Emitter):
    for ch in coll.children:
        items = doc.get(ch.source) or []
        if ch.kind == "scalar_set":
            seen = set()
            for el in items:
                if el in (None, "") or el in seen:
                    continue
                seen.add(el)
                em.emit(ch.table, {ch.parent_fk: pk_val, ch.element_col: common.to_str(el)})
        else:  # object
            for el in items:
                if not isinstance(el, dict):
                    continue
                crow = {ch.parent_fk: pk_val}
                if ch.pk:
                    cpk_col, cpk_field = ch.pk
                    crow[cpk_col] = el.get(cpk_field) or common.dumps(el)[:64]
                for col, spec in ch.cols.items():
                    crow[col] = coerce(spec, el, ch.table)
                em.emit(ch.table, crow)


# ---------------------------------------------------------------- hooks
def _hook_lead_contact_persons(coll, doc, pk_val, em):
    labels = [("contactPersons", "contact"), ("enrichedContactPersons", "enriched"),
              ("selectedContactPersons", "selected")]
    for field, label in labels:
        for el in (doc.get(field) or []):
            if not isinstance(el, dict):
                continue
            bi = el.get("basic_info") or {}
            g = lambda k: el.get(k) if el.get(k) is not None else bi.get(k)
            em.emit("lead_contact_persons", {
                "lead_uid": pk_val, "source_array": label,
                "contact_id": common.to_str(g("contactId")), "first_name": common.to_str(g("firstName")),
                "last_name": common.to_str(g("lastName")), "job_title": common.to_str(g("jobTitle")),
                "normalized_job_title": common.to_str(g("normalizedJobTitle")), "email": common.to_str(g("email")),
                "business_phone": common.to_str(g("businessPhone")), "mobile_phone": common.to_str(g("mobilePhone")),
                "linkedin_profile": common.to_str(g("linkedinProfile") or g("linkedinUrl")),
                "decision_maker": common.to_bool(g("decisionMaker")),
                "preferred_contact": common.to_str(g("preferredContact")),
                "recent_posts_json": common.to_json(g("recentPosts")), "interests_json": common.to_json(g("interests")),
                "pain_points_json": common.to_json(g("painPoints")),
                "personality_traits_json": common.to_json(g("personalityTraits")),
                "background_json": common.to_json(g("background")), "raw_json": common.to_json(el),
            })


def _hook_lead_lists_statistics(coll, doc, pk_val, em):
    st = doc.get("statistics") or {}
    em.emit("lead_list_statistics", {
        "lead_list_uid": pk_val,
        "warmup_exported": common.to_int(st.get("warmup_exported")) or 0,
        "synthetic_warmup_exported": common.to_int(st.get("synthetic_warmup_exported")) or 0,
        "hubspot_exported": common.to_int(st.get("hubspot_exported")) or 0,
        "total_leads": common.to_int(st.get("total_leads")),
        "last_updated": None,
    })


def _hook_org_ids_apollo(coll, doc, pk_val, em):
    for k, v in doc.items():
        if k.startswith("__"):
            continue
        em.emit("organization_ids_apollo",
                {"industry_name": k, "apollo_id": common.to_str(v), "source_doc_id": doc.get("__id")})


def _hook_competence_legacy(coll, doc, pk_val, em):
    loc = doc.get("location") or {}
    em.emit("competence", {
        "uid": doc.get("__id"),
        "location_city": common.to_str(loc.get("city")), "location_country": common.to_str(loc.get("country")),
        "location_json": common.to_json(loc), "list_competence_json": common.to_json(doc.get("list_competence")),
        "updated_at": None,
    })


def _hook_settings_broker_firms(coll, doc, pk_val, em):
    sid = doc.get("__id")
    em.emit("settings", {"setting_id": sid, "payload_json": common.to_json(
        {k: v for k, v in doc.items() if not k.startswith("__") and k != "names"}) or None})
    if sid == "broker_firms" or "names" in doc:
        seen = set()
        for nm in (doc.get("names") or []):
            if nm and nm not in seen:
                seen.add(nm)
                em.emit("settings_broker_firms", {"setting_id": sid, "name": common.to_str(nm)})


def _hook_acl_users(coll, doc, pk_val, em):
    for u in (doc.get("users") or []):
        if not isinstance(u, dict):
            continue
        email = (u.get("email") or "").strip().lower()
        if not email:
            continue
        em.emit("acl_users", {"email": email, "has_access": common.to_bool(u.get("access")) if u.get("access") is not None else True,
                              "is_admin": common.to_bool(u.get("admin")) or False})


def _hook_deleted_tombstones(coll, doc, pk_val, em):
    src = doc.get("__id")
    for eid in (doc.get("Ids") or []):
        if eid:
            em.emit("deleted_tombstones", {"source_doc_id": src, "deleted_entity_id": common.to_str(eid)})


HOOKS = {
    "lead_contact_persons": _hook_lead_contact_persons,
    "lead_lists_statistics": _hook_lead_lists_statistics,
    "org_ids_apollo_pivot": _hook_org_ids_apollo,
    "competence_legacy": _hook_competence_legacy,
    "settings_broker_firms": _hook_settings_broker_firms,
    "acl_users": _hook_acl_users,
    "deleted_tombstones": _hook_deleted_tombstones,
}


def transform():
    em = Emitter()
    for coll in manifest.COLLECTIONS:
        if coll.target != "sql" or coll.name in manifest.EXCLUDE_COLLECTIONS:
            continue
        src = common.NDJSON_DIR / f"{coll.name}.ndjson"
        if not src.exists():
            print(f"  (skip {coll.name}: no ndjson — not extracted?)")
            continue
        n = 0
        for doc in common.read_ndjson(src):
            n += 1
            if coll.hook in HOOKS and not coll.cols:
                # whole-doc hook (org_ids pivot, competence legacy, settings, acl, deleted)
                HOOKS[coll.hook](coll, doc, doc.get("__id"), em)
                continue
            pk_val, row = build_row(coll, doc)
            em.emit(coll.table, row)
            emit_children(coll, doc, pk_val, em)
            if coll.hook in HOOKS:
                HOOKS[coll.hook](coll, doc, pk_val, em)
        print(f"  {coll.name:32} {n} docs -> {coll.table}")
    counts = em.close()
    print("\nTransformed row counts per table:")
    for t in manifest.TABLE_LOAD_ORDER:
        if t in counts:
            print(f"  {t:34} {counts[t]}")
    return counts


if __name__ == "__main__":
    transform()
