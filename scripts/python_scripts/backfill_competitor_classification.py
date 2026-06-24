"""
scripts/python_scripts/backfill_competitor_classification.py

One-off classification of silver.competence_competitors into flexible-workspace operators
(keep) vs. not, writing verdicts to silver.competence_competitor_classification. The scrape's
category_name is only ever 3 flex-ish values, so the website is the real signal; this dedupes
by domain (one verdict per operator, applied to all its locations) and uses Anthropic only on
the ambiguous middle. See shared/competence/classifier_service.py.

Run order:
  1. scripts/sql_scripts/competence_classification.sql           (creates the table + view)
  2. this script

Recommended sequence (validate cheaply, then scale — mirrors the project's gated approach):
  # 0. Free rules only, no AI, no writes — see how the rules split things
  python scripts/python_scripts/backfill_competitor_classification.py --rules-only --dry-run

  # 1. Tiny AI sample per category — eyeball the per-category keep/drop hit rate + samples
  python scripts/python_scripts/backfill_competitor_classification.py --sample-per-category 20 --dry-run

  # 2. A capped real run to write a first slice and sanity-check the table
  python scripts/python_scripts/backfill_competitor_classification.py --max-ai 200

  # 3. The full backfill (deduped by domain, so far fewer than 15k AI calls)
  python scripts/python_scripts/backfill_competitor_classification.py

Reads ANTHROPIC_API_KEY + the SQL connection from .env (same as the app).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from shared.competence.classifier_service import CompetitorClassifier  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description="Classify competitors as flexible-workspace operators.")
    p.add_argument("--dry-run", action="store_true", help="Classify but write nothing.")
    p.add_argument("--rules-only", action="store_true", help="Free rules only — no AI calls.")
    p.add_argument("--limit", type=int, default=None, help="Only load the first N competitor rows.")
    p.add_argument("--sample-per-category", type=int, default=None,
                   help="Only classify N units per category_name (quick validation).")
    p.add_argument("--max-ai", type=int, default=None, help="Cap the number of AI-classified units.")
    p.add_argument("--no-fetch", action="store_true", help="Skip the homepage-fetch escalation pass.")
    p.add_argument("--trust-coworking", action="store_true",
                   help="Auto-keep the 'Coworking space' category for free (enable once a sample shows it's clean).")
    p.add_argument("--batch", type=int, default=20, help="Units per AI call.")
    p.add_argument("--model", default=None, help="Override the LLM model id.")
    args = p.parse_args()

    kwargs = dict(
        batch_size=args.batch,
        trust_coworking_category=args.trust_coworking,
        max_ai_units=args.max_ai,
        fetch_websites=not args.no_fetch,
    )
    if args.model:
        kwargs["model"] = args.model
    classifier = CompetitorClassifier(**kwargs)

    report = classifier.classify(
        limit=args.limit,
        sample_per_category=args.sample_per_category,
        dry_run=args.dry_run,
        rules_only=args.rules_only,
    )

    d = report.as_dict()
    print(json.dumps({k: v for k, v in d.items() if k != "samples"}, indent=2, ensure_ascii=False))
    if report.samples:
        print("\nSample AI verdicts:")
        for s in report.samples:
            flag = {True: "KEEP", False: "drop", None: "unsure"}[s["is_flex"]]
            print(f"  [{flag:6}] {(s['title'] or '')[:40]:40}  {s['category'] or '':28}  {s['domain'] or ''}")

    tag = "DRY RUN — nothing written" if args.dry_run else f"wrote {report.rows_written} rows"
    print(
        f"\n{tag}: units={report.units} (rules keep={report.rules_keep} drop={report.rules_drop}; "
        f"ai keep={report.ai_keep} drop={report.ai_drop} unsure={report.ai_unsure}); "
        f"ai_calls={report.ai_calls} escalated={report.escalated} skipped_by_cap={report.ai_skipped_cap}"
    )


if __name__ == "__main__":
    main()
