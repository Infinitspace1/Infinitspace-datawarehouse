import json
import unittest

from shared.competence.classification import (
    Verdict,
    build_classification_messages,
    classification_input_hash,
    classify_by_rules,
    domain_of,
    parse_classification_response,
)


class RuleClassifierTests(unittest.TestCase):
    def test_drop_clearly_unrelated_categories(self):
        for cat in ("Restaurant", "Hotel", "Fitness center", "Hair salon", "Coffee shop", "Bank"):
            v = classify_by_rules("Some Place", cat)
            self.assertFalse(v.is_flex, cat)
            self.assertEqual(v.method, "rule:category")

    def test_flex_categories_are_undecided_by_default(self):
        # The 3 scrape categories are flex-ish but can't prove an operator -> defer to AI.
        for cat in ("Coworking space", "Office space rental agency", "Business center"):
            v = classify_by_rules("Some Place", cat)
            self.assertIsNone(v.is_flex, cat)
            self.assertFalse(v.decided)

    def test_trust_coworking_keeps_only_the_coworking_category(self):
        keep = classify_by_rules("X", "Coworking space", trust_coworking_category=True)
        self.assertTrue(keep.is_flex)
        self.assertEqual(keep.method, "rule:category")
        # The two noisy categories stay undecided even when coworking is trusted.
        for cat in ("Office space rental agency", "Business center"):
            self.assertIsNone(classify_by_rules("X", cat, trust_coworking_category=True).is_flex, cat)

    def test_drop_wins_even_when_trusting_coworking(self):
        v = classify_by_rules("X", "Restaurant", trust_coworking_category=True)
        self.assertFalse(v.is_flex)

    def test_empty_or_missing_category_is_undecided(self):
        for cat in ("", None):
            self.assertIsNone(classify_by_rules("Generic Name", cat).is_flex)


class DomainAndHashTests(unittest.TestCase):
    def test_domain_of_strips_scheme_and_www(self):
        self.assertEqual(domain_of("https://www.Spaces.com/amsterdam"), "spaces.com")
        self.assertEqual(domain_of("regus.com"), "regus.com")
        self.assertEqual(domain_of("http://office.example.co.uk:8080/x"), "office.example.co.uk")
        self.assertIsNone(domain_of(""))
        self.assertIsNone(domain_of(None))

    def test_input_hash_ignores_url_path_but_tracks_domain(self):
        a = classification_input_hash("Spaces", "Coworking space", "https://www.spaces.com/ams")
        b = classification_input_hash("Spaces", "Coworking space", "https://spaces.com/other")
        c = classification_input_hash("Spaces", "Coworking space", "https://regus.com")
        self.assertEqual(a, b)        # same domain, different path -> same hash
        self.assertNotEqual(a, c)     # different domain -> different hash

    def test_input_hash_is_case_insensitive_on_title_and_category(self):
        self.assertEqual(
            classification_input_hash("WeWork", "Coworking Space", "wework.com"),
            classification_input_hash("wework", "coworking space", "wework.com"),
        )


class PromptAndParseTests(unittest.TestCase):
    def test_prompt_includes_items_and_excerpt_only_when_requested(self):
        items = [{"id": "p1", "title": "Acme Offices", "category_name": "Office", "domain": "acme.com",
                  "website_excerpt": "Private offices and coworking memberships available."}]
        _sys, user_meta = build_classification_messages(items, with_website=False)
        self.assertIn("acme.com", user_meta)
        self.assertNotIn("website_excerpt", user_meta)
        _sys, user_web = build_classification_messages(items, with_website=True)
        self.assertIn("website_excerpt", user_web)
        self.assertIn("coworking memberships", user_web)

    def test_parse_decisions_and_unsure(self):
        text = (
            '[{"id":"p1","verdict":"yes","confidence":0.9},'
            '{"id":"p2","verdict":"no","confidence":0.8},'
            '{"id":"p3","verdict":"unsure","confidence":0.4}]'
        )
        out = parse_classification_response(text, ["p1", "p2", "p3"])
        self.assertTrue(out["p1"].is_flex)
        self.assertEqual(out["p1"].method, "ai:meta")
        self.assertEqual(out["p1"].confidence, 0.9)
        self.assertFalse(out["p2"].is_flex)
        self.assertIsNone(out["p3"].is_flex)        # unsure -> undecided
        self.assertIsNone(out["p3"].method)

    def test_parse_tolerates_code_fences_and_prose(self):
        text = "Here you go:\n```json\n[{\"id\":\"p1\",\"verdict\":\"yes\",\"confidence\":1}]\n```"
        out = parse_classification_response(text, ["p1"], method="ai:web")
        self.assertTrue(out["p1"].is_flex)
        self.assertEqual(out["p1"].method, "ai:web")

    def test_parse_ignores_unknown_ids(self):
        out = parse_classification_response('[{"id":"ghost","verdict":"yes"}]', ["p1"])
        self.assertEqual(out, {})

    def test_parse_raises_on_non_json(self):
        with self.assertRaises(ValueError):
            parse_classification_response("not json at all", ["p1"])


class DomainDedupTests(unittest.TestCase):
    """The cost lever: rows of the same operator (website host) collapse to one unit."""

    def _classifier(self):
        from shared.competence.classifier_service import CompetitorClassifier
        return CompetitorClassifier()

    def test_same_domain_collapses_to_one_unit(self):
        rows = [
            {"place_id": "a", "title": "Regus Amsterdam", "category_name": "Business center",
             "website": "https://www.regus.com/amsterdam"},
            {"place_id": "b", "title": "Regus Rotterdam", "category_name": "Business center",
             "website": "https://regus.com/rotterdam"},
            {"place_id": "c", "title": "Regus Berlin", "category_name": "Office space rental agency",
             "website": "http://www.regus.com/berlin"},
        ]
        units = self._classifier().build_units(rows)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]["domain"], "regus.com")
        self.assertEqual(len(units[0]["rows"]), 3)        # one AI call covers all 3 locations

    def test_rows_without_website_are_singletons(self):
        rows = [
            {"place_id": "a", "title": "No Site Coworking", "category_name": "Coworking space", "website": None},
            {"place_id": "b", "title": "Other", "category_name": "Coworking space", "website": "   "},
        ]
        units = self._classifier().build_units(rows)
        self.assertEqual(len(units), 2)
        self.assertTrue(all(u["domain"] is None and len(u["rows"]) == 1 for u in units))

    def test_distinct_domains_stay_separate(self):
        rows = [
            {"place_id": "a", "title": "A", "category_name": "Coworking space", "website": "a-cowork.nl"},
            {"place_id": "b", "title": "B", "category_name": "Coworking space", "website": "b-cowork.nl"},
        ]
        self.assertEqual(len(self._classifier().build_units(rows)), 2)


if __name__ == "__main__":
    unittest.main()
