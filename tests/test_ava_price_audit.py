import unittest

from functions.ava_refresh import (
    _fetch_duplicate_desk_prices,
    _group_duplicate_rows,
    _render_duplicate_price_html,
)


class _StubSQL:
    def __init__(self, rows):
        self._rows = rows
        self.last_query = None
        self.last_params = None

    def execute_query(self, query, params=None):
        self.last_query = query
        self.last_params = params
        return self._rows


class TestDuplicatePriceRender(unittest.TestCase):

    def test_clean_table_renders_empty(self):
        self.assertEqual(_render_duplicate_price_html([]), "")

    def test_groups_by_location_and_category(self):
        rows = [
            {"location_name": "London - Aldgate - 2 Leman Street", "item_category": "hot_desk",
             "currency_code": "GBP", "price": 35, "product_source_id": 1, "item_name": "Hot Desk PT"},
            {"location_name": "London - Aldgate - 2 Leman Street", "item_category": "hot_desk",
             "currency_code": "GBP", "price": 195, "product_source_id": 2, "item_name": "Hot Desk Monthly"},
        ]
        groups = _group_duplicate_rows(rows)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]["items"]), 2)

    def test_html_contains_prices_ids_and_category(self):
        rows = [
            {"location_name": "Amsterdam - Noord - Papaverhof 59", "item_category": "dedicated_desk",
             "currency_code": "EUR", "price": 200, "product_source_id": 111, "item_name": "DD A"},
            {"location_name": "Amsterdam - Noord - Papaverhof 59", "item_category": "dedicated_desk",
             "currency_code": "EUR", "price": 400, "product_source_id": 222, "item_name": "DD B"},
        ]
        body = _render_duplicate_price_html(rows)
        self.assertIn("€200", body)
        self.assertIn("€400", body)
        self.assertIn("111", body)
        self.assertIn("222", body)
        self.assertIn("dedicated_desk", body)

    def test_two_distinct_groups(self):
        rows = [
            {"location_name": "Loc A", "item_category": "hot_desk",
             "currency_code": "GBP", "price": 10, "product_source_id": 1, "item_name": "x"},
            {"location_name": "Loc A", "item_category": "hot_desk",
             "currency_code": "GBP", "price": 20, "product_source_id": 2, "item_name": "y"},
            {"location_name": "Loc B", "item_category": "dedicated_desk",
             "currency_code": "EUR", "price": 30, "product_source_id": 3, "item_name": "z"},
            {"location_name": "Loc B", "item_category": "dedicated_desk",
             "currency_code": "EUR", "price": 40, "product_source_id": 4, "item_name": "w"},
        ]
        self.assertEqual(len(_group_duplicate_rows(rows)), 2)


class TestFetchQuery(unittest.TestCase):

    def test_query_filters_to_desk_categories_and_passes_params(self):
        stub = _StubSQL([])
        _fetch_duplicate_desk_prices(stub)
        # Both the inner HAVING subquery and the outer WHERE filter to the two
        # single-price categories → category list passed twice.
        self.assertEqual(
            stub.last_params,
            ("hot_desk", "dedicated_desk", "hot_desk", "dedicated_desk"),
        )
        self.assertIn("COUNT(DISTINCT price) > 1", stub.last_query)


if __name__ == "__main__":
    unittest.main()
