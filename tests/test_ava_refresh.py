import unittest

from functions.ava_refresh import _assert_ava_objects_exist


class _StubSQL:
    def __init__(self, results):
        self._results = list(results)
        self.queries = []

    def execute_scalar(self, query):
        self.queries.append(query)
        return self._results.pop(0)


class TestAvaRefreshPreflight(unittest.TestCase):

    def test_accepts_present_table_and_procedure(self):
        sql = _StubSQL([1, 1])
        _assert_ava_objects_exist(sql)
        self.assertEqual(len(sql.queries), 2)

    def test_raises_clear_message_when_table_is_missing(self):
        sql = _StubSQL([0, 1])

        with self.assertRaises(RuntimeError) as ctx:
            _assert_ava_objects_exist(sql)

        self.assertIn("ava.product_availability", str(ctx.exception))

    def test_raises_clear_message_when_procedure_is_missing(self):
        sql = _StubSQL([1, 0])

        with self.assertRaises(RuntimeError) as ctx:
            _assert_ava_objects_exist(sql)

        self.assertIn("ava.sp_refresh_product_availability", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
