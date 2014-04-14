import unittest

import tinyquery


class TinyQueryTest(unittest.TestCase):
    def setUp(self):
        self.tq = tinyquery.TinyQuery()
        self.tq.load_table(tinyquery.Table(
            'test_table', {
                'val1': tinyquery.Column('int', [4, 1, 8, 1, 2]),
                'val2': tinyquery.Column('int', [8, 2, 4, 1, 6])
            })
        )

    def assert_query_result(self, query, expected_result):
        result = self.tq.evaluate_query(query)
        self.assertEqual(expected_result, result)

    def test_select_literal(self):
        self.assert_query_result(
            'SELECT 0',
            tinyquery.Table('query_result',
                            {'f0_': tinyquery.Column('int', [0])})
        )

    def test_simple_arithmetic(self):
        self.assert_query_result(
            'SELECT 1 + 2',
            tinyquery.Table('query_result',
                            {'f0_': tinyquery.Column('int', [3])})
        )

    def test_select_from_table(self):
        self.assert_query_result(
            'SELECT val1 FROM test_table',
            tinyquery.Table('query_result',
                            {'val1': tinyquery.Column('int', [4, 1, 8, 1, 2])})
        )

    def test_select_comparison(self):
        self.assert_query_result(
            'SELECT val1 = val2 FROM test_table',
            tinyquery.Table(
                'query_result',
                {'f0_': tinyquery.Column('bool',
                                         [False, False, False, True, False])})
        )
