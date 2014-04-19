import collections
import unittest

import tinyquery
import tq_types


class TinyQueryTest(unittest.TestCase):
    def setUp(self):
        self.tq = tinyquery.TinyQuery()
        self.tq.load_table(tinyquery.Table(
            'test_table',
            collections.OrderedDict([
                ('val1', tinyquery.Column('int', [4, 1, 8, 1, 2])),
                ('val2', tinyquery.Column('int', [8, 2, 4, 1, 6]))
            ])))

    def assert_query_result(self, query, expected_result):
        result = self.tq.evaluate_query(query)
        self.assertEqual(expected_result, result)

    def test_select_literal(self):
        self.assert_query_result(
            'SELECT 0',
            tinyquery.Table(
                'query_result',
                collections.OrderedDict([
                    ('f0_', tinyquery.Column(tq_types.INT, [0]))
                ])
            )
        )

    def test_simple_arithmetic(self):
        self.assert_query_result(
            'SELECT 1 + 2',
            tinyquery.Table(
                'query_result',
                collections.OrderedDict([
                    ('f0_', tinyquery.Column(tq_types.INT, [3]))
                ])
            )
        )

    def test_select_from_table(self):
        self.assert_query_result(
            'SELECT val1 FROM test_table',
            tinyquery.Table(
                'query_result',
                collections.OrderedDict([
                    ('val1', tinyquery.Column(tq_types.INT, [4, 1, 8, 1, 2]))
                ])
            )
        )

    def test_select_comparison(self):
        self.assert_query_result(
            'SELECT val1 = val2 FROM test_table',
            tinyquery.Table(
                'query_result',
                collections.OrderedDict([
                    ('f0_', tinyquery.Column(
                        tq_types.BOOL, [False, False, False, True, False]))
                ])
            )
        )

    def test_where(self):
        self.assert_query_result(
            'SELECT val1 + 2 FROM test_table WHERE val2 > 3',
            tinyquery.Table(
                'query_result',
                collections.OrderedDict([
                    ('f0_', tinyquery.Column(tq_types.INT, [6, 10, 4]))
                ])
            )
        )

    def test_multiple_select(self):
        self.assert_query_result(
            'SELECT val1 + 1 foo, val2, val2 * 2'
            'FROM test_table WHERE val1 < 5',
            tinyquery.Table(
                'query_result',
                collections.OrderedDict([
                    ('foo', tinyquery.Column(tq_types.INT, [5, 2, 2, 3])),
                    ('val2', tinyquery.Column(tq_types.INT, [8, 2, 1, 6])),
                    ('f0_', tinyquery.Column(tq_types.INT, [16, 4, 2, 12]))
                ])
            )
        )
