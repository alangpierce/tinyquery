import collections
import mock
import unittest

import tinyquery
import tq_types


class TinyQueryTest(unittest.TestCase):
    def setUp(self):
        self.tq = tinyquery.TinyQuery()
        self.tq.load_table(tinyquery.Table(
            'test_table',
            5,
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
                1,
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
                1,
                collections.OrderedDict([
                    ('f0_', tinyquery.Column(tq_types.INT, [3]))
                ])
            )
        )

    def test_precedence(self):
        self.assert_query_result(
            'SELECT 2 * (3 + 1) + 2 * 3',
            tinyquery.Table(
                'query_result',
                1,
                collections.OrderedDict([
                    ('f0_', tinyquery.Column(tq_types.INT, [14]))
                ])
            )
        )

    def test_negative_number(self):
        self.assert_query_result(
            'SELECT -3',
            tinyquery.Table(
                'query_result',
                1,
                collections.OrderedDict([
                    ('f0_', tinyquery.Column(tq_types.INT, [-3]))
                ])
            )
        )

    def test_function_calls(self):
        with mock.patch('time.time', lambda: 15):
            self.assert_query_result(
                'SELECT ABS(-2), POW(2, 3), NOW()',
                tinyquery.Table(
                    'query_result',
                    1,
                    collections.OrderedDict([
                        ('f0_', tinyquery.Column(tq_types.INT, [2])),
                        ('f1_', tinyquery.Column(tq_types.INT, [8])),
                        ('f2_', tinyquery.Column(tq_types.INT, [15000000])),
                    ])
                )
            )

    def test_select_from_table(self):
        self.assert_query_result(
            'SELECT val1 FROM test_table',
            tinyquery.Table(
                'query_result',
                5,
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
                5,
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
                3,
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
                4,
                collections.OrderedDict([
                    ('foo', tinyquery.Column(tq_types.INT, [5, 2, 2, 3])),
                    ('val2', tinyquery.Column(tq_types.INT, [8, 2, 1, 6])),
                    ('f0_', tinyquery.Column(tq_types.INT, [16, 4, 2, 12]))
                ])
            )
        )

    def test_simple_aggregate(self):
        self.assert_query_result(
            'SELECT SUM(val1) + MIN(val2) FROM test_table',
            tinyquery.Table(
                'query_result',
                1,
                collections.OrderedDict([
                    ('f0_', tinyquery.Column(tq_types.INT, [17]))
                ])
            )
        )

    def test_aggregate_evaluation(self):
        self.assert_query_result(
            'SELECT 2 * SUM(val1 + 1) FROM test_table WHERE val1 < 5',
            tinyquery.Table(
                'query_result',
                1,
                collections.OrderedDict([
                    ('f0_', tinyquery.Column(tq_types.INT, [24]))
                ])
            )
        )

    def test_group_by_field(self):
        result = self.tq.evaluate_query(
            'SELECT SUM(val2) FROM test_table GROUP BY val1')
        self.assertEqual([3, 4, 6, 8], sorted(result.columns['f0_'].values))

    def test_group_by_used_field(self):
        result = self.tq.evaluate_query(
            'SELECT val1 + SUM(val2) FROM test_table GROUP BY val1')
        self.assertEqual([4, 8, 12, 12], sorted(result.columns['f0_'].values))
