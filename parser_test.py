import unittest

import tq_ast
import parser


def literal(value):
    return tq_ast.Literal(value)


class ParserTest(unittest.TestCase):
    def assert_parsed_select(self, text, expected_ast):
        actual_ast = parser.parse_text(text)
        self.assertEqual(expected_ast, actual_ast,
                         'Expected: %s, Actual %s.\nReprs: %r vs. %r.' %
                         (expected_ast, actual_ast, expected_ast, actual_ast))

    def test_arithmetic_operator_parsing(self):
        self.assert_parsed_select(
            'SELECT 1 * 2 + 3 / 4',
            tq_ast.Select([
                tq_ast.SelectField(
                    tq_ast.BinaryOperator(
                        '+',
                        tq_ast.BinaryOperator('*', literal(1), literal(2)),
                        tq_ast.BinaryOperator('/', literal(3), literal(4))),
                    None)],
                None,
                None,
                None,
                None))

    def test_select_from_table(self):
        self.assert_parsed_select(
            'SELECT foo FROM bar',
            tq_ast.Select(
                [tq_ast.SelectField(tq_ast.ColumnId('foo'), None)],
                tq_ast.TableId('bar', None),
                None,
                None,
                None
            ))

    def test_select_comparison(self):
        self.assert_parsed_select(
            'SELECT foo = bar FROM baz',
            tq_ast.Select(
                [tq_ast.SelectField(
                    tq_ast.BinaryOperator(
                        '=',
                        tq_ast.ColumnId('foo'),
                        tq_ast.ColumnId('bar')),
                    None)],
                tq_ast.TableId('baz', None),
                None,
                None,
                None
            )
        )

    def test_operator_precedence(self):
        self.assert_parsed_select(
            'SELECT 2 + 3 * 4 + 5',
            tq_ast.Select(
                [tq_ast.SelectField(
                    tq_ast.BinaryOperator(
                        '+',
                        tq_ast.BinaryOperator(
                            '+',
                            literal(2),
                            tq_ast.BinaryOperator(
                                '*', literal(3), literal(4))),
                        literal(5)),
                    None
                )],
                None,
                None,
                None,
                None
            )
        )

    def test_parens(self):
        self.assert_parsed_select(
            'SELECT 2 + (3 * 4)',
            tq_ast.Select(
                [tq_ast.SelectField(
                    tq_ast.BinaryOperator(
                        '+',
                        literal(2),
                        tq_ast.BinaryOperator('*', literal(3), literal(4))
                    ),
                    None
                )],
                None,
                None,
                None,
                None
            )
        )

    def test_negative_numbers(self):
        self.assert_parsed_select(
            'SELECT -5',
            tq_ast.Select(
                [tq_ast.SelectField(
                    tq_ast.UnaryOperator('-', literal(5)), None
                )],
                None,
                None,
                None,
                None
            ),
        )

    def test_function_calls(self):
        self.assert_parsed_select(
            'SELECT ABS(-3), POW(2, 3), NOW()',
            tq_ast.Select([
                tq_ast.SelectField(
                    tq_ast.FunctionCall('abs', [
                        tq_ast.UnaryOperator('-', literal(3))
                    ]),
                    None
                ),
                tq_ast.SelectField(
                    tq_ast.FunctionCall('pow', [literal(2), literal(3)]),
                    None
                ),
                tq_ast.SelectField(
                    tq_ast.FunctionCall('now', []),
                    None
                )],
                None,
                None,
                None,
                None
            )
        )

    def test_where(self):
        self.assert_parsed_select(
            'SELECT foo + 2 FROM bar WHERE foo > 3',
            tq_ast.Select(
                [tq_ast.SelectField(tq_ast.BinaryOperator(
                    '+',
                    tq_ast.ColumnId('foo'),
                    tq_ast.Literal(2)),
                    None)],
                tq_ast.TableId('bar', None),
                tq_ast.BinaryOperator(
                    '>',
                    tq_ast.ColumnId('foo'),
                    tq_ast.Literal(3)),
                None,
                None))

    def test_multiple_select(self):
        self.assert_parsed_select(
            'SELECT a AS foo, b bar, a + 1 baz FROM test_table',
            tq_ast.Select(
                [tq_ast.SelectField(tq_ast.ColumnId('a'), 'foo'),
                 tq_ast.SelectField(tq_ast.ColumnId('b'), 'bar'),
                 tq_ast.SelectField(
                     tq_ast.BinaryOperator(
                         '+',
                         tq_ast.ColumnId('a'),
                         tq_ast.Literal(1)),
                     'baz'
                 )],
                tq_ast.TableId('test_table', None),
                None,
                None,
                None
            )
        )

    def test_aggregates(self):
        self.assert_parsed_select(
            'SELECT MAX(foo) FROM bar',
            tq_ast.Select(
                [tq_ast.SelectField(
                    tq_ast.FunctionCall('max', [tq_ast.ColumnId('foo')]),
                    None
                )],
                tq_ast.TableId('bar', None),
                None,
                None,
                None
            )
        )

    def test_group_by(self):
        self.assert_parsed_select(
            'SELECT foo FROM bar GROUP BY baz',
            tq_ast.Select(
                [tq_ast.SelectField(tq_ast.ColumnId('foo'), None)],
                tq_ast.TableId('bar', None),
                None,
                ['baz'],
                None
            )
        )

    def test_multiple_table_select(self):
        self.assert_parsed_select(
            'SELECT foo FROM table1, table2',
            tq_ast.Select(
                [tq_ast.SelectField(tq_ast.ColumnId('foo'), None)],
                tq_ast.TableUnion([
                    tq_ast.TableId('table1', None),
                    tq_ast.TableId('table2', None),
                ]),
                None,
                None,
                None
            )
        )

    def test_subquery(self):
        self.assert_parsed_select(
            'SELECT foo FROM (SELECT val AS foo FROM table)',
            tq_ast.Select(
                [tq_ast.SelectField(tq_ast.ColumnId('foo'), None)],
                tq_ast.Select(
                    [tq_ast.SelectField(tq_ast.ColumnId('val'), 'foo')],
                    tq_ast.TableId('table', None),
                    None,
                    None,
                    None
                ),
                None,
                None,
                None
            )
        )

    def test_parenthesized_union_not_allowed(self):
        self.assertRaises(SyntaxError, parser.parse_text,
                          'SELECT foo FROM (table1, table2)')

    def test_fully_qualified_name(self):
        self.assert_parsed_select(
            'SELECT table.foo FROM table',
            tq_ast.Select(
                [tq_ast.SelectField(tq_ast.ColumnId('table.foo'), None)],
                tq_ast.TableId('table', None),
                None,
                None,
                None
            )
        )

    def test_join(self):
        self.assert_parsed_select(
            'SELECT t1.foo, t2.bar '
            'FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id',
            tq_ast.Select([
                tq_ast.SelectField(tq_ast.ColumnId('t1.foo'), None),
                tq_ast.SelectField(tq_ast.ColumnId('t2.bar'), None)],
                tq_ast.Join(
                    tq_ast.TableId('table1', 't1'),
                    tq_ast.TableId('table2', 't2'),
                    tq_ast.BinaryOperator(
                        '=',
                        tq_ast.ColumnId('t1.id'),
                        tq_ast.ColumnId('t2.id')
                    )
                ),
                None,
                None,
                None
            )
        )
