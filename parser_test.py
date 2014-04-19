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
                None))

    def test_select_from_table(self):
        self.assert_parsed_select(
            'SELECT foo FROM bar',
            tq_ast.Select(
                [tq_ast.SelectField(tq_ast.ColumnId('foo'), None)],
                tq_ast.TableId('bar'),
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
                tq_ast.TableId('baz'),
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
                tq_ast.TableId('bar'),
                tq_ast.BinaryOperator(
                    '>',
                    tq_ast.ColumnId('foo'),
                    tq_ast.Literal(3)),
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
                tq_ast.TableId('test_table'),
                None,
                None
            )
        )

    def test_group_by(self):
        self.assert_parsed_select(
            'SELECT foo FROM bar GROUP BY baz',
            tq_ast.Select(
                [tq_ast.SelectField(tq_ast.ColumnId('foo'), None)],
                tq_ast.TableId('bar'),
                None,
                ['baz']
            )
        )
