import unittest

import tq_ast
import parser


def literal(value):
    return tq_ast.Literal(value)


class ParserTest(unittest.TestCase):
    def test_arithmetic_operator_parsing(self):
        self.assert_parsed_select(
            'SELECT 1 * 2 + 3 / 4',
            tq_ast.Select(
                tq_ast.SelectField(tq_ast.BinaryOperator(
                    '+',
                    tq_ast.BinaryOperator('*', literal(1), literal(2)),
                    tq_ast.BinaryOperator('/', literal(3), literal(4)))),
                None))

    def test_select_from_table(self):
        self.assert_parsed_select(
            'SELECT foo FROM bar',
            tq_ast.Select(
                tq_ast.SelectField(tq_ast.ColumnId('foo')),
                tq_ast.TableId('bar')
            ))

    def assert_parsed_select(self, text, expected_ast):
        actual_ast = parser.parse_text(text)
        self.assertEqual(expected_ast, actual_ast,
                         'Expected: %s, Actual %s' % (expected_ast, actual_ast))
