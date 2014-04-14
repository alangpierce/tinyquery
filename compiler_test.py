import unittest

import compiler
import runtime
import tinyquery
import tq_types
import typed_ast


class CompilerTest(unittest.TestCase):
    def setUp(self):
        self.tables_by_name = {
            'table1': tinyquery.Table(
                'table1', {
                'value': tinyquery.Column('int', [])
                }
            )
        }

    def assert_compiled_expr(self, text, expected_ast):
        ast = compiler.compile_text(text, self.tables_by_name)
        self.assertEqual(expected_ast, ast)

    def test_compile_simple_select(self):
        self.assert_compiled_expr(
            'SELECT value FROM table1',
            typed_ast.Select(
                [typed_ast.SelectField(
                    typed_ast.ColumnRef('table1', 'value', tq_types.INT),
                    'value')],
                'table1',
                typed_ast.Literal(True, tq_types.BOOL))
        )

    def test_where(self):
        self.assert_compiled_expr(
            'SELECT value FROM table1 WHERE value > 3',
            typed_ast.Select(
                [typed_ast.SelectField(
                    typed_ast.ColumnRef('table1', 'value', tq_types.INT),
                    'value')],
                'table1',
                typed_ast.FunctionCall(
                    runtime.get_operator('>'),
                    [typed_ast.ColumnRef('table1', 'value', tq_types.INT),
                     typed_ast.Literal(3, tq_types.INT)],
                    tq_types.BOOL)
            )
        )
