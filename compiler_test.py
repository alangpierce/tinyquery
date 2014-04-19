import collections
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
                'table1', collections.OrderedDict([
                    ('value', tinyquery.Column('int', []))
                ])
            )
        }

    def assert_compiled_select(self, text, expected_ast):
        ast = compiler.compile_text(text, self.tables_by_name)
        self.assertEqual(expected_ast, ast)

    def assert_compile_error(self, text):
        self.assertRaises(compiler.CompileError, compiler.compile_text,
                          text, self.tables_by_name)

    def test_compile_simple_select(self):
        self.assert_compiled_select(
            'SELECT value FROM table1',
            typed_ast.Select(
                [typed_ast.SelectField(
                    typed_ast.ColumnRef('table1.value', tq_types.INT),
                    'value')],
                'table1',
                typed_ast.Literal(True, tq_types.BOOL))
        )

    def test_where(self):
        self.assert_compiled_select(
            'SELECT value FROM table1 WHERE value > 3',
            typed_ast.Select(
                [typed_ast.SelectField(
                    typed_ast.ColumnRef('table1.value', tq_types.INT),
                    'value')],
                'table1',
                typed_ast.FunctionCall(
                    runtime.get_operator('>'),
                    [typed_ast.ColumnRef('table1.value', tq_types.INT),
                     typed_ast.Literal(3, tq_types.INT)],
                    tq_types.BOOL)
            )
        )

    def test_multiple_select(self):
        self.assert_compiled_select(
            'SELECT value * 3 AS foo, value, value + 1, value bar, value - 1 '
            'FROM table1',
            typed_ast.Select(
                [typed_ast.SelectField(
                    typed_ast.FunctionCall(
                        runtime.get_operator('*'),
                        [typed_ast.ColumnRef('table1.value', tq_types.INT),
                         typed_ast.Literal(3, tq_types.INT)],
                        tq_types.INT),
                    'foo'),
                 typed_ast.SelectField(
                     typed_ast.ColumnRef('table1.value', tq_types.INT),
                     'value'),
                 typed_ast.SelectField(
                     typed_ast.FunctionCall(
                         runtime.get_operator('+'),
                         [typed_ast.ColumnRef('table1.value', tq_types.INT),
                          typed_ast.Literal(1, tq_types.INT)],
                         tq_types.INT),
                     'f0_'),
                 typed_ast.SelectField(
                     typed_ast.ColumnRef('table1.value', tq_types.INT),
                     'bar'),
                 typed_ast.SelectField(
                     typed_ast.FunctionCall(
                         runtime.get_operator('-'),
                         [typed_ast.ColumnRef('table1.value', tq_types.INT),
                          typed_ast.Literal(1, tq_types.INT)],
                         tq_types.INT),
                     'f1_')],
                'table1',
                typed_ast.Literal(True, tq_types.BOOL)
            )
        )

    def test_duplicate_aliases_not_allowed(self):
        self.assert_compile_error(
            'SELECT 0 AS foo, value foo FROM table1')
