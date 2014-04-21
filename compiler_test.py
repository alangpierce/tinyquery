import collections
import unittest

import compiler
import runtime
import tinyquery
import tq_types
import typed_ast


class CompilerTest(unittest.TestCase):
    def setUp(self):
        self.table1 = tinyquery.Table(
            'table1',
            0,
            collections.OrderedDict([
                ('value', tinyquery.Column(tq_types.INT, []))
            ]))
        self.tables_by_name = {
            'table1': self.table1
        }

        self.table1_type_ctx = typed_ast.TypeContext(
            collections.OrderedDict([('table1.value', tq_types.INT)]),
            {'value': 'table1.value'},
            [],
            None)

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
                typed_ast.Table('table1', self.table1_type_ctx),
                typed_ast.Literal(True, tq_types.BOOL),
                None)
        )

    def test_unary_operator(self):
        self.assert_compiled_select(
            'SELECT -5',
            typed_ast.Select(
                [typed_ast.SelectField(
                    typed_ast.FunctionCall(
                        runtime.get_unary_op('-'),
                        [typed_ast.Literal(5, tq_types.INT)],
                        tq_types.INT),
                    'f0_'
                )],
                typed_ast.NoTable(),
                typed_ast.Literal(True, tq_types.BOOL),
                None
            )
        )

    def test_function_calls(self):
        self.assert_compiled_select(
            'SELECT ABS(-3), POW(2, 3), NOW()',
            typed_ast.Select([
                typed_ast.SelectField(
                    typed_ast.FunctionCall(
                        runtime.get_func('abs'),
                        [typed_ast.FunctionCall(
                            runtime.get_unary_op('-'),
                            [typed_ast.Literal(3, tq_types.INT)],
                            tq_types.INT
                        )],
                        tq_types.INT),
                    'f0_'),
                typed_ast.SelectField(
                    typed_ast.FunctionCall(
                        runtime.get_func('pow'), [
                            typed_ast.Literal(2, tq_types.INT),
                            typed_ast.Literal(3, tq_types.INT)],
                        tq_types.INT
                    ),
                    'f1_'
                ),
                typed_ast.SelectField(
                    typed_ast.FunctionCall(
                        runtime.get_func('now'), [], tq_types.INT
                    ),
                    'f2_'
                )],
                typed_ast.NoTable(),
                typed_ast.Literal(True, tq_types.BOOL),
                None
            )
        )

    def test_where(self):
        self.assert_compiled_select(
            'SELECT value FROM table1 WHERE value > 3',
            typed_ast.Select(
                [typed_ast.SelectField(
                    typed_ast.ColumnRef('table1.value', tq_types.INT),
                    'value')],
                typed_ast.Table('table1', self.table1_type_ctx),
                typed_ast.FunctionCall(
                    runtime.get_binary_op('>'),
                    [typed_ast.ColumnRef('table1.value', tq_types.INT),
                     typed_ast.Literal(3, tq_types.INT)],
                    tq_types.BOOL),
                None
            )
        )

    def test_multiple_select(self):
        self.assert_compiled_select(
            'SELECT value * 3 AS foo, value, value + 1, value bar, value - 1 '
            'FROM table1',
            typed_ast.Select(
                [typed_ast.SelectField(
                    typed_ast.FunctionCall(
                        runtime.get_binary_op('*'),
                        [typed_ast.ColumnRef('table1.value', tq_types.INT),
                         typed_ast.Literal(3, tq_types.INT)],
                        tq_types.INT),
                    'foo'),
                 typed_ast.SelectField(
                     typed_ast.ColumnRef('table1.value', tq_types.INT),
                     'value'),
                 typed_ast.SelectField(
                     typed_ast.FunctionCall(
                         runtime.get_binary_op('+'),
                         [typed_ast.ColumnRef('table1.value', tq_types.INT),
                          typed_ast.Literal(1, tq_types.INT)],
                         tq_types.INT),
                     'f0_'),
                 typed_ast.SelectField(
                     typed_ast.ColumnRef('table1.value', tq_types.INT),
                     'bar'),
                 typed_ast.SelectField(
                     typed_ast.FunctionCall(
                         runtime.get_binary_op('-'),
                         [typed_ast.ColumnRef('table1.value', tq_types.INT),
                          typed_ast.Literal(1, tq_types.INT)],
                         tq_types.INT),
                     'f1_')],
                typed_ast.Table('table1', self.table1_type_ctx),
                typed_ast.Literal(True, tq_types.BOOL),
                None
            )
        )

    def test_duplicate_aliases_not_allowed(self):
        self.assert_compile_error(
            'SELECT 0 AS foo, value foo FROM table1')

    def test_aggregates(self):
        self.assert_compiled_select(
            'SELECT MAX(value), MIN(value) FROM table1',
            typed_ast.Select([
                typed_ast.SelectField(
                    typed_ast.AggregateFunctionCall(
                        runtime.get_func('max'),
                        [typed_ast.ColumnRef('table1.value', tq_types.INT)],
                        tq_types.INT
                    ),
                    'f0_'),
                typed_ast.SelectField(
                    typed_ast.AggregateFunctionCall(
                        runtime.get_func('min'),
                        [typed_ast.ColumnRef('table1.value', tq_types.INT)],
                        tq_types.INT
                    ),
                    'f1_')],
                typed_ast.Table('table1', self.table1_type_ctx),
                typed_ast.Literal(True, tq_types.BOOL),
                []
            )
        )

    def mixed_aggregate_non_aggregate_not_allowed(self):
        self.assert_compile_error(
            'SELECT value, SUM(value) FROM table1')

    def mixed_aggregate_non_aggregate_single_field_not_allowed(self):
        self.assert_compile_error(
            'SELECT value + SUM(value) FROM table1')
