import unittest

import compiler
import tinyquery
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
                    typed_ast.ColumnRef('table1', 'value', 'int'),
                    'value')],
                'table1')
        )
