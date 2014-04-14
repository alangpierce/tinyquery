"""The compiler step turns an AST into a planned query.

This step has a number of responsibilities:
-Validate that the expression is well-typed.
-Resolve all select fields to their aliases and types.
"""
import parser
import runtime
import tq_ast
import typed_ast
import tq_types


class CompileError(Exception):
    pass


def compile_text(text, tables_by_name):
    ast = parser.parse_text(text)
    return Compiler(tables_by_name).compile_select(ast)


class Compiler(object):
    def __init__(self, tables_by_name):
        self.tables_by_name = tables_by_name

    def compile_select(self, select):
        assert isinstance(select, tq_ast.Select)
        if select.table_expr:
            tables = self.compile_table_expr(select.table_expr)
        else:
            tables = []
        select_fields = [self.compile_select_field(field, tables)
                         for field in select.select_fields]
        if len(tables) > 0:
            table_result = tables[0].name
        else:
            table_result = None
        return typed_ast.Select(select_fields, table_result)

    def compile_table_expr(self, table_expr):
        """Given a table expression, return the tables referenced.

        Arguments:
            table_expr: A TableId (and, in the future, a more general table
                expression).

        Returns: A list of tables accessible to select fields.
        """
        if not isinstance(table_expr, tq_ast.TableId):
            raise NotImplementedError(
                'Only direct table access supported for now.')
        assert isinstance(table_expr, tq_ast.TableId), ''
        return [self.tables_by_name[table_expr.name]]

    def compile_select_field(self, select_field, tables):
        assert isinstance(select_field, tq_ast.SelectField)
        source_expr = select_field.expr
        compiled_expr = self.compile_expr(source_expr, tables)

        if isinstance(source_expr, tq_ast.ColumnId):
            alias = source_expr.name
        else:
            alias = 'f0_'

        return typed_ast.SelectField(compiled_expr, alias)

    def compile_expr(self, expr, tables):
        try:
            method = getattr(self, 'compile_' + expr.__class__.__name__)
        except AttributeError:
            raise NotImplementedError(
                'Missing handler for type {}'.format(expr.__class__.__name__))
        return method(expr, tables)

    def compile_ColumnId(self, expr, tables):
        matching_tables = []
        column_name = expr.name
        for table in tables:
            if column_name in table.columns:
                matching_tables.append(table)

        if len(matching_tables) == 0:
            raise CompileError('Field not found: {}'.format(column_name))
        if len(matching_tables) > 1:
            raise CompileError('Ambiguous field: {}'.format(column_name))
        table = matching_tables[0]
        return typed_ast.ColumnRef(table.name, column_name,
                                   table.columns[column_name].type)

    def compile_Literal(self, expr, tables):
        if isinstance(expr.value, int):
            return typed_ast.Literal(expr.value, tq_types.INT)
        else:
            raise NotImplementedError('Only int literals supported for now.')

    def compile_BinaryOperator(self, expr, tables):
        func = runtime.get_operator(expr.operator)

        compiled_left = self.compile_expr(expr.left, tables)
        compiled_right = self.compile_expr(expr.right, tables)

        result_type = func.check_types(compiled_left.type, compiled_right.type)

        return typed_ast.FunctionCall(
            func, [compiled_left, compiled_right], result_type)
