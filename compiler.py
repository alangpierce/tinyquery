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
        aliases = self.get_aliases(select.select_fields)
        select_fields = [
            self.compile_select_field(field.expr, alias, tables)
            for field, alias in zip(select.select_fields, aliases)]
        if len(tables) > 0:
            table_result = tables[0].name
        else:
            table_result = None

        if select.where_expr:
            where_expr = self.compile_expr(select.where_expr, tables)
        else:
            where_expr = typed_ast.Literal(True, tq_types.BOOL)
        return typed_ast.Select(select_fields, table_result, where_expr)

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

    def compile_select_field(self, expr, alias, tables):
        compiled_expr = self.compile_expr(expr, tables)
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
        return typed_ast.ColumnRef(table.name + '.' + column_name,
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

    @classmethod
    def get_aliases(cls, select_field_list):
        """Given a list of tq_ast.SelectField, return the aliases to use."""
        used_aliases = set()
        proposed_aliases = [cls.field_alias(select_field)
                            for select_field in select_field_list]
        for alias in proposed_aliases:
            if alias is not None:
                if alias in used_aliases:
                    raise CompileError(
                        'Ambiguous column name {}.'.format(alias))
                used_aliases.add(alias)

        generic_field_num = 0
        result = []
        for alias in proposed_aliases:
            if alias is not None:
                result.append(alias)
            else:
                while ('f%s_' % generic_field_num) in used_aliases:
                    generic_field_num += 1
                result.append('f%s_' % generic_field_num)
                generic_field_num += 1
        return result

    @staticmethod
    def field_alias(select_field):
        """Gets the alias to use, or None if it's not specified."""
        if select_field.alias is not None:
            return select_field.alias
        if isinstance(select_field.expr, tq_ast.ColumnId):
            return select_field.expr.name
        return None
