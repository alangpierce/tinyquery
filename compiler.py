"""The compiler step turns an AST into a planned query.

This step has a number of responsibilities:
-Validate that the expression is well-typed.
-Resolve all select fields to their aliases and types.
"""
import collections

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
        table_expr = self.compile_table_expr(select.table_expr)
        table_ctx = table_expr.type_ctx

        is_aggregate_select = any(
            self.expression_contains_aggregate(field.expr)
            for field in select.select_fields)

        # If this is a regular aggregate select (without a GROUP BY), we
        # effectively group by nothing, and we need to set up the type context
        # so that the table fields are only available inside aggregates.
        if is_aggregate_select:
            groups = []
            select_field_ctx = typed_ast.TypeContext(
                collections.OrderedDict(), {}, set(), table_ctx)
        else:
            groups = None
            select_field_ctx = table_ctx

        aliases = self.get_aliases(select.select_fields)
        select_fields = [
            self.compile_select_field(field.expr, alias, select_field_ctx)
            for field, alias in zip(select.select_fields, aliases)]

        if select.where_expr:
            where_expr = self.compile_expr(select.where_expr, table_ctx)
        else:
            where_expr = typed_ast.Literal(True, tq_types.BOOL)

        return typed_ast.Select(select_fields, table_expr, where_expr, groups)

    def compile_table_expr(self, table_expr):
        """Compile a table expression and determine its result type context.

        Arguments:
            table_expr: Either None (indicating that there no table being
                selected or a TableId.

        Returns: A typed_ast.TableExpression.
        """
        if table_expr is None:
            return typed_ast.NoTable()
        else:
            assert isinstance(table_expr, tq_ast.TableId)
            table_name = table_expr.name
            table = self.tables_by_name[table_expr.name]

            columns = collections.OrderedDict()
            aliases = {}
            for name, column in table.columns.iteritems():
                full_name = table_name + '.' + name
                columns[full_name] = column.type
                aliases[name] = full_name

            type_context = typed_ast.TypeContext(columns, aliases, [], None)
            return typed_ast.Table(table_name, type_context)

    def compile_select_field(self, expr, alias, type_ctx):
        compiled_expr = self.compile_expr(expr, type_ctx)
        return typed_ast.SelectField(compiled_expr, alias)

    def compile_expr(self, expr, type_ctx):
        try:
            method = getattr(self, 'compile_' + expr.__class__.__name__)
        except AttributeError:
            raise NotImplementedError(
                'Missing handler for type {}'.format(expr.__class__.__name__))
        return method(expr, type_ctx)

    def compile_ColumnId(self, expr, type_ctx):
        return type_ctx.column_ref_for_name(expr.name)

    def compile_Literal(self, expr, type_ctx):
        if isinstance(expr.value, int):
            return typed_ast.Literal(expr.value, tq_types.INT)
        else:
            raise NotImplementedError('Only int literals supported for now.')

    def compile_UnaryOperator(self, expr, type_ctx):
        func = runtime.get_unary_op(expr.operator)
        compiled_val = self.compile_expr(expr.expr, type_ctx)
        result_type = func.check_types(compiled_val.type)
        return typed_ast.FunctionCall(func, [compiled_val], result_type)

    def compile_BinaryOperator(self, expr, type_ctx):
        func = runtime.get_binary_op(expr.operator)

        compiled_left = self.compile_expr(expr.left, type_ctx)
        compiled_right = self.compile_expr(expr.right, type_ctx)

        result_type = func.check_types(compiled_left.type, compiled_right.type)

        return typed_ast.FunctionCall(
            func, [compiled_left, compiled_right], result_type)

    def compile_FunctionCall(self, expr, type_ctx):
        # Innermost aggregates are special, since the context to use changes
        # inside them. We also need to generate an AggregateFunctionCall AST so
        # that the evaluator knows to change the context.
        if self.is_innermost_aggregate(expr):
            if type_ctx.aggregate_context is None:
                raise CompileError('Unexpected aggregate function.')
            sub_expr_ctx = type_ctx.aggregate_context
            ast_type = typed_ast.AggregateFunctionCall
        else:
            sub_expr_ctx = type_ctx
            ast_type = typed_ast.FunctionCall

        func = runtime.get_func(expr.name)
        compiled_args = [self.compile_expr(sub_expr, sub_expr_ctx)
                         for sub_expr in expr.args]
        result_type = func.check_types(*(arg.type for arg in compiled_args))
        return ast_type(func, compiled_args, result_type)

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

    @classmethod
    def expression_contains_aggregate(cls, expr):
        """Given a tq_ast expression, check if it does any aggregation.

        We need to operate on an uncompiled AST here since we use this
        information to figure out how to compile these expressions.
        """
        if isinstance(expr, tq_ast.UnaryOperator):
            return cls.expression_contains_aggregate(expr.expr)
        elif isinstance(expr, tq_ast.BinaryOperator):
            return (cls.expression_contains_aggregate(expr.left) or
                    cls.expression_contains_aggregate(expr.right))
        elif isinstance(expr, tq_ast.FunctionCall):
            return (runtime.is_aggregate_func(expr.name) or
                    any(cls.expression_contains_aggregate(arg)
                        for arg in expr.args))
        elif isinstance(expr, tq_ast.Literal):
            return False
        elif isinstance(expr, tq_ast.ColumnId):
            return False
        else:
            assert False, 'Unexpected expression type: %s' % (
                expr.__class__.__name__)

    @classmethod
    def is_innermost_aggregate(cls, expr):
        """Return True if the given expression is an innermost aggregate.

        Only arguments to innermost aggregates actually have access to fields
        from the original table expression, so we need to detect this case
        specifically.

        You might think that repeatedly calling this function while traversing
        the tree takes quadratic time in the size of the tree, but it actually
        only takes linear time overall. There's a nice proof of this fact,
        which this docstring is to small to contain.
        """
        return (isinstance(expr, tq_ast.FunctionCall) and
                runtime.is_aggregate_func(expr.name) and
                not any(cls.expression_contains_aggregate(sub_expr)
                        for sub_expr in expr.args))
