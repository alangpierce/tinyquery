"""Implementation of the TinyQuery service."""
import collections

import compiler
import typed_ast


class TinyQuery(object):
    def __init__(self):
        self.tables_by_name = {}

    def load_table(self, table):
        """Create a table.

        Arguments:
            name: The name of the table.
            data: A dict mapping column name to list of values.
        """
        self.tables_by_name[table.name] = table

    def get_all_tables(self):
        return self.tables_by_name

    def evaluate_query(self, query):
        select_ast = compiler.compile_text(query, self.tables_by_name)
        return self.evaluate_select(select_ast)

    def evaluate_select(self, select_ast):
        """Given a typed select statement, return a Table with the results."""
        assert isinstance(select_ast, typed_ast.Select)
        if select_ast.table:
            table = self.tables_by_name[select_ast.table]
            num_rows = len(table.columns.itervalues().next().values)
        else:
            num_rows = 1
        result_columns = [self.evaluate_select_field(select_field, num_rows)
                          for select_field in select_ast.select_fields]
        return Table('query_result', dict(result_columns))

    def evaluate_select_field(self, select_field, num_rows):
        """Given a typed select field, return a resulting name and  Column."""
        assert isinstance(select_field, typed_ast.SelectField)
        results = self.evaluate_expr(select_field.expr, num_rows)
        return select_field.alias, Column(select_field.expr.type, results)

    def evaluate_expr(self, expr, num_rows):
        try:
            method = getattr(self, 'evaluate_' + expr.__class__.__name__)
        except AttributeError:
            raise NotImplementedError(
                'Missing handler for type {}'.format(expr.__class__.__name__))
        return method(expr, num_rows)

    functions = {
        '+': lambda a, b: a + b,
        '-': lambda a, b: a - b,
        '*': lambda a, b: a * b,
        '/': lambda a, b: a / b,
        '%': lambda a, b: a % b,
    }

    def evaluate_FunctionCall(self, func_call, num_rows):
        arg_results = [self.evaluate_expr(arg, num_rows)
                       for arg in func_call.args]
        func = self.functions[func_call.name]
        return [func(*func_args) for func_args in zip(*arg_results)]

    def evaluate_Literal(self, literal, num_rows):
        return [literal.value for _ in xrange(num_rows)]

    def evaluate_ColumnRef(self, column_ref, num_rows):
        table = self.tables_by_name[column_ref.table]
        return table.columns[column_ref.column].values


class Table(collections.namedtuple('Table', ['name', 'columns'])):
    """Information containing metadata and contents of a table.

    Fields:
        columns: A dict mapping column name to column.
    """
    pass


class Column(collections.namedtuple('Column', ['type', 'values'])):
    pass
