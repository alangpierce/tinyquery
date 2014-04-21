"""Implementation of the TinyQuery service."""
import collections
import itertools

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

        table_context = self.evaluate_table_expr(select_ast.table)
        mask_column = self.evaluate_expr(select_ast.where_expr, table_context)
        select_context = mask_context(table_context, mask_column)

        if select_ast.groups is not None:
            assert select_ast.groups == [], (
                'Only the empty group list is supported for now.')
            select_context = Context(1, collections.OrderedDict(),
                                     select_context)

        result_columns = [
            self.evaluate_select_field(select_field, select_context)
            for select_field in select_ast.select_fields]
        return Table('query_result', select_context.num_rows,
                     collections.OrderedDict(result_columns))

    def evaluate_select_field(self, select_field, context):
        """Given a typed select field, return a resulting name and Column."""
        assert isinstance(select_field, typed_ast.SelectField)
        results = self.evaluate_expr(select_field.expr, context)
        return select_field.alias, Column(select_field.expr.type, results)

    def evaluate_table_expr(self, table_expr):
        """Given a table expression, return a Context with its values."""
        try:
            method = getattr(self,
                             'eval_table_' + table_expr.__class__.__name__)
        except AttributeError:
            raise NotImplementedError(
                'Missing handler for table type {}'.format(
                    table_expr.__class__.__name__))
        return method(table_expr)

    def eval_table_NoTable(self, table_expr):
        # If the user isn't selecting from any tables, just specify that there
        # is one column to return and no table accessible.
        return Context(1, collections.OrderedDict(), None)

    def eval_table_Table(self, table_expr):
        table = self.tables_by_name[table_expr.name]
        return context_from_table(table)

    def evaluate_expr(self, expr, context):
        """Computes the raw data for the output column for the expression."""
        try:
            method = getattr(self, 'evaluate_' + expr.__class__.__name__)
        except AttributeError:
            raise NotImplementedError(
                'Missing handler for type {}'.format(expr.__class__.__name__))
        return method(expr, context)

    def evaluate_FunctionCall(self, func_call, context):
        arg_results = [self.evaluate_expr(arg, context)
                       for arg in func_call.args]
        return func_call.func.evaluate(context.num_rows, *arg_results)

    def evaluate_AggregateFunctionCall(self, func_call, context):
        # Switch to the aggregate context when evaluating the arguments to the
        # aggregate.
        assert context.aggregate_context is not None, (
            'Aggregate function called without a valid aggregate context.')
        arg_results = [self.evaluate_expr(arg, context.aggregate_context)
                       for arg in func_call.args]
        return func_call.func.evaluate(context.num_rows, *arg_results)

    def evaluate_Literal(self, literal, context):
        return [literal.value for _ in xrange(context.num_rows)]

    def evaluate_ColumnRef(self, column_ref, context):
        column = context.columns[column_ref.column]
        return column.values


class Table(collections.namedtuple('Table', ['name', 'num_rows', 'columns'])):
    """Information containing metadata and contents of a table.

    Fields:
        columns: A dict mapping column name to column.
    """
    def __init__(self, name, num_rows, columns):
        assert isinstance(columns, collections.OrderedDict)
        for name, column in columns.iteritems():
            assert len(column.values) == num_rows, (
                'Column %s had %s rows, expected %s.' % (
                    name, len(column.values), num_rows))
        super(Table, self).__init__()


class Context(collections.namedtuple(
        'Context', ['num_rows', 'columns', 'aggregate_context'])):
    """Represents the columns accessible when evaluating an expression.

    Fields:
        num_rows: The number of rows for all columns in this context.
        columns: An OrderedDict from column name to Column.
        aggregate_context: Either None, indicating that aggregate functions
            aren't allowed, or another Context to use whenever we enter into an
            aggregate function.
    """
    def __init__(self, num_rows, columns, aggregate_context):
        assert isinstance(columns, collections.OrderedDict)
        for name, column in columns.iteritems():
            assert len(column.values) == num_rows, (
                'Column %s had %s rows, expected %s.' % (
                    name, len(column.values), num_rows))
        if aggregate_context is not None:
            assert isinstance(aggregate_context, Context)
        super(Context, self).__init__()


class Column(collections.namedtuple('Column', ['type', 'values'])):
    pass


def context_from_table(table):
    any_column = table.columns.itervalues().next()
    new_columns = collections.OrderedDict([
        (table.name + '.' + column_name, column)
        for (column_name, column) in table.columns.iteritems()
    ])
    return Context(len(any_column.values), new_columns, None)


def mask_context(context, mask):
    """Apply a row filter to a given context.

    Arguments:
        context: A Context to filter.
        mask: A column of type bool. Each row in this column should be True if
            the row should be kept for the whole context and False otherwise.
    """
    assert context.aggregate_context is None, (
        'Cannot mask a context with an aggregate context.')
    new_columns = collections.OrderedDict([
        (column_name,
         Column(column.type, list(itertools.compress(column.values, mask))))
        for (column_name, column) in context.columns.iteritems()
    ])
    return Context(sum(mask), new_columns, None)
