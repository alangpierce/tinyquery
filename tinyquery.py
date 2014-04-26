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

        if select_ast.group_set is not None:
            result_context = self.evaluate_groups(
                select_ast.select_fields, select_ast.group_set, select_context)
        else:
            result_context = self.evaluate_select_fields(
                select_ast.select_fields, select_context)
        return table_from_context('query_result', result_context)

    def evaluate_groups(self, select_fields, group_set, select_context):
        """Evaluate a list of select fields, grouping by some of the values.

        Arguments:
            select_fields: A list of SelectField instances to evaluate.
            group_set: The groups (either fields in select_context or aliases
                referring to an element of select_fields) to group by.
            select_context: A context with the data that the select statement
                has access to.

        Returns:
            A context with the results.
        """
        assert group_set.alias_groups == set(), (
            'Alias groups are currently unsupported.')
        field_groups = group_set.field_groups
        # Dictionary mapping (singleton) group key context to the context of
        # values for that key.
        group_contexts = {}
        for i in xrange(select_context.num_rows):
            def value_from_field(field_group):
                return field_group.column, Column(field_group.type, [
                    select_context.columns[field_group.column].values[i]])
            key = Context(1, collections.OrderedDict(
                value_from_field(field) for field in field_groups), None)
            if key not in group_contexts:
                new_group_context = empty_context_from_template(select_context)
                group_contexts[key] = new_group_context
            group_context = group_contexts[key]
            append_row_to_context(src_context=select_context, index=i,
                                  dest_context=group_context)
        result_context = self.empty_context_from_select_fields(select_fields)
        for context_key, group_context in group_contexts.iteritems():
            group_eval_context = Context(1, context_key.columns, group_context)
            group_result_context = self.evaluate_select_fields(
                select_fields, group_eval_context)
            append_row_to_context(group_result_context, 0, result_context)
        return result_context

    def empty_context_from_select_fields(self, select_fields):
        return Context(
            0,
            collections.OrderedDict(
                (select_field.alias, Column(select_field.expr.type, []))
                for select_field in select_fields
            ),
            None)

    def evaluate_select_fields(self, select_fields, context):
        """Evaluate a table result given the data the fields have access to.

        Arguments:
            select_fields: A list of typed_ast.SelectField values to evaluate.
            context: The "source" context that the expressions can access when
                being evaluated.
        """
        return Context(context.num_rows,
                       collections.OrderedDict(
                           self.evaluate_select_field(select_field, context)
                           for select_field in select_fields),
                       None)

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


class Context(object):
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
        self.num_rows = num_rows
        self.columns = columns
        self.aggregate_context = aggregate_context

    def __repr__(self):
        return 'Context({}, {}, {})'.format(self.num_rows, self.columns,
                                            self.aggregate_context)

    def __eq__(self, other):
        return ((self.num_rows, self.columns, self.aggregate_context) ==
                other.num_rows, other.columns, other.aggregate_context)

    def __hash__(self):
        return hash((
            self.num_rows,
            tuple(tuple(column.values) for column in self.columns.values()),
            self.aggregate_context))


class Column(collections.namedtuple('Column', ['type', 'values'])):
    """Represents a single column of data.

    Fields:
        type: A constant from the tq_types module.
        values: A list of raw values for the column contents.
    """


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


def empty_context_from_template(context):
    """Returns a new context that has the same columns as the given context."""
    return Context(num_rows=0,
                   columns=collections.OrderedDict(
                       (name, empty_column_from_template(column))
                       for name, column in context.columns.iteritems()
                   ),
                   aggregate_context=None)


def empty_column_from_template(column):
    """Returns a new empty column with the same type as the given one."""
    return Column(column.type, [])


def append_row_to_context(src_context, index, dest_context):
    """Take row i from src_context and append it to dest_context.

    The schemas of the two contexts must match.
    """
    dest_context.num_rows += 1
    for name, column in dest_context.columns.iteritems():
        column.values.append(src_context.columns[name].values[index])

def table_from_context(table_name, context):
    return Table(table_name, context.num_rows, context.columns)
