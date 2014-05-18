"""A context is similar to a table, but doesn't have a specific name.

It is the basic container for intermediate data when evaluating a query.
"""

import collections
import itertools


class Context(object):
    """Represents the columns accessible when evaluating an expression.

    Fields:
        num_rows: The number of rows for all columns in this context.
        columns: An OrderedDict from (table_name, column_name) name to Column.
            The table_name can be None. These should match the values in the
            corresponding TypeContext.
        aggregate_context: Either None, indicating that aggregate functions
            aren't allowed, or another Context to use whenever we enter into an
            aggregate function.
    """
    def __init__(self, num_rows, columns, aggregate_context):
        assert isinstance(columns, collections.OrderedDict)
        for (table_name, col_name), column in columns.iteritems():
            assert len(column.values) == num_rows, (
                'Column %s had %s rows, expected %s.' % (
                    (table_name, col_name), len(column.values), num_rows))
        if aggregate_context is not None:
            assert isinstance(aggregate_context, Context)
        self.num_rows = num_rows
        self.columns = columns
        self.aggregate_context = aggregate_context

    def column_from_ref(self, column_ref):
        """Given a ColumnRef, return the corresponding column."""
        return self.columns[(column_ref.table, column_ref.column)]

    def __repr__(self):
        return 'Context({}, {}, {})'.format(self.num_rows, self.columns,
                                            self.aggregate_context)

    def __eq__(self, other):
        return ((self.num_rows, self.columns, self.aggregate_context) ==
                (other.num_rows, other.columns, other.aggregate_context))

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


def context_from_table(table, type_context):
    """Given a table and a type context, build a context with those values.

    The order of the columns in the type context must match the order of the
    columns in the table.
    """
    any_column = table.columns.itervalues().next()
    new_columns = collections.OrderedDict([
        (column_name, column)
        for (column_name, column) in zip(type_context.columns.iterkeys(),
                                         table.columns.itervalues())
    ])
    return Context(len(any_column.values), new_columns, None)


def context_with_overlayed_type_context(context, type_context):
    """Given a context, use the given type context for all column names."""
    any_column = context.columns.itervalues().next()
    new_columns = collections.OrderedDict([
        (column_name, column)
        for (column_name, column) in zip(type_context.columns.iterkeys(),
                                         context.columns.itervalues())
    ])
    return Context(len(any_column.values), new_columns, None)


def empty_context_from_type_context(type_context):
    assert type_context.aggregate_context is None
    result_columns = collections.OrderedDict(
        (col_name, Column(col_type, []))
        for col_name, col_type in type_context.columns.iteritems()
    )
    return Context(0, result_columns, None)


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
    return Context(
        num_rows=0,
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


def append_partial_context_to_context(src_context, dest_context):
    """Modifies dest_context to include all rows in src_context.

    The schemas don't need to match exactly; src_context just needs to have a
    subset, and all other columns will be given a null value.

    Also, it is assumed that the destination context only uses short column
    names rather than fully-qualified names.
    """
    dest_context.num_rows += src_context.num_rows
    # Ignore fully-qualified names for this operation.
    short_named_src_column_values = {
        col_name: column.values
        for (_, col_name), column in src_context.columns.iteritems()}

    for (_, col_name), dest_column in dest_context.columns.iteritems():
        src_column_values = short_named_src_column_values.get(col_name)
        if src_column_values is None:
            dest_column.values.extend([None] * src_context.num_rows)
        else:
            dest_column.values.extend(src_column_values)


def append_context_to_context(src_context, dest_context):
    """Adds all rows in src_context to dest_context.

    The columns must be a subset, but all fully-qualified names are taken into
    account.
    """
    dest_context.num_rows += src_context.num_rows
    for dest_column_key, dest_column in dest_context.columns.iteritems():
        src_column = src_context.columns.get(dest_column_key)
        if src_column is None:
            dest_column.values.extend([None] * src_context.num_rows)
        else:
            dest_column.values.extend(src_column.values)


def row_context_from_context(src_context, index):
    """Pull a specific row out of a context as its own context."""
    assert src_context.aggregate_context is None
    columns = collections.OrderedDict(
        (col_name, Column(col.type, [col.values[index]]))
        for col_name, col in src_context.columns.iteritems()
    )
    return Context(1, columns, None)


def cross_join_contexts(context1, context2):
    assert context1.aggregate_context is None
    assert context2.aggregate_context is None
    result_columns = collections.OrderedDict(
        [(col_name, Column(col.type, []))
         for col_name, col in context1.columns.iteritems()] +
        [(col_name, Column(col.type, []))
         for col_name, col in context2.columns.iteritems()])

    for index1 in xrange(context1.num_rows):
        for index2 in xrange(context2.num_rows):
            for col_name, column in context1.columns.iteritems():
                result_columns[col_name].values.append(column.values[index1])
            for col_name, column in context2.columns.iteritems():
                result_columns[col_name].values.append(column.values[index2])
    return Context(context1.num_rows * context2.num_rows, result_columns, None)


def truncate_context(context, limit):
    """Modify the given context to have at most the given number of rows."""
    assert context.aggregate_context is None
    # BigQuery adds non-int limits, so we need to allow floats up until now.
    limit = int(limit)
    if context.num_rows <= limit:
        return
    context.num_rows = limit

    for column in context.columns.itervalues():
        column.values[limit:] = []
