"""A set of AST classes with types and aliases filled in."""

import collections
import type_context


class Select(collections.namedtuple(
        'Select', ['select_fields', 'table', 'where_expr', 'group_set',
                   'type_ctx'])):
    """A compiled query.

    Fields:
        select_fields: A list of SelectField, one for each item being selected.
        table: The table expression to select from.
        where_expr: A filter to apply on the selected table expression. Note
            that this filter should always be valid; if the user didn't specify
            a WHERE clause, this is the literal true.
        groups: Either None, indicating that no grouping should be done, or a
            GroupSet object. If there were groups explicitly specified by
            GROUP BY, then the GroupSet always exists and is nonempty. If there
            was no GROUP BY but the select is an aggregate select, the GroupSet
            exists and is empty (since grouping by nothing puts everything into
            the same group).
    """


class SelectField(collections.namedtuple('SelectField', ['expr', 'alias'])):
    pass


class GroupSet(collections.namedtuple(
        'GroupSet', ['alias_groups', 'field_groups'])):
    """Information about the groups to use for a query.

    Fields:
        alias_groups: A set of string names of aliases for select fields that
            we should group by. These are special because they need to be
            compiled and evaluated differently from normal select fields.
        field_groups: A list of ColumnRefs referencing columns in the table
            expression of the SELECT statement.
    """


class TableExpression(object):
    """Abstract class for all table expression ASTs."""
    def __init__(self, *_):
        assert hasattr(self, 'type_ctx')


class NoTable(collections.namedtuple('NoTable', []), TableExpression):
    @property
    def type_ctx(self):
        return type_context.TypeContext(collections.OrderedDict(), {}, [],
                                        None)


class Table(collections.namedtuple('Table', ['name', 'type_ctx']),
            TableExpression):
    pass


class TableUnion(collections.namedtuple('TableUnion', ['tables', 'type_ctx']),
                 TableExpression):
    pass


class Expression(object):
    """Abstract interface for all expression ASTs."""
    def __init__(self, *args):
        assert hasattr(self, 'type')


class FunctionCall(collections.namedtuple(
        'FunctionCall', ['func', 'args', 'type']), Expression):
    """Expression representing a call to a built-in function.

    Fields:
        func: A runtime.Function for the function to call.
        args: A list of expressions to pass in as the function's arguments.
        type: The result type of the expression.
    """


class AggregateFunctionCall(collections.namedtuple(
        'AggregateFunctionCall', ['func', 'args', 'type']), Expression):
    """Expression representing a call to a built-in aggregate function.

    Aggregate functions are called differently from regular functions, so we
    need to have a special case for them in the AST format.

    Fields:
        func: A runtime.Function for the function to call.
        args: A list of expressions to pass in as the function's arguments.
        type: The result type of the expression.
    """


class Literal(collections.namedtuple('Literal', ['value', 'type'])):
    pass


class ColumnRef(collections.namedtuple('ColumnRef', ['column', 'type'])):
    """References a column from the current context."""
