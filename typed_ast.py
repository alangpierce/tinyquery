"""A set of AST classes with types and aliases filled in."""

import collections
import compiler


class Select(collections.namedtuple(
        'Select', ['select_fields', 'table', 'where_expr', 'group_set'])):
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


class TypeContext(collections.namedtuple(
        'TypeContext', ['columns', 'aliases', 'ambig_aliases',
                        'aggregate_context'])):
    """Defines the types available at a point in code.

    This class is responsible for resolving column names into fully-qualified
    names. For example, if table1 and table2 are joined

    Fields:
        columns: An OrderedDict mapping from column name to type.
        aliases: A dict mapping any allowed aliases to their values. For
            example, the "value" column on a table "table" has full name
            "table.value" but the alias "value" also refers to it (as long as
            there are no other tables with a column named "value").
        ambig_aliases: A set of aliases that cannot be used because they are
            ambiguous. This is used for
        aggregate_context: Either None, indicating that aggregates are not
            allowed, or a TypeContext to use if we enter into an aggregate.

    """
    def column_ref_for_name(self, name):
        """Gets the full identifier for a column from any possible alias."""
        if name in self.columns:
            return ColumnRef(name, self.columns[name])
        elif name in self.aliases:
            full_name = self.aliases[name]
            return ColumnRef(full_name, self.columns[full_name])
        elif name in self.ambig_aliases:
            raise compiler.CompileError('Ambiguous field: {}'.format(name))
        else:
            raise compiler.CompileError('Field not found: {}'.format(name))


class TableExpression(object):
    """Abstract class for all table expression ASTs."""
    def __init__(self):
        assert hasattr(self, 'type_ctx')


class NoTable(collections.namedtuple('NoTable', []), TableExpression):
    @property
    def type_ctx(self):
        return TypeContext(collections.OrderedDict(), {}, [], None)


class Table(collections.namedtuple('Table', ['name', 'type_ctx'])):
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
