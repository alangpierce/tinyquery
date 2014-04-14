"""A set of AST classes with types and aliases filled in."""

import collections


class Select(collections.namedtuple(
        'Select', ['select_fields', 'table', 'where_expr'])):
    """Currently, you can only select directly from table columns."""
    pass


class SelectField(collections.namedtuple('SelectField', ['expr', 'alias'])):
    pass


class FunctionCall(collections.namedtuple(
        'FunctionCall', ['func', 'args', 'type'])):
    """Expression representing a call to a built-in function.

    Fields:
        func: A runtime.Function for the function to call.
        args: A list of expressions to pass in as the function's arguments.
        type: The result type of the expression.
    """


class Literal(collections.namedtuple('Literal', ['value', 'type'])):
    pass


class ColumnRef(collections.namedtuple(
        'ColumnRef', ['table', 'column', 'type'])):
    pass
