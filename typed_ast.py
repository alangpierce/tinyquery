"""A set of AST classes with types and aliases filled in."""

import collections


class Select(collections.namedtuple(
        'Select', ['select_fields', 'table'])):
    """Currently, you can only select directly from table columns."""
    pass


class SelectField(collections.namedtuple('SelectField', ['expr', 'alias'])):
    pass


class FunctionCall(collections.namedtuple(
        'FunctionCall', ['name', 'args', 'type'])):
    pass


class Literal(collections.namedtuple('Literal', ['value', 'type'])):
    pass


class ColumnRef(collections.namedtuple(
        'ColumnRef', ['table', 'column', 'type'])):
    pass
