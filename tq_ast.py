"""A set of AST classes that correspond to the code."""

import collections


class Select(collections.namedtuple(
        'Select', ['select_fields', 'table_expr', 'where_expr'])):
    """Represents a top-level select statement.

    Fields:
        select_fields: A list of SelectField objects.
        table_expr: A table expression referring to the data to select from, or
            None if there is no table specified.
        where_expr: An expression for the WHERE filter, or None if there is
            no WHERE filter.
    """
    def __str__(self):
        result = 'SELECT {}'.format(
            ', '.join([str(field) for field in self.select_fields]))
        if self.table_expr:
            result += ' FROM {}'.format(self.table_expr)
        if self.where_expr:
            result += ' WHERE {}'.format(self.where_expr)
        return result


class SelectField(collections.namedtuple('SelectField', ['expr'])):
    def __str__(self):
        return str(self.expr)


class BinaryOperator(collections.namedtuple(
        'BinaryOperator', ['operator', 'left', 'right'])):
    def __str__(self):
        return '({}{}{})'.format(self.left, self.operator, self.right)


class Literal(collections.namedtuple('Literal', ['value'])):
    def __str__(self):
        return str(self.value)


class ColumnId(collections.namedtuple('ColumnId', ['name'])):
    def __str__(self):
        return self.name


class TableId(collections.namedtuple('TableId', ['name'])):
    def __str__(self):
        return self.name
