"""A set of AST classes that correspond to the code."""

import collections


class Select(collections.namedtuple(
        'Select', ['select_fields', 'table_expr', 'where_expr', 'groups'])):
    """Represents a top-level select statement.

    Fields:
        select_fields: A list of SelectField objects.
        table_expr: A table expression referring to the data to select from, or
            None if there is no table specified.
        where_expr: An expression for the WHERE filter, or None if there is
            no WHERE filter.
        groups: A list of strings for fields to group by, or None if there is
            no GROUP BY clause.
    """
    def __str__(self):
        result = 'SELECT {}'.format(
            ', '.join([str(field) for field in self.select_fields]))
        if self.table_expr:
            result += ' FROM {}'.format(self.table_expr)
        if self.where_expr:
            result += ' WHERE {}'.format(self.where_expr)
        if self.groups:
            result += ' GROUP BY {}'.format(', '.join(self.groups))
        return result


class SelectField(collections.namedtuple('SelectField', ['expr', 'alias'])):
    def __str__(self):
        if self.alias is not None:
            return '{} AS {}'.format(self.expr, self.alias)
        else:
            return str(self.expr)


class UnaryOperator(collections.namedtuple(
        'UnaryOperator', ['operator', 'expr'])):
    def __str__(self):
        return '({}{})'.format(self.operator, self.expr)


class BinaryOperator(collections.namedtuple(
        'BinaryOperator', ['operator', 'left', 'right'])):
    def __str__(self):
        return '({}{}{})'.format(self.left, self.operator, self.right)


class FunctionCall(collections.namedtuple('FunctionCall', ['name', 'args'])):
    def __str__(self):
        return '({}({}))'.format(self.name, self.args)


class Literal(collections.namedtuple('Literal', ['value'])):
    def __str__(self):
        return str(self.value)


class ColumnId(collections.namedtuple('ColumnId', ['name'])):
    def __str__(self):
        return self.name


class TableId(collections.namedtuple('TableId', ['name'])):
    def __str__(self):
        return self.name
