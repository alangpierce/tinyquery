"""A set of AST classes that correspond to the code.

This AST format is desinged to be easy to parse into. See typed_ast for the AST
format that is used during the evaluation step.
"""

import collections


class Select(collections.namedtuple(
        'Select', ['select_fields', 'table_expr', 'where_expr', 'groups',
                   'orderings', 'limit', 'alias'])):
    """Represents a top-level select statement.

    Fields:
        select_fields: A list of SelectField objects.
        table_expr: A table expression referring to the data to select from, or
            None if there is no table specified.
        where_expr: An expression for the WHERE filter, or None if there is
            no WHERE filter.
        groups: A list of strings for fields to group by, or None if there is
            no GROUP BY clause.
        orderings: A list of Ordering instances, or None if there was no
            ORDER BY clause.
        limit: An integer limit
        alias: For subqueries, a name given to the subquery, or None if no name
            was given (or if this is an outermost query).
    """
    def __str__(self):
        result = 'SELECT {}'.format(
            ', '.join([str(field) for field in self.select_fields]))
        if self.table_expr:
            result += ' FROM {}'.format(self.table_expr)
        if self.where_expr:
            result += ' WHERE {}'.format(self.where_expr)
        if self.groups:
            result += ' GROUP BY {}'.format(
                ', '.join(str(group) for group in self.groups))
        if self.orderings:
            result += ' ORDER BY {}'.format(
                ', '.join(str(ordering) for ordering in self.orderings))
        if self.limit:
            result += ' LIMIT {}'.format(self.limit)
        return result


class SelectField(collections.namedtuple('SelectField', ['expr', 'alias'])):
    def __str__(self):
        if self.alias is not None:
            return '{} AS {}'.format(self.expr, self.alias)
        else:
            return str(self.expr)


class Star(collections.namedtuple('Star', [])):
    def __str__(self):
        return '*'


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


class Ordering(collections.namedtuple('Ordering',
                                      ['column_id', 'is_ascending'])):
    def __str__(self):
        if self.is_ascending:
            return '{} ASC'.format(self.column_id)
        else:
            return '{} DESC'.format(self.column_id)


class TableId(collections.namedtuple('TableId', ['name', 'alias'])):
    """Table expression referencing a table to select from.

    Fields:
        name: The name of the table to select from.
        alias: An alias to assign to use for this table, or None if no alias
            was specified.
    """
    def __str__(self):
        return self.name


class TableUnion(collections.namedtuple('TableUnion', ['tables'])):
    """Table expression for a union of tables (the comma operator).

    The tables can be arbitrary table expressions.
    """
    def __str__(self):
        return ', '.join(str(table) for table in self.tables)


class Join(collections.namedtuple('Join', ['table1', 'table2', 'condition',
                                           'is_left_outer'])):
    """Table expression for a join of two tables.

    Joining more than two tables currently isn't supported.
    """
    def __str__(self):
        if self.is_left_outer:
            return '{} LEFT OUTER JOIN {} ON {}'.format(
                self.table1, self.table2, self.condition)
        else:
            return '{} JOIN {} ON {}'.format(
                self.table1, self.table2, self.condition)


class CrossJoin(collections.namedtuple('CrossJoin', ['table1', 'table2'])):
    """Table expression for a cross join of two tables.

    This needs to be parsed separately instead of joining on true since there's
    no way to write a regular JOIN that behaves as a CROSS JOIN.
    """
    def __str__(self):
        return '{} CROSS JOIN {}'.format(self.table1, self.table2)
