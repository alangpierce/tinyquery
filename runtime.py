"""Implementation of the standard built-in functions."""
import abc
import datetime
import random
import time
import math
import re

import arrow

import compiler
import context
import tq_types
import tq_modes


class Function(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def check_types(self, *arg_types):
        """Return the type of the result as a function of the arg types.

        Raises a SyntaxError if the types are disallowed.
        """

    @abc.abstractmethod
    def evaluate(self, num_rows, *arg_lists):
        """Evaluates the function on

        Arguments:
            num_rows: The number of rows that should be returned. This is used
                by zero-arg functions that can't infer the number of rows from
                the args themselves.
            arg_lists: Each argument is a list of values for the column to
                operate on. For normal functions, the length of each arugment
                must be num_rows, but for aggregate functions, num_rows will be
                1 and each arg can be any length.
        """


class ArithmeticOperator(Function):
    """Basic operators like +."""
    def __init__(self, func):
        self.func = func

    def check_types(self, type1, type2):
        numeric_types = (tq_types.FLOAT, tq_types.INT)
        if type1 not in numeric_types or type2 not in numeric_types:
            raise TypeError('Expected int or float type')
        if type1 == tq_types.FLOAT or type2 == tq_types.FLOAT:
            return tq_types.FLOAT
        else:
            return tq_types.INT

    def evaluate(self, num_rows, column1, column2):
        values = [self.func(arg1, arg2) for arg1, arg2 in zip(column1.values,
                                                              column2.values)]
        # TODO(Samantha): Code smell incoming
        t = self.check_types(column1.type, column2.type)
        return context.Column(type=t, mode=tq_modes.NULLABLE, values=values)


class ComparisonOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, type1, type2):
        # TODO: Fail if types are wrong.
        return tq_types.BOOL

    def evaluate(self, num_rows, column1, column2):
        values = [self.func(arg1, arg2) for arg1, arg2 in zip(column1.values,
                                                              column2.values)]
        return context.Column(type=tq_types.BOOL, mode=tq_modes.NULLABLE,
                              values=values)


class BooleanOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, type1, type2):
        # TODO: Fail if types are wrong.
        return tq_types.BOOL

    def evaluate(self, num_rows, column1, column2):
        values = [self.func(arg1, arg2) for arg1, arg2 in zip(column1.values,
                                                              column2.values)]
        return context.Column(type=tq_types.BOOL, mode=tq_modes.NULLABLE,
                              values=values)


class UnaryIntOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, arg):
        if arg != tq_types.INT:
            raise TypeError('Expected int type')
        return tq_types.INT

    def evaluate(self, num_rows, column):
        values = [self.func(arg) for arg in column.values]
        return context.Column(type=tq_types.INT, mode=tq_modes.NULLABLE,
                              values=values)


class UnaryBoolOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, arg):
        return tq_types.BOOL

    def evaluate(self, num_rows, column):
        values = [self.func(arg) for arg in column.values]
        return context.Column(type=tq_types.BOOL, mode=tq_modes.NULLABLE,
                              values=values)


class IfFunction(Function):
    def check_types(self, cond, arg1, arg2):
        if cond != tq_types.BOOL:
            raise TypeError('Expected bool type.')
        if arg1 == tq_types.NONETYPE:
            return arg2
        if arg2 == tq_types.NONETYPE:
            return arg1
        if arg1 != arg2:
            raise TypeError('Expected types to be the same.')
        return arg1

    def evaluate(self, num_rows, condition_column, then_column, else_column):
        values = [arg1 if cond else arg2
                  for cond, arg1, arg2 in zip(condition_column.values,
                                              then_column.values,
                                              else_column.values)]
        t = self.check_types(condition_column.type, then_column.type,
                             else_column.type)
        return context.Column(type=t, mode=tq_modes.NULLABLE, values=values)


class IfNullFunction(Function):
    def check_types(self, arg1, arg2):
        if arg1 == tq_types.NONETYPE:
            return arg2
        if arg2 == tq_types.NONETYPE:
            return arg1
        if arg1 != arg2:
            raise TypeError('Expected types to be the same.')
        return arg1

    def evaluate(self, num_rows, arg1, arg2):
        return arg2 if arg1 is None else arg1


class HashFunction(Function):
    def check_types(self, arg):
        return tq_types.INT

    def evaluate(self, num_rows, column):
        # TODO: Use CityHash.
        return context.Column(type=tq_types.INT, mode=tq_modes.NULLABLE,
                              values=[hash(arg) for arg in column.values])


class FloorFunction(Function):
    def check_types(self, arg):
        if arg not in (tq_types.INT, tq_types.FLOAT):
            raise TypeError('Expected type int or float.')
        return tq_types.FLOAT

    def evaluate(self, num_rows, column):
        values = [math.floor(arg) for arg in column.values]
        return context.Column(type=tq_types.FLOAT, mode=tq_modes.NULLABLE,
                              values=values)


class RandFunction(Function):
    def check_types(self):
        return tq_types.FLOAT

    def evaluate(self, num_rows):
        values = [random.random() for _ in xrange(num_rows)]
        # TODO(Samantha): Should this be required?
        return context.Column(type=tq_types.FLOAT, mode=tq_modes.NULLABLE,
                              values=values)


def _check_regexp_types(*types):
    if any(t != tq_types.STRING for t in types):
        raise TypeError('Expected string arguments.')


def _ensure_literal(regexps):
    assert all(r == regexps[0] for r in regexps), "Must provide a literal."
    return regexps[0]


# TODO(colin): the regexp functions here use the python re module, while
# bigquery uses the re2 library, which only has a subset of the functionality.
# Investigate pulling in re2 here.

class RegexpMatchFunction(Function):
    def check_types(self, type1, type2):
        _check_regexp_types(type1, type2)
        return tq_types.BOOL

    def evaluate(self, num_rows, strings, regexps):
        regexp = _ensure_literal(regexps.values)
        values = (
            [True if re.search(regexp, s) else False for s in strings.values])
        return context.Column(type=tq_types.BOOL, mode=tq_modes.NULLABLE,
                              values=values)


class RegexpExtractFunction(Function):
    def check_types(self, type1, type2):
        _check_regexp_types(type1, type2)
        return tq_types.STRING

    def evaluate(self, num_rows, strings, regexps):
        regexp = _ensure_literal(regexps.values)
        values = []
        for s in strings.values:
            match_result = re.search(regexp, s)
            if match_result is None:
                values.append(None)
            else:
                assert len(match_result.groups()) == 1, (
                    "Exactly one capturing group required")
                values.append(match_result.group(1))
        return context.Column(type=tq_types.STRING, mode=tq_modes.NULLABLE,
                              values=values)


class RegexpReplaceFunction(Function):
    def check_types(self, re_type, str_type, repl_type):
        _check_regexp_types(re_type, str_type, repl_type)
        return tq_types.STRING

    def evaluate(self, num_rows, strings, regexps, replacements):
        regexp = _ensure_literal(regexps.values)
        replacement = _ensure_literal(replacements.values)
        values = [re.sub(regexp, replacement, s) for s in strings.values]
        return context.Column(type=tq_types.STRING, mode=tq_modes.NULLABLE,
                              values=values)


# TODO(Samantha): I'm not sure how this actually works, leaving for now.
class NthFunction(Function):
    # TODO(alan): Enforce that NTH takes a constant as its first arg.
    def check_types(self, index_type, rep_list_type):
        if index_type != tq_types.INT:
            raise SyntaxError('Expected an int index.')
        # TODO(alan): Add a list type instead of assuming ints.
        return tq_types.INT

    def evaluate(self, num_rows, index_list, column):
        # Ideally, this would take a constant and not a full expression, but to
        # hack around it for now, we'll just take the value at the first "row",
        # which works if the expression was a constant.
        index = index_list.values[0]
        values = [self.safe_index(rep_elem, index)
                  for rep_elem in column.values]
        # TODO(Samantha): This is almost certainly incorrect.
        return context.Column(type=tq_types.INT, mode=tq_modes.NULLABLE,
                              values=values)

    @staticmethod
    def safe_index(rep_elem, index):
        if not rep_elem:
            return None
        if index <= 0 or index > len(rep_elem):
            return None
        return rep_elem[index - 1]


class FirstFunction(Function):
    def check_types(self, rep_list_type):
        return rep_list_type

    def evaluate(self, num_rows, column):
        values = []
        if len(column.values) == 0:
            values = [None]

        if column.mode == tq_modes.REPEATED:
            values = [repeated_row[0] if len(repeated_row) > 0 else None
                      for repeated_row in column.values]
        else:
            values = [column.values[0]]
        return context.Column(type=column.type, mode=tq_modes.NULLABLE,
                              values=values)


class NoArgFunction(Function):
    def __init__(self, func, return_type=tq_types.INT):
        self.func = func
        self.type = return_type

    def check_types(self):
        return self.type

    def evaluate(self, num_rows):
        return context.Column(type=self.type, mode=tq_modes.NULLABLE,
                              values=[self.func() for _ in xrange(num_rows)])


class InFunction(Function):
    def check_types(self, arg1, *arg_types):
        return tq_types.BOOL

    def evaluate(self, num_rows, arg1, *other_args):
        values = [val1 in val_list
                  for val1, val_list in zip(arg1.values,
                                            zip(*(map(lambda x: x.values,
                                                      other_args))))]
        return context.Column(type=tq_types.BOOL, mode=tq_modes.NULLABLE,
                              values=values)


class ConcatFunction(Function):
    def check_types(self, *arg_types):
        if any(arg_type != tq_types.STRING for arg_type in arg_types):
            raise TypeError('CONCAT only takes string arguments.')
        return tq_types.STRING

    def evaluate(self, num_rows, *columns):
        values = [''.join(strs) for strs in zip(*map(lambda x: x.values,
                                                     columns))]
        return context.Column(tq_types.STRING, tq_modes.NULLABLE,
                              values=values)


class StringFunction(Function):
    def check_types(self, arg_type):
        return tq_types.STRING

    def evaluate(self, num_rows, column):
        values = [str(arg) for arg in column.values]
        return context.Column(type=tq_types.STRING, mode=tq_modes.NULLABLE,
                              values=values)


class MinMaxFunction(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, arg):
        return arg

    def evaluate(self, num_rows, column):
        return context.Column(type=self.check_types(column.type),
                              mode=tq_modes.NULLABLE,
                              values=[self.func(column.values)])


class SumFunction(Function):
    def check_types(self, arg):
        if arg == tq_types.BOOL:
            return tq_types.INT
        elif arg == tq_types.INT or arg == tq_types.FLOAT:
            return arg
        else:
            raise TypeError('Unexpected type.')

    def evaluate(self, num_rows, column):
        values = [sum([0 if arg is None else arg for arg in column.values])]
        return context.Column(type=tq_types.INT, mode=tq_modes.NULLABLE,
                              values=values)


class CountFunction(Function):
    def check_types(self, arg):
        return tq_types.INT

    def evaluate(self, num_rows, column):
        values = [len([0 for arg in column.values if arg is not None])]
        return context.Column(type=tq_types.INT, mode=tq_modes.NULLABLE,
                              values=values)


class AvgFunction(Function):
    def check_types(self, arg):
        return tq_types.FLOAT

    def evaluate(self, num_rows, column):
        filtered_args = [arg for arg in column.values if arg is not None]
        values = ([None] if not filtered_args else
                  [float(sum(filtered_args)) / len(filtered_args)])
        return context.Column(type=tq_types.FLOAT, mode=tq_modes.NULLABLE,
                              values=values)


class CountDistinctFunction(Function):
    def check_types(self, arg):
        return tq_types.INT

    def evaluate(self, num_rows, column):
        return context.Column(type=tq_types.INT, mode=tq_modes.NULLABLE,
                              values=[len(set(column.values))])


class StddevSampFunction(Function):
    def check_types(self, arg):
        return tq_types.FLOAT

    def evaluate(self, num_rows, column):
        # TODO(alan): Implement instead of returning 0.
        return context.Column(type=tq_types.FLOAT, mode=tq_modes.NULLABLE,
                              values=[0.0])


class QuantilesFunction(Function):
    # TODO(alan): Enforce that QUANTILES takes a constant as its second arg.
    def check_types(self, arg_list_type, num_quantiles_type):
        if num_quantiles_type != tq_types.INT:
            raise SyntaxError('Expected an int number of quantiles.')
        # TODO(alan): This should actually return a repeated version of the arg
        # list type.
        return tq_types.INT

    def evaluate(self, num_rows, column, num_quantiles_list):
        sorted_args = sorted(arg for arg in column.values if arg is not None)
        values = []
        if not sorted_args:
            values = [None]
        # QUANTILES is special because it takes a constant, not an expression
        # that gets repeated for each column. To hack around this for now, just
        # take the first element off.
        num_quantiles = num_quantiles_list.values[0]
        # Stretch the quantiles out so the first is always the min of the list
        # and the last is always the max of the list, but make sure it stays
        # within the bounds of the list so we don't get an IndexError.
        # This returns a single repeated field rather than one row per
        # quantile, so we need one more set of brackets than you might expect.
        values = [[
            sorted_args[
                min(len(sorted_args) * i / (num_quantiles - 1),
                    len(sorted_args) - 1)
            ] for i in xrange(num_quantiles)
        ]]
        return context.Column(type=tq_types.INT, mode=tq_modes.REPEATED,
                              values=values)


class ContainsFunction(Function):
    def check_types(self, type1, type2):
        if type1 != tq_types.STRING or type2 != tq_types.STRING:
            raise TypeError("CONTAINS must operate on strings.")
        return tq_types.BOOL

    def evaluate(self, num_rows, column1, column2):
        if len(column1.values) == len(column2.values):
            values = (
                [v2 in v1 for v1, v2 in zip(column1.values, column2.values)])
            return context.Column(type=tq_types.BOOL, mode=tq_modes.NULLABLE,
                                  values=values)


class TimestampFunction(Function):
    def check_types(self, type1):
        if type1 not in (tq_types.STRING, tq_types.INT):
            raise TypeError(
                'TIMESTAMP requires an ISO8601 string or unix timestamp in '
                'microseconds.')
        return tq_types.TIMESTAMP

    def evaluate(self, num_rows, column):
        converter = lambda ts: ts
        if num_rows > 0 and column.type == tq_types.INT:
            # Bigquery accepts integer number of microseconds since the unix
            # epoch here, whereas arrow wants a unix timestamp, with possible
            # decimal part representing microseconds.
            converter = lambda ts: float(ts) / 1E6
        values = [
            # arrow.get parses ISO8601 strings and int/float unix timestamps
            # without a format parameter
            arrow.get(converter(ts)).to('UTC').naive
            for ts in column.values
        ]
        return context.Column(type=tq_types.TIMESTAMP, mode=tq_modes.NULLABLE,
                              values=values)


_UNARY_OPERATORS = {
    '-': UnaryIntOperator(lambda a: -a),
    'is_null': UnaryBoolOperator(lambda a: a is None),
    'is_not_null': UnaryBoolOperator(lambda a: a is not None),
}


_BINARY_OPERATORS = {
    '+': ArithmeticOperator(lambda a, b: a + b),
    '-': ArithmeticOperator(lambda a, b: a - b),
    '*': ArithmeticOperator(lambda a, b: a * b),
    '/': ArithmeticOperator(lambda a, b: a / b),
    '%': ArithmeticOperator(lambda a, b: a % b),
    '=': ComparisonOperator(lambda a, b: a == b),
    '!=': ComparisonOperator(lambda a, b: a != b),
    '>': ComparisonOperator(lambda a, b: a > b),
    '<': ComparisonOperator(lambda a, b: a < b),
    '>=': ComparisonOperator(lambda a, b: a >= b),
    '<=': ComparisonOperator(lambda a, b: a <= b),
    'and': BooleanOperator(lambda a, b: a and b),
    'or': BooleanOperator(lambda a, b: a or b),
    'contains': ContainsFunction(),
}


_FUNCTIONS = {
    'abs': UnaryIntOperator(abs),
    'floor': FloorFunction(),
    'rand': RandFunction(),
    'nth': NthFunction(),
    'concat': ConcatFunction(),
    'string': StringFunction(),
    'pow': ArithmeticOperator(lambda a, b: a ** b),
    'now': NoArgFunction(lambda: int(time.time() * 1000000)),
    'in': InFunction(),
    'if': IfFunction(),
    'ifnull': IfNullFunction(),
    'hash': HashFunction(),
    'regexp_match': RegexpMatchFunction(),
    'regexp_extract': RegexpExtractFunction(),
    'regexp_replace': RegexpReplaceFunction(),
    'timestamp': TimestampFunction(),
    'current_date': NoArgFunction(
        lambda: datetime.datetime.utcnow().strftime('%Y-%m-%d'),
        return_type=tq_types.STRING),
    'current_time': NoArgFunction(
        lambda: datetime.datetime.utcnow().strftime('%H:%M:%S'),
        return_type=tq_types.STRING),
    'current_timestamp': NoArgFunction(
        lambda: datetime.datetime.utcnow(),
        return_type=tq_types.TIMESTAMP),
}


_AGGREGATE_FUNCTIONS = {
    'sum': SumFunction(),
    'min': MinMaxFunction(min),
    'max': MinMaxFunction(max),
    'count': CountFunction(),
    'avg': AvgFunction(),
    'count_distinct': CountDistinctFunction(),
    'stddev_samp': StddevSampFunction(),
    'quantiles': QuantilesFunction(),
    'first': FirstFunction()
}


def get_unary_op(name):
    result = _UNARY_OPERATORS[name]
    assert isinstance(result, Function)
    return result


def get_binary_op(name):
    result = _BINARY_OPERATORS[name]
    assert isinstance(result, Function)
    return result


def get_func(name):
    if name in _FUNCTIONS:
        return _FUNCTIONS[name]
    elif name in _AGGREGATE_FUNCTIONS:
        return _AGGREGATE_FUNCTIONS[name]
    else:
        raise compiler.CompileError('Unknown function: {}'.format(name))


def is_aggregate_func(name):
    return name in _AGGREGATE_FUNCTIONS
