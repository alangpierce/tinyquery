"""Implementation of the standard built-in functions."""
import abc
import time
import compiler

import tq_types


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
        # TODO: Fail if types are wrong.
        return tq_types.INT

    def evaluate(self, num_rows, list1, list2):
        return [self.func(arg1, arg2) for arg1, arg2 in zip(list1, list2)]


class ComparisonOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, type1, type2):
        # TODO: Fail if types are wrong.
        return tq_types.BOOL

    def evaluate(self, num_rows, list1, list2):
        return [self.func(arg1, arg2) for arg1, arg2 in zip(list1, list2)]


class BooleanOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, type1, type2):
        # TODO: Fail if types are wrong.
        return tq_types.BOOL

    def evaluate(self, num_rows, list1, list2):
        return [self.func(arg1, arg2) for arg1, arg2 in zip(list1, list2)]


class UnaryIntOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, arg):
        if arg != tq_types.INT:
            raise TypeError('Expected int type')
        return tq_types.INT

    def evaluate(self, num_rows, arg_list):
        return [self.func(arg) for arg in arg_list]


class UnaryBoolOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, arg):
        return tq_types.BOOL

    def evaluate(self, num_rows, arg_list):
        return [self.func(arg) for arg in arg_list]


class NoArgFunction(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self):
        return tq_types.INT

    def evaluate(self, num_rows):
        return [self.func() for _ in xrange(num_rows)]


class InFunction(Function):
    def check_types(self, arg1, *arg_types):
        return tq_types.BOOL

    def evaluate(self, num_rows, arg1, *other_args):
        return [val1 in val_list
                for val1, val_list in zip(arg1, zip(*other_args))]

class AggregateIntFunction(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, arg):
        if not (arg in (tq_types.INT, tq_types.FLOAT)):
            raise TypeError('Expected int type')
        return tq_types.INT

    def evaluate(self, num_rows, arg_list):
        return [self.func(arg_list)]


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
}


_FUNCTIONS = {
    'abs': UnaryIntOperator(abs),
    'pow': ArithmeticOperator(lambda a, b: a ** b),
    'now': NoArgFunction(lambda: int(time.time() * 1000000)),
    'in': InFunction()
}


_AGGREGATE_FUNCTIONS = {
    'sum': AggregateIntFunction(sum),
    'min': AggregateIntFunction(min),
    'max': AggregateIntFunction(max)
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
