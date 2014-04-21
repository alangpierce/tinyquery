"""Implementation of the standard built-in functions."""
import abc
import time

import tq_types

class Function(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def check_types(self, *arg_types):
        """Return the type of the result as a function of the arg types.

        Raises a SyntaxError if the types are disallowed.
        """

    @abc.abstractmethod
    def evaluate(self, *args):
        """Evaluates the function itself.

        For normal, non-aggregate function, each argument is an individual
        value of the given type. For aggregate functions, each argument is a
        list of values. In either case, a single value of the result type is
        returned.
        """


class ArithmeticOperator(Function):
    """Basic operators like +."""
    def __init__(self, func):
        self.func = func

    def check_types(self, type1, type2):
        # TODO: Fail if types are wrong.
        return tq_types.INT

    def evaluate(self, arg1, arg2):
        return self.func(arg1, arg2)


class ComparisonOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, type1, type2):
        # TODO: Fail if types are wrong.
        return tq_types.BOOL

    def evaluate(self, arg1, arg2):
        return self.func(arg1, arg2)


class UnaryIntOperator(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, arg):
        assert arg == tq_types.INT
        return tq_types.INT

    def evaluate(self, arg):
        return self.func(arg)


class NoArgFunction(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self):
        return tq_types.INT

    def evaluate(self):
        return self.func()


class AggregateIntFunction(Function):
    def __init__(self, func):
        self.func = func

    def check_types(self, arg):
        assert arg == tq_types.INT
        return tq_types.INT

    def evaluate(self, arg_list):
        return self.func(arg_list)


_UNARY_OPERATORS = {
    '-': UnaryIntOperator(lambda a: -a)
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
}


_FUNCTIONS = {
    'abs': UnaryIntOperator(abs),
    'pow': ArithmeticOperator(lambda a, b: a ** b),
    'now': NoArgFunction(lambda: int(time.time() * 1000000))
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
    result = _FUNCTIONS[name]
    assert isinstance(result, Function)
    return result


def is_aggregate_func(name):
    return name in _AGGREGATE_FUNCTIONS


def get_aggregate_func(name):
    result = _AGGREGATE_FUNCTIONS[name]
    assert isinstance(result, Function)
    return result
