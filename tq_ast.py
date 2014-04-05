import collections


class Select(collections.namedtuple('Select', ['expr'])):
    def __str__(self):
        return 'SELECT {}'.format(self.expr)


class BinaryOperator(collections.namedtuple(
        'BinaryOperator', ['operator', 'left', 'right'])):
    def __str__(self):
        return '({}{}{})'.format(self.left, self.operator, self.right)


class Literal(collections.namedtuple('Literal', ['value'])):
    def __str__(self):
        return str(self.value)
