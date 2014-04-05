import parser
import tq_ast


def evaluate_text(text):
    ast = parser.parse_text(text)
    if not isinstance(ast, tq_ast.Select):
        raise RuntimeError('Expected a select expression.')
    return evaluate_expr(ast.expr)


def evaluate_expr(expr):
    try:
        method = globals()['evaluate_' + expr.__class__.__name__]
    except KeyError:
        raise NotImplementedError('Missing handler for type {0} ({1}).'
                                  .format(expr.__class__.__name__, expr))
    return method(expr)


operators = {
    '+': lambda a, b: a + b,
    '-': lambda a, b: a - b,
    '*': lambda a, b: a * b,
    '/': lambda a, b: a / b,
    '%': lambda a, b: a % b,
}


def evaluate_BinaryOperator(expr):
    left_val = evaluate_expr(expr.left)
    right_val = evaluate_expr(expr.right)
    return operators[expr.operator](left_val, right_val)


def evaluate_Literal(expr):
    return expr.value
