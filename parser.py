
from ply import yacc

import tq_ast
import lexer


tokens = lexer.tokens

precedence = (
    ('left', 'PLUS', 'MINUS'),
    ('left', 'TIMES', 'DIVIDEDBY', 'MOD'),
)


def p_select(p):
    """select : SELECT expression"""
    p[0] = tq_ast.Select(p[2])


def p_expression_binary(p):
    """expression : expression PLUS expression
                  | expression MINUS expression
                  | expression TIMES expression
                  | expression DIVIDEDBY expression
                  | expression MOD expression
    """
    p[0] = tq_ast.BinaryOperator(p[2], p[1], p[3])


def p_int_literal(p):
    """expression : NUMBER"""
    p[0] = tq_ast.Literal(p[1])


def p_error(p):
    raise SyntaxError('Syntax error!')


def parse_text(text):
    parser = yacc.yacc()
    return parser.parse(text, lexer=lexer.get_lexer())
