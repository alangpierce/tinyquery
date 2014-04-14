"""The parser turns a stream of tokens into an AST."""
from ply import yacc

import tq_ast
import lexer


tokens = lexer.tokens

precedence = (
    ('left', 'PLUS', 'MINUS'),
    ('left', 'TIMES', 'DIVIDED_BY', 'MOD'),
)


def p_select(p):
    """select : SELECT select_field_list
              | SELECT select_field_list FROM table_expr"""
    if len(p) == 3:
        p[0] = tq_ast.Select(p[2], None)
    elif len(p) == 5:
        p[0] = tq_ast.Select(p[2], p[4])
    else:
        assert False, 'Unexpected number of captured tokens.'


def p_table_expr_id(p):
    """table_expr : ID"""
    p[0] = tq_ast.TableId(p[1])


def p_select_field_list(p):
    """select_field_list : expression"""
    p[0] = [tq_ast.SelectField(p[1])]


def p_expression_binary(p):
    """expression : expression PLUS expression
                  | expression MINUS expression
                  | expression TIMES expression
                  | expression DIVIDED_BY expression
                  | expression MOD expression
    """
    p[0] = tq_ast.BinaryOperator(p[2], p[1], p[3])


def p_int_literal(p):
    """expression : NUMBER"""
    p[0] = tq_ast.Literal(p[1])


def p_expr_id(p):
    """expression : ID"""
    p[0] = tq_ast.ColumnId(p[1])


def p_error(p):
    raise SyntaxError('Unexpected token: %s' % p)


def parse_text(text):
    parser = yacc.yacc()
    return parser.parse(text, lexer=lexer.get_lexer())
