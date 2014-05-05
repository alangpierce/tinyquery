"""The parser turns a stream of tokens into an AST."""
from ply import yacc

import tq_ast
import lexer


tokens = lexer.tokens

precedence = (
    ('left', 'EQUALS', 'NOT_EQUAL', 'GREATER_THAN', 'LESS_THAN',
     'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL'),
    ('left', 'PLUS', 'MINUS'),
    ('left', 'TIMES', 'DIVIDED_BY', 'MOD'),
)


def p_select(p):
    """select : SELECT select_field_list
              | SELECT select_field_list FROM full_table_expr optional_where \
                    optional_group_by
    """
    if len(p) == 3:
        p[0] = tq_ast.Select(p[2], None, None, None, None)
    elif len(p) == 7:
        p[0] = tq_ast.Select(p[2], p[4], p[5], p[6], None)
    else:
        assert False, 'Unexpected number of captured tokens.'


def p_optional_where(p):
    """optional_where :
                      | WHERE expression
    """
    if len(p) == 1:
        p[0] = None
    else:
        p[0] = p[2]


def p_optional_group_by(p):
    """optional_group_by :
                         | GROUP BY id_list
                         | GROUP EACH BY id_list
    """
    if len(p) == 1:
        p[0] = None
    else:
        p[0] = p[len(p) - 1]


def p_id_list(p):
    """id_list : ID
               | id_list COMMA ID
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[3])
        p[0] = p[1]


def p_table_expr_table_or_union(p):
    """full_table_expr : aliased_table_expr_list"""
    # Unions are special in their naming rules, so we only call a table list a
    # union if it has at least two tables. Otherwise, it's just a table.
    if len(p[1]) == 1:
        p[0] = p[1][0]
    else:
        p[0] = tq_ast.TableUnion(p[1])


def p_table_expr_join(p):
    """full_table_expr : aliased_table_expr JOIN aliased_table_expr \
                            ON expression
    """
    p[0] = tq_ast.Join(p[1], p[3], p[5])


def p_table_expr_join_each(p):
    """full_table_expr : aliased_table_expr JOIN EACH aliased_table_expr \
                            ON expression
    """
    p[0] = tq_ast.Join(p[1], p[4], p[6])


def p_aliased_table_expr_list(p):
    """aliased_table_expr_list : aliased_table_expr
                               | aliased_table_expr_list COMMA \
                                    aliased_table_expr
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[3])
        p[0] = p[1]


def p_aliased_table_expr(p):
    """aliased_table_expr : table_expr
                          | table_expr ID
                          | table_expr AS ID"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        if isinstance(p[1], tq_ast.TableId):
            p[0] = tq_ast.TableId(p[1].name, p[len(p) - 1])
        elif isinstance(p[1], tq_ast.Select):
            p[0] = tq_ast.Select(p[1].select_fields, p[1].table_expr,
                                 p[1].where_expr, p[1].groups, p[len(p) - 1])
        else:
            assert False, 'Unexpected table_expr type: %s' % type(p[1])


def p_table_id(p):
    """table_expr : id_component_list"""
    p[0] = tq_ast.TableId(p[1], None)


def p_select_table_expression(p):
    """table_expr : select"""
    p[0] = p[1]


def p_table_expression_parens(p):
    """table_expr : LPAREN table_expr RPAREN"""
    p[0] = p[2]


def p_select_field_list(p):
    """select_field_list : select_field
                         | select_field_list COMMA select_field
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[3])
        p[0] = p[1]


def p_select_field(p):
    """select_field : expression
                    | expression ID
                    | expression AS ID
    """
    if len(p) > 2:
        alias = p[len(p) - 1]
    else:
        alias = None
    p[0] = tq_ast.SelectField(p[1], alias)


def p_expression_parens(p):
    """expression : LPAREN expression RPAREN"""
    p[0] = p[2]


def p_expression_unary(p):
    """expression : MINUS expression"""
    p[0] = tq_ast.UnaryOperator(p[1], p[2])


def p_expression_is_null(p):
    """expression : expression IS NULL"""
    p[0] = tq_ast.UnaryOperator('is_null', p[1])


def p_expression_is_not_null(p):
    """expression : expression IS NOT NULL"""
    p[0] = tq_ast.UnaryOperator('is_not_null', p[1])


def p_expression_binary(p):
    """expression : expression PLUS expression
                  | expression MINUS expression
                  | expression TIMES expression
                  | expression DIVIDED_BY expression
                  | expression MOD expression
                  | expression EQUALS expression
                  | expression NOT_EQUAL expression
                  | expression GREATER_THAN expression
                  | expression LESS_THAN expression
                  | expression GREATER_THAN_OR_EQUAL expression
                  | expression LESS_THAN_OR_EQUAL expression
                  | expression AND expression
                  | expression OR expression
    """
    p[0] = tq_ast.BinaryOperator(p[2], p[1], p[3])


def p_expression_func_call(p):
    """expression : ID LPAREN arg_list RPAREN"""
    p[0] = tq_ast.FunctionCall(p[1], p[3])


def p_arg_list(p):
    """arg_list :
                | expression
                | arg_list COMMA expression"""
    if len(p) == 1:
        p[0] = []
    elif len(p) == 2:
        p[0] = [p[1]]
    elif len(p) == 4:
        p[1].append(p[3])
        p[0] = p[1]
    else:
        assert False, 'Unexpected number of captured tokens.'


def p_expression_in(p):
    """expression : expression IN LPAREN constant_list RPAREN"""
    p[0] = tq_ast.FunctionCall('in', [p[1]] + p[4])


def p_constant_list(p):
    """constant_list : constant
                     | constant_list COMMA constant"""
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[3])
        p[0] = p[1]


def p_expression_constant(p):
    """expression : constant"""
    p[0] = p[1]


def p_int_literal(p):
    """constant : NUMBER"""
    p[0] = tq_ast.Literal(p[1])


def p_string_literal(p):
    """constant : STRING"""
    p[0] = tq_ast.Literal(p[1])


def p_true_literal(p):
    """constant : TRUE"""
    p[0] = tq_ast.Literal(True)


def p_false_literal(p):
    """constant : FALSE"""
    p[0] = tq_ast.Literal(False)


def p_null_literal(p):
    """constant : NULL"""
    p[0] = tq_ast.Literal(None)


def p_expr_id(p):
    """expression : id_component_list"""
    p[0] = tq_ast.ColumnId(p[1])


def p_id_component_list(p):
    """id_component_list : ID
                         | id_component_list DOT ID"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = p[1] + '.' + p[3]


def p_error(p):
    raise SyntaxError('Unexpected token: %s' % p)


def parse_text(text):
    parser = yacc.yacc()
    return parser.parse(text, lexer=lexer.get_lexer())
