"""The lexer turns a query string into a stream of tokens."""

from ply import lex


# The keys here should all be lowercase, since we convert tokens to lowercase
# before looking them up in this map.
reserved_words = {
    'select': 'SELECT',
    'as': 'AS',
    'from': 'FROM',
    'where': 'WHERE',
    'join': 'JOIN',
    'on': 'ON',
    'group': 'GROUP',
    'by': 'BY',
    'each': 'EACH',
    'and': 'AND',
    'or': 'OR',
    'not': 'NOT',
    'is': 'IS',
    'null': 'NULL',
    'true': 'TRUE',
    'false': 'FALSE',
    'in': 'IN'
}

tokens = [
    'PLUS',
    'MINUS',
    'TIMES',
    'DIVIDED_BY',
    'MOD',
    'EQUALS',
    'NOT_EQUAL',
    'GREATER_THAN',
    'LESS_THAN',
    'GREATER_THAN_OR_EQUAL',
    'LESS_THAN_OR_EQUAL',
    'LPAREN',
    'RPAREN',
    'COMMA',
    'DOT',
    'NUMBER',
    'ID',
    'STRING'
] + reserved_words.values()


t_PLUS = r'\+'
t_MINUS = r'-'
t_TIMES = r'\*'
t_DIVIDED_BY = r'/'
t_MOD = r'%'
t_EQUALS = r'='
t_NOT_EQUAL = r'!='
t_GREATER_THAN = r'>'
t_LESS_THAN = r'<'
t_GREATER_THAN_OR_EQUAL = r'>='
t_LESS_THAN_OR_EQUAL = r'<='
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_COMMA = r','
t_DOT = r'\.'


def string_regex(delim):
    return '{0}[^{0}]*{0}'.format(delim)


@lex.TOKEN(string_regex("'") + '|' + string_regex('"'))
def t_STRING(t):
    # TODO: Escaped quotation marks and other escapes.
    t.value = t.value[1:-1]
    return t


def t_NUMBER(token):
    r"""\d+"""
    try:
        token.value = int(token.value)
    except ValueError:
        print("Integer value too large %d", token.value)
        token.value = 0
    return token


# Taken from example at http://www.dabeaz.com/ply/ply.html#ply_nn6
def t_ID(t):
    r"""[a-zA-Z_][a-zA-Z_0-9]*"""
    # Canonicalize on lower-case ID tokens for everything.
    t.value = t.value.lower()
    t.type = reserved_words.get(t.value, 'ID')
    return t


def t_COMMENT(t):
    r"""--.*"""


t_ignore = ' \t\n'


def t_error(t):
    raise SyntaxError('Unexpected token: ' + str(t))


def lex_text(text):
    lexer = get_lexer()
    lexer.input(text)
    result = []
    while True:
        token = lex.token()
        if token:
            result.append(token)
        else:
            break
    return result


def get_lexer():
    return lex.lex()
