from ply import lex


tokens = [
    'PLUS',
    'MINUS',
    'TIMES',
    'DIVIDEDBY',
    'MOD',
    'SELECT',
    'NUMBER'
]


t_PLUS = r'\+'
t_MINUS = r'-'
t_TIMES = r'\*'
t_DIVIDEDBY = r'/'
t_MOD = r'%'
t_SELECT = r'SELECT'


def t_NUMBER(token):
    r'\d+'
    try:
        token.value = int(token.value)
    except ValueError:
        print("Integer value too large %d", token.value)
        token.value = 0
    return token


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
