from ply import lex


tokens = [
    'SELECT',
    'NUMBER'
]


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
    lex.lex()
    lex.input(text)
    result = []
    while True:
        token = lex.token()
        if token:
            result.append(token)
        else:
            break
    return result
