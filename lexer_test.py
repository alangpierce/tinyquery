import unittest

import lexer


plus = ('PLUS', '+')
minus = ('MINUS', '-')
times = ('TIMES', '*')
dividedby = ('DIVIDEDBY', '/')
mod = ('MOD', '%')
select = ('SELECT', 'select')
from_tok = ('FROM', 'from')


def num(n):
    return 'NUMBER', n

def ident(name):
    return 'ID', name


class LexerTest(unittest.TestCase):
    def test_lex_simple_select(self):
        self.assert_tokens('SELECT 0', [select, num(0)])

    def test_lex_addition(self):
        self.assert_tokens('SELECT 1 + 2', [select, num(1), plus, num(2)])

    def test_arithmetic_operators(self):
        self.assert_tokens(
            'SELECT 0 + 1 - 2 * 3 / 4 % 5',
            [select, num(0), plus, num(1), minus, num(2), times, num(3),
             dividedby, num(4), mod, num(5)])

    def test_select_from_table(self):
        self.assert_tokens(
            'SELECT foo FROM bar',
            [select, ident('foo'), from_tok, ident('bar')])

    def assert_tokens(self, text, expected_tokens):
        tokens = lexer.lex_text(text)
        self.assertEqual(expected_tokens,
                         [(tok.type, tok.value) for tok in tokens])
