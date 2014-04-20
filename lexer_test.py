import unittest

import lexer


plus = ('PLUS', '+')
minus = ('MINUS', '-')
times = ('TIMES', '*')
divided_by = ('DIVIDED_BY', '/')
mod = ('MOD', '%')
equals = ('EQUALS', '=')
not_equal = ('NOT_EQUAL', '!=')
greater_than = ('GREATER_THAN', '>')
less_than = ('LESS_THAN', '<')
greater_than_or_equal = ('GREATER_THAN_OR_EQUAL', '>=')
less_than_or_equal = ('LESS_THAN_OR_EQUAL', '<=')
select = ('SELECT', 'select')
as_tok = ('AS', 'as')
from_tok = ('FROM', 'from')
where = ('WHERE', 'where')
group = ('GROUP', 'group')
by = ('BY', 'by')
lparen = ('LPAREN', '(')
rparen = ('RPAREN', ')')
comma = ('COMMA', ',')


def num(n):
    return 'NUMBER', n

def ident(name):
    return 'ID', name


class LexerTest(unittest.TestCase):
    def assert_tokens(self, text, expected_tokens):
        tokens = lexer.lex_text(text)
        self.assertEqual(expected_tokens,
                         [(tok.type, tok.value) for tok in tokens])

    def test_lex_simple_select(self):
        self.assert_tokens('SELECT 0', [select, num(0)])

    def test_lex_addition(self):
        self.assert_tokens('SELECT 1 + 2', [select, num(1), plus, num(2)])

    def test_arithmetic_operators(self):
        self.assert_tokens(
            'SELECT 0 + 1 - 2 * 3 / 4 % 5',
            [select, num(0), plus, num(1), minus, num(2), times, num(3),
             divided_by, num(4), mod, num(5)])

    def test_select_from_table(self):
        self.assert_tokens(
            'SELECT foo FROM bar',
            [select, ident('foo'), from_tok, ident('bar')])

    def test_comparisons(self):
        self.assert_tokens(
            'SELECT 1 > 2 <= 3 = 4 != 5 < 6 >= 7',
            [select, num(1), greater_than, num(2), less_than_or_equal, num(3),
             equals, num(4), not_equal, num(5), less_than, num(6),
             greater_than_or_equal, num(7)]
        )

    def test_parens(self):
        self.assert_tokens(
            'SELECT 2 * (3 + 4)',
            [select, num(2), times, lparen, num(3), plus, num(4), rparen]
        )

    def test_negative_numbers(self):
        self.assert_tokens(
            'SELECT -5',
            [select, minus, num(5)]
        )

    def test_function_call(self):
        self.assert_tokens(
            'SELECT ABS(-5), POW(x, 3), NOW() FROM test_table',
            [select, ident('abs'), lparen, minus, num(5), rparen, comma,
             ident('pow'), lparen, ident('x'), comma, num(3), rparen, comma,
             ident('now'), lparen, rparen, from_tok, ident('test_table')]
        )

    def test_select_where(self):
        self.assert_tokens(
            'SELECT foo FROM bar WHERE foo > 3',
            [select, ident('foo'), from_tok, ident('bar'), where, ident('foo'),
             greater_than, num(3)]
        )

    def test_multiple_select(self):
        self.assert_tokens(
            'SELECT a AS foo, b bar, a + 1 baz FROM test_table',
            [select, ident('a'), as_tok, ident('foo'), comma, ident('b'),
             ident('bar'), comma, ident('a'), plus, num(1), ident('baz'),
             from_tok, ident('test_table')])

    def test_group_by(self):
        self.assert_tokens(
            'SELECT foo FROM bar GROUP BY baz',
            [select, ident('foo'), from_tok, ident('bar'), group, by,
             ident('baz')])
