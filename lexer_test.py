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
not_tok = ('NOT', 'not')
is_tok = ('IS', 'is')
null_tok = ('NULL', 'null')
true_tok = ('TRUE', 'true')
false_tok = ('FALSE', 'false')
select = ('SELECT', 'select')
as_tok = ('AS', 'as')
from_tok = ('FROM', 'from')
where = ('WHERE', 'where')
join = ('JOIN', 'join')
on = ('ON', 'on')
group = ('GROUP', 'group')
by = ('BY', 'by')
each = ('EACH', 'each')
lparen = ('LPAREN', '(')
rparen = ('RPAREN', ')')
comma = ('COMMA', ',')
dot = ('DOT', '.')


def num(n):
    return 'NUMBER', n


def ident(name):
    return 'ID', name


def string(s):
    return 'STRING', s


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

    def test_aggregates(self):
        self.assert_tokens(
            'SELECT MAX(foo) FROM bar',
            [select, ident('max'), lparen, ident('foo'), rparen, from_tok,
             ident('bar')]
        )

    def test_group_by(self):
        self.assert_tokens(
            'SELECT foo FROM bar GROUP BY baz',
            [select, ident('foo'), from_tok, ident('bar'), group, by,
             ident('baz')])

    def test_select_multiple_tales(self):
        self.assert_tokens(
            'SELECT foo FROM table1, table2',
            [select, ident('foo'), from_tok, ident('table1'), comma,
             ident('table2')])

    def test_subquery(self):
        self.assert_tokens(
            'SELECT foo FROM (SELECT val + 1 AS foo FROM test_table)',
            [select, ident('foo'), from_tok, lparen, select, ident('val'),
             plus, num(1), as_tok, ident('foo'), from_tok, ident('test_table'),
             rparen]
        )

    def test_join(self):
        self.assert_tokens(
            'SELECT foo FROM table1 JOIN table2 ON table1.bar = table2.bar',
            [select, ident('foo'), from_tok, ident('table1'), join,
             ident('table2'), on, ident('table1'), dot, ident('bar'), equals,
             ident('table2'), dot, ident('bar')]
        )

    def test_null_comparisons(self):
        self.assert_tokens(
            'SELECT foo IS NULL, bar IS NOT NULL FROM table1',
            [select, ident('foo'), is_tok, null_tok, comma, ident('bar'),
             is_tok, not_tok, null_tok, from_tok, ident('table1')]
        )

    def test_group_each_by(self):
        self.assert_tokens(
            'SELECT 0 FROM table GROUP EACH BY foo',
            [select, num(0), from_tok, ident('table'), group, each, by,
             ident('foo')]
        )

    def test_string_literal(self):
        self.assert_tokens(
            'SELECT foo = "hello", bar = \'world\' FROM table',
            [select, ident('foo'), equals, string('hello'), comma,
             ident('bar'), equals, string('world'), from_tok, ident('table')]
        )

    def test_other_literals(self):
        self.assert_tokens(
            'SELECT true, false, null',
            [select, true_tok, comma, false_tok, comma, null_tok])
