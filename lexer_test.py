import unittest

import lexer


def num(n):
    return 'NUMBER', n


select = ('SELECT', 'SELECT')


class LexerTest(unittest.TestCase):
    def test_lex_simple_select(self):
        self.assert_tokens('SELECT 0', [select, num(0)])

    def assert_tokens(self, text, expected_tokens):
        tokens = lexer.lex_text(text)
        self.assertEqual(expected_tokens,
                         [(tok.type, tok.value) for tok in tokens])
