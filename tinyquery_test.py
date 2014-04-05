import unittest

import tinyquery


class TinyQueryTest(unittest.TestCase):
    def test_select_literal(self):
        tq = tinyquery.TinyQuery()
        result = tq.evaluate('SELECT 0')
        self.assertEqual(0, result)
