import unittest

import evaluator


class EvaluatorTest(unittest.TestCase):
    def test_simple_arithmetic(self):
        self.assertEqual(3, evaluator.evaluate_select('SELECT 1 + 2'))
