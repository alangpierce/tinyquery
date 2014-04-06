"""Implementation of the TinyQuery service."""
import evaluator


class TinyQuery(object):
    def evaluate(self, query):
        return evaluator.evaluate_select(query)
