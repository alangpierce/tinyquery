"""Implementation of the TinyQuery service."""
import compiler
import evaluator


class TinyQuery(object):
    def __init__(self):
        self.tables_by_name = {}
        self.next_job_num = 0
        self.job_map = {}

    def load_table(self, table):
        """Create a table.

        Arguments:
            name: The name of the table.
            data: A dict mapping column name to list of values.
        """
        self.tables_by_name[table.name] = table

    def get_all_tables(self):
        return self.tables_by_name

    def evaluate_query(self, query):
        select_ast = compiler.compile_text(query, self.tables_by_name)
        select_evaluator = evaluator.Evaluator(self.tables_by_name)
        return select_evaluator.evaluate_select(select_ast)

    def create_job(self, status):
        """Create a job with the given status and return an ID for it."""
        job_id = 'job:%s' % self.next_job_num
        self.next_job_num += 1
        self.job_map[job_id] = {
            'status': {
                'state': 'DONE'
            },
            'jobReference': {
                'jobId': job_id
            },
            'statistics': {
                'query': {
                    'totalBytesProcessed': '0'
                }
            }
        }
        return self.job_map[job_id]

    def get_job_info(self, job_id):
        return self.job_map[job_id]
