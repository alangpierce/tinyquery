"""Implementation of the TinyQuery service."""
import collections

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

    def get_table_info(self, dataset, table_name):
        # Will throw KeyError if the table doesn't exist.
        table = self.tables_by_name[dataset + '.' + table_name]
        schema_fields = []
        for col_name, column in table.columns.iteritems():
            schema_fields.append({
                'name': col_name,
                'type': column.type,
                'mode': 'NULLABLE'
            })

        return {
            'schema': {
                'fields': schema_fields
            }
        }

    def delete_table(self, dataset, table_name):
        del self.tables_by_name[dataset + '.' + table_name]

    def evaluate_query(self, query):
        select_ast = compiler.compile_text(query, self.tables_by_name)
        select_evaluator = evaluator.Evaluator(self.tables_by_name)
        return select_evaluator.evaluate_select(select_ast)

    def create_job(self, project_id, status):
        """Create a job with the given status and return an ID for it."""
        job_id = 'job:%s' % self.next_job_num
        self.next_job_num += 1
        self.job_map[job_id] = {
            'status': {
                'state': status
            },
            'jobReference': {
                'projectId': project_id,
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


class Table(collections.namedtuple('Table', ['name', 'num_rows', 'columns'])):
    """Information containing metadata and contents of a table.

    Fields:
        columns: A dict mapping column name to column.
    """
    def __init__(self, name, num_rows, columns):
        assert isinstance(columns, collections.OrderedDict)
        for name, column in columns.iteritems():
            assert len(column.values) == num_rows, (
                'Column %s had %s rows, expected %s.' % (
                    name, len(column.values), num_rows))
        super(Table, self).__init__()
