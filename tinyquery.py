"""Implementation of the TinyQuery service."""
import collections

import compiler
import context
import evaluator
import tq_types


class TinyQueryError(Exception):
    # TODO: Use BigQuery-specific error codes here.
    pass


class TinyQuery(object):
    def __init__(self):
        self.tables_by_name = {}
        self.next_job_num = 0
        self.job_map = {}

    def load_table_or_view(self, table):
        """Create a table."""
        self.tables_by_name[table.name] = table

    def load_table_from_csv(self, table_name, raw_schema, filename):
        result_table = self.make_empty_table(table_name, raw_schema)
        with open(filename, 'r') as f:
            for line in f:
                if line[-1] == '\n':
                    line = line[:-1]
                tokens = line.split(',')
                assert len(tokens) == len(result_table.columns), (
                    'Expected {} tokens on line {}, but got {}'.format(
                        len(result_table.columns), line, len(tokens)))
                for token, column in zip(tokens,
                                         result_table.columns.itervalues()):
                    if column.type == tq_types.INT:
                        token = int(token)
                    elif column.type == tq_types.FLOAT:
                        token = float(token)
                    elif token == 'null':
                        token = None
                    column.values.append(token)
                result_table.num_rows += 1
        self.load_table_or_view(result_table)

    def make_empty_table(self, table_name, raw_schema):
        columns = collections.OrderedDict()
        for field in raw_schema['fields']:
            # TODO: Handle the mode here. We should default to NULLABLE, but
            # allow other specifiers.
            # TODO: Validate that the type is legal. Currently we take
            # advantage of the fact that type names match the types defined in
            # tq_types.py.
            columns[field['name']] = context.Column(field['type'], [])
        return Table(table_name, 0, columns)

    def make_view(self, view_name, query):
        # TODO: Figure out the schema by compiling the query, and refactor the
        # code so that the compiler can use the schema instead of expecting
        # every TableId to have actual Columns. For now, we just validate that
        # the view works, and things will break later if the view is actually
        # used.
        compiler.compile_text(query, self.tables_by_name)
        return View(view_name, query)

    def get_all_tables(self):
        return self.tables_by_name

    def get_table_names_for_dataset(self, dataset):
        # TODO(alan): Improve this to use a more first-class dataset structure.
        return [full_table[len(dataset + '.'):]
                for full_table in self.tables_by_name.iterkeys()
                if full_table.startswith(dataset + '.')]

    def get_all_table_info_in_dataset(self, project_id, dataset):
        """Gets a "table info" dictionary for each table, sorted by name.

        In practice, this is a bit wasteful when it is used for multiple pages.
        """
        return [self.get_short_table_info(project_id, dataset, table)
                for table in sorted(self.get_table_names_for_dataset(dataset))]

    def get_short_table_info(self, project_id, dataset, table_name):
        """Returns the format from bq_service.tables().list()."""
        return {
            'tableReference': {
                'projectId': project_id,
                'datasetId': dataset,
                'tableId': table_name
            }
        }

    def get_table_info(self, project, dataset, table_name):
        # TODO(alan): Don't just ignore the project parameter.
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
            },
            'tableReference': {
                'projectId': project,
                'datasetId': dataset,
                'tableId': table_name
            }
        }

    def get_table(self, dataset, table_name):
        """Returns the tinyquery.Table with the given dataset and name."""
        return self.tables_by_name[dataset + '.' + table_name]

    def delete_table(self, dataset, table_name):
        del self.tables_by_name[dataset + '.' + table_name]

    def evaluate_query(self, query):
        select_ast = compiler.compile_text(query, self.tables_by_name)
        select_evaluator = evaluator.Evaluator(self.tables_by_name)
        return select_evaluator.evaluate_select(select_ast)

    def create_job(self, project_id, job_object):
        """Create a job with the given status and return the info for it."""
        job_id = 'job:%s' % self.next_job_num
        self.next_job_num += 1
        job_object.job_info['jobReference'] = {
            'projectId': project_id,
            'jobId': job_id
        }
        self.job_map[job_id] = job_object
        return job_object.job_info

    def run_query_job(self, project_id, query, dest_dataset, dest_table_name,
                      create_disposition, write_disposition):
        query_result_context = self.evaluate_query(query)
        query_result_table = self.table_from_context('query_results',
                                                     query_result_context)

        if dest_dataset is not None and dest_table_name is not None:
            dest_full_table_name = dest_dataset + '.' + dest_table_name
            self.copy_table(query_result_table, dest_full_table_name,
                            create_disposition, write_disposition)

        return self.create_job(project_id, QueryJob({
            'status': {
                'state': 'DONE'
            },
            'statistics': {
                'query': {
                    'totalBytesProcessed': '0'
                }
            }
        }, query_result_table))

    @staticmethod
    def table_from_context(table_name, ctx):
        return Table(table_name, ctx.num_rows, collections.OrderedDict(
            (col_name, column)
            for (_, col_name), column in ctx.columns.iteritems()
        ))

    def run_copy_job(self, project_id, src_dataset, src_table_name,
                     dest_dataset, dest_table_name, create_disposition,
                     write_disposition):
        # TODO: Handle errors in the same way as BigQuery.
        src_full_table_name = src_dataset + '.' + src_table_name
        dest_full_table_name = dest_dataset + '.' + dest_table_name
        src_table = self.tables_by_name[src_full_table_name]
        self.copy_table(src_table, dest_full_table_name, create_disposition,
                        write_disposition)
        return self.create_job(project_id, CopyJob({
            'status': {
                'state': 'DONE'
            },
        }))

    def copy_table(self, src_table, dest_table_name, create_disposition,
                   write_disposition):
        """Write the given Table object to the destination table name."""
        if dest_table_name not in self.tables_by_name:
            if create_disposition == 'CREATE_NEVER':
                raise TinyQueryError('CREATE_NEVER specified, but table did '
                                     'not exist: {}'.format(dest_table_name))
            self.load_empty_table_from_template(dest_table_name, src_table)

        # TODO: Handle schema differences and raise errors with illegal schema
        # updates.
        dest_table = self.tables_by_name[dest_table_name]
        if dest_table.num_rows > 0:
            if write_disposition == 'WRITE_EMPTY':
                raise TinyQueryError(
                    'WRITE_EMPTY was specified, but the table {} was not '
                    'empty.'.format(dest_table_name))
            if write_disposition == 'WRITE_TRUNCATE':
                self.clear_table(dest_table)
        self.append_to_table(src_table, dest_table)

    def load_empty_table_from_template(self, table_name, template_table):
        columns = collections.OrderedDict(
            (col_name, context.Column(col.type, []))
            for col_name, col in template_table.columns.iteritems()
        )
        table = Table(table_name, 0, columns)
        self.load_table_or_view(table)

    @staticmethod
    def clear_table(table):
        table.num_rows = 0
        for column in table.columns.itervalues():
            column.values[:] = []

    @staticmethod
    def append_to_table(src_table, dest_table):
        dest_table.num_rows += src_table.num_rows
        for col_name, column in dest_table.columns.iteritems():
            if col_name in src_table.columns:
                column.values.extend(src_table.columns[col_name].values)
            else:
                column.values.extend([None] * src_table.num_rows)

    def get_job_info(self, job_id):
        # Raise a KeyError if the table doesn't exist.
        return self.job_map[job_id].job_info

    def get_query_result_table(self, job_id):
        # TODO: Return an appropriate error if not a query job.
        return self.job_map[job_id].query_results


class Table(object):
    """Information containing metadata and contents of a table.

    Fields:
        name: The name of the table.
        num_rows: The number of rows in the table.
        columns: An OrderedDict mapping column name to Column. Note that unlike
            in Context objects, the column name is just a string and does not
            include a table component.
    """
    def __init__(self, name, num_rows, columns):
        assert isinstance(columns, collections.OrderedDict)
        for col_name, column in columns.iteritems():
            assert isinstance(col_name, basestring)
            assert len(column.values) == num_rows, (
                'Column %s had %s rows, expected %s.' % (
                    col_name, len(column.values), num_rows))
        self.name = name
        self.num_rows = num_rows
        self.columns = columns

    def __repr__(self):
        return 'Table({}, {}, {})'.format(self.name, self.num_rows,
                                          self.columns)


class View(object):
    """Information about a view (a virtual table defined by a query).

    Fields:
        name: The name of the view.
        query: The query string for the view.
    """
    def __init__(self, name, query):
        self.name = name
        self.query = query


class QueryJob(collections.namedtuple('QueryJob', ['job_info',
                                                   'query_results'])):
    pass


class CopyJob(collections.namedtuple('CopyJob', ['job_info'])):
    pass
