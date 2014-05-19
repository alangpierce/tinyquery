"""TinyQuery wrapper that acts like BigQuery's Python API Client.

This can be used in place of the value returned by apiclient.discovery.build().
"""
import collections
import functools
import json
import context
import tinyquery


class TinyQueryApiClient(object):
    def __init__(self, tq_service):
        self.tq_service = tq_service

    def tables(self):
        return TableServiceApiClient(self.tq_service)

    def jobs(self):
        return JobServiceApiClient(self.tq_service)


class FakeHttpError(Exception):
    """Replace this with a real HttpError class if your code catches one."""
    def __init__(self, response, content):
        super(FakeHttpError, self).__init__(content)
        self.response = response
        self.content = content


class FakeHttpRequest(object):
    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def execute(self):
        return self.func(*self.args, **self.kwargs)


def http_request_provider(func):
    """Given a function, turn

    For example, if we decorate a function get(projectId, datasetId, tableId),
    the returned function will return a FakeHttpRequest object that, when
    executed, will actually call the get function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return FakeHttpRequest(func, args, kwargs)
    return wrapper


class TableServiceApiClient(object):
    def __init__(self, tq_service):
        """Service object for creating and managing tables.

        :type tq_service: tinyquery.TinyQuery
        """
        self.tq_service = tq_service

    @http_request_provider
    def insert(self, projectId, datasetId, body):
        """Create an empty table."""
        table_reference = body['tableReference']
        raw_schema = body['schema']
        table_name = (table_reference['datasetId'] + '.' +
                      table_reference['tableId'])
        table = self.tq_service.make_empty_table(table_name, raw_schema)
        self.tq_service.load_table(table)

    @http_request_provider
    def get(self, projectId, datasetId, tableId):
        try:
            return self.tq_service.get_table_info(datasetId, tableId)
        except KeyError:
            raise FakeHttpError(None, json.dumps({
                'error': {
                    'code': 404,
                    'message': 'Table not found: %s.%s' % (datasetId, tableId)
                }
            }))

    @http_request_provider
    def delete(self, projectId, datasetId, tableId):
        try:
            return self.tq_service.delete_table(datasetId, tableId)
        except KeyError:
            raise FakeHttpError(None, json.dumps({
                'error': {
                    'code': 404,
                    'message': 'Table not found: %s.%s' % (datasetId, tableId)
                }
            }))


class JobServiceApiClient(object):
    def __init__(self, tq_service):
        """Service object for creating and managing jobs.

        :type tq_service: tinyquery.TinyQuery
        """
        self.tq_service = tq_service

    @http_request_provider
    def insert(self, projectId, body):
        if 'query' in body['configuration']:
            config = body['configuration']['query']
            query = config['query']
            dest_dataset, dest_table = self._get_config_table(
                config, 'destinationTable')
            create_disposition = config.get('createDisposition',
                                            'CREATE_IF_NEEDED')
            write_disposition = config.get('writeDisposition', 'WRITE_EMPTY')
            return self.tq_service.run_query_job(
                projectId, query, dest_dataset, dest_table, create_disposition,
                write_disposition)
        elif 'copy' in body['configuration']:
            config = body['configuration']['copy']
            src_dataset, src_table = self._get_config_table(
                config, 'sourceTable')
            dest_dataset, dest_table = self._get_config_table(
                config, 'destinationTable')
            create_disposition = config.get('createDisposition',
                                            'CREATE_IF_NEEDED')
            write_disposition = config.get('writeDisposition', 'WRITE_EMPTY')
            return self.tq_service.run_copy_job(
                projectId, src_dataset, src_table, dest_dataset, dest_table,
                create_disposition, write_disposition)
        else:
            assert False, 'Unknown job type: {}'.format(
                body['configuration'].keys())

    @staticmethod
    def _get_config_table(config, key):
        """Return the dataset_id, table_id, if any."""
        dest_table_config = config.get(key)
        if dest_table_config:
            return dest_table_config['datasetId'], dest_table_config['tableId']
        else:
            return None, None

    @http_request_provider
    def get(self, projectId, jobId):
        return self.tq_service.get_job_info(jobId)

    @http_request_provider
    def getQueryResults(self, projectId, jobId):
        result_table = self.tq_service.get_query_result_table(jobId)
        result_rows = []
        for i in xrange(result_table.num_rows):
            field_values = [{'v': str(col.values[i])}
                            for col in result_table.columns.itervalues()]
            result_rows.append({
                'f': field_values
            })

        # TODO: Also add the schema.
        return {
            'rows': result_rows
        }
