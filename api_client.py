"""TinyQuery wrapper that acts like BigQuery's Python API Client.

This can be used in place of the value returned by apiclient.discovery.build().
"""
import functools
import json


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
        pass

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
        pass


class JobServiceApiClient(object):
    def __init__(self, tq_service):
        """Service object for creating and managing jobs.

        :type tq_service: tinyquery.TinyQuery
        """
        self.tq_service = tq_service

    @http_request_provider
    def insert(self, projectId, body):
        return self.tq_service.create_job(projectId, 'DONE')

    @http_request_provider
    def get(self, projectId, jobId):
        return self.tq_service.get_job_info(jobId)

    @http_request_provider
    def getQueryResults(self, projectId, jobId):
        pass
