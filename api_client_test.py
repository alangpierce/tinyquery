import unittest

import api_client
import tinyquery


class ApiClientTest(unittest.TestCase):
    def test_table_management(self):
        service = api_client.TinyQueryApiClient(tinyquery.TinyQuery())
        service.tables().insert(
            projectId='test_project',
            datasetId='test_dataset',
            body={
                'tableReference': {
                    'projectId': 'test_project',
                    'datasetId': 'test_dataset',
                    'tableId': 'test_table'
                },
                'schema': {
                    'fields': [
                        {'name': 'foo', 'type': 'INTEGER'},
                        {'name': 'bar', 'type': 'BOOLEAN'},
                    ]
                }
            }).execute()

        table_info = service.tables().get(
            projectId='test_project', datasetId='test_dataset',
            tableId='test_table').execute()
        self.assertEqual(
            {'name': 'bar', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            table_info['schema']['fields'][1])

        service.tables().delete(
            projectId='test_project', datasetId='test_dataset',
            tableId='test_table').execute()

        try:
            service.tables().get(
                projectId='test_project', datasetId='test_dataset',
                tableId='test_table').execute()
            self.fail('Expected exception to be raised.')
        except api_client.FakeHttpError as e:
            self.assertTrue('404' in e.content)

        try:
            service.tables().delete(
                projectId='test_project', datasetId='test_dataset',
                tableId='test_table').execute()
            self.fail('Expected exception to be raised.')
        except api_client.FakeHttpError as e:
            self.assertTrue('404' in e.content)
