"""Test compute.py"""
import unittest
from googleapiclient.http import HttpMockSequence
from gcloud_utils import dataproc
from freezegun import freeze_time

class TestDataproc(unittest.TestCase):
    """Test Compute Class"""

    def test_list_cluster_1(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'},open('tests/fixtures/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'},open('tests/fixtures/dataproc/list_clusters.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="",region="",http=http_mocked)
        result = dataproc_test.list_clusters()
        self.assertEqual(len(result),1)

    def test_list_cluster_0(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'},open('tests/fixtures/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'},open('tests/fixtures/dataproc/list_clusters_0.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="",region="",http=http_mocked)
        result = dataproc_test.list_clusters()
        self.assertEqual(len(result),0)

    def test_list_with_iterate_pages(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'},open('tests/fixtures/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'},open('tests/fixtures/dataproc/list_clusters_page_1.json', 'rb').read()),
            ({'status': '200'},open('tests/fixtures/dataproc/list_clusters_page_2.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="",region="",http=http_mocked)
        result = dataproc_test.list_clusters()
        self.assertEqual(len(result),2)

    def test_create_cluster(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'}, 'echo_request_body'),
            ({'status': '200'}, open('tests/fixtures/dataproc/cluster_not_done.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/dataproc/cluster_not_done.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/dataproc/cluster_done.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="project", region="region", http=http_mocked)
        result = dataproc_test.create_cluster("NAME", 2, ["B1", "B2"])
        expected = {
            'clusterName': 'NAME',
            'projectId': 'project',
            'config': {
                'workerConfig': {
                    'machineTypeUri': 'n1-standard-4',
                    'numInstances': 2,
                    'instanceNames': ['B1', 'B2'],
                    'diskConfig': {
                        'numLocalSsds': 0,
                        'bootDiskSizeGb': 10
                    }
                },
                'masterConfig': {
                    'machineTypeUri': 'n1-standard-4',
                    'numInstances': 1,
                    'instanceNames': ['cluster-yarn-recsys-m'],
                    'diskConfig': {
                        'numLocalSsds': 0,
                        'bootDiskSizeGb': 10
                    }
                },
                'gceClusterConfig': {
                    'subnetworkUri': 'default',
                    'zoneUri': 'region-b'
                },
                'configBucket': '',
                'softwareConfig': {
                    'imageVersion': '1.2.54-deb8'
                },
            },
        }
        self.assertEqual(result,expected)

    def test_delete_cluster(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'}, 'echo_request_headers_as_json'),
            ({'status': '200'}, open('tests/fixtures/dataproc/cluster_deleting.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="project", region="region", http=http_mocked)
        result = dataproc_test.delete_cluster("NAME")
        self.assertEqual(result['content-length'],'0')

    @freeze_time("1994-04-27 12:00:01")
    def test_submit_job_success(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'}, 'echo_request_body'),
            ({'status': '200'}, open('tests/fixtures/dataproc/job_status_running.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/dataproc/job_status_running.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/dataproc/job_status_done.json', 'rb').read())
        ])

        dataproc_test = dataproc.Dataproc(project="project", region="region", http=http_mocked)
        result = dataproc_test.submit_job(
            "CLUSTER",
            "BUCKET",
            ["/path/to/jar/jarname.jar"],
            "main",
            ["arg1", "arg2"]
            )

        body_request_expected = {
            'projectId': 'project',
            'job': {
                'placement': {'clusterName': 'CLUSTER'},
                'sparkJob': {
                    'jarFileUris': ['/path/to/jar/jarname.jar'],
                    'mainClass': 'main',
                    'args': ['arg1', 'arg2']},
                'reference': {'jobId': 'main_1994_04_27_12_00_01'}
            }
        }

        self.assertEqual(body_request_expected, result)

    @freeze_time("1994-04-27 12:00:01")
    def test_submit_job_error(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'}, 'echo_request_body'),
            ({'status': '200'}, open('tests/fixtures/dataproc/job_status_running.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/dataproc/job_status_error.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/dataproc/job_status_done.json', 'rb').read())
        ])

        dataproc_test = dataproc.Dataproc(project="project", region="region", http=http_mocked)

        with self.assertRaises(Exception):
            dataproc_test.submit_job("", "", [""], "", [])
