"""Test compute.py"""
import unittest
from googleapiclient.http import HttpMockSequence
from gcloud_utils import dataproc

class TestDataproc(unittest.TestCase):
    """Test Compute Class"""

    def test_list_cluster_1(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'},open('tests/mock/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'},open('tests/mock/dataproc/list_clusters.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="",region="",http=http_mocked)
        result = dataproc_test.list_clusters()
        self.assertEqual(len(result),1)

    def test_list_cluster_0(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'},open('tests/mock/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'},open('tests/mock/dataproc/list_clusters_0.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="",region="",http=http_mocked)
        result = dataproc_test.list_clusters()
        self.assertEqual(len(result),0)

    def test_list_with_iterate_pages(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'},open('tests/mock/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'},open('tests/mock/dataproc/list_clusters_page_1.json', 'rb').read()),
            ({'status': '200'},open('tests/mock/dataproc/list_clusters_page_2.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="",region="",http=http_mocked)
        result = dataproc_test.list_clusters()
        self.assertEqual(len(result),2)

    def test_create_cluster(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'}, open('tests/mock/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'}, 'echo_request_body'),
            ({'status': '200'}, open('tests/mock/dataproc/cluster_not_done.json', 'rb').read()),
            ({'status': '200'}, open('tests/mock/dataproc/cluster_not_done.json', 'rb').read()),
            ({'status': '200'}, open('tests/mock/dataproc/cluster_done.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="project", region="region", http=http_mocked)
        result = dataproc_test.create_cluster("NAME", 2, ["B1", "B2"])
        expected = {
            u'clusterName': u'NAME',
            u'projectId': u'project'
            , u'config': {
                u'workerConfig': {
                    u'machineTypeUri': u'n1-standard-4',
                    u'numInstances': 2,
                    u'instanceNames': [u'B1', u'B2'],
                    u'diskConfig': {
                        u'numLocalSsds': 0,
                        u'bootDiskSizeGb': 10
                    }
                },
                u'masterConfig': {
                    u'machineTypeUri': u'n1-standard-4',
                    u'numInstances': 1,
                    u'instanceNames': [u'cluster-yarn-recsys-m'],
                    u'diskConfig': {
                        u'numLocalSsds': 0,
                        u'bootDiskSizeGb': 10
                    }
                },
                u'gceClusterConfig': {
                    u'subnetworkUri': u'default',
                    u'zoneUri': u'region-b'
                },
                u'configBucket': u''
                }
            }
        self.assertEqual(result,expected)

    def test_delete_cluster(self):
        http_mocked = HttpMockSequence([
            ({'status': '200'}, open('tests/mock/dataproc/first_request.json', 'rb').read()),
            ({'status': '200'}, 'echo_request_headers_as_json'),
            ({'status': '200'}, open('tests/mock/dataproc/cluster_deleting.json', 'rb').read())
        ])
        dataproc_test = dataproc.Dataproc(project="project", region="region", http=http_mocked)
        result = dataproc_test.delete_cluster("NAME")
        self.assertEqual(result['content-length'],u'0')