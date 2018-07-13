"""Test compute.py"""
import unittest
from freezegun import freeze_time
from googleapiclient.http import HttpMock, HttpMockSequence
from gcloud_utils import dataproc
from mock import patch

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