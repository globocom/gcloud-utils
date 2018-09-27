"""Test Storage Module"""
import unittest
import os
from googleapiclient.http import HttpMockSequence
from gcloud_utils.storage import Storage

class TestStorage(unittest.TestCase):
    "Test Storage module"

    def test_get_abs_path(self):
        """Test get abs path"""
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./tests/fixtures/mock_credentials.json"

        http_mocked = HttpMockSequence([
            ({'status': '200','content-type':'application/json'},open('tests/fixtures/storage/first_result.json', 'rb').read())
            ])

        storage_test = Storage(bucket="teste", http=http_mocked)
        abs_path = storage_test.get_abs_path("path/something/inside/bucket")
        expected = "gs://teste/path/something/inside/bucket"

        self.assertEqual(abs_path, expected)