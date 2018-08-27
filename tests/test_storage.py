"""Test Storage Module"""
import unittest
from googleapiclient.http import HttpMockSequence
from gcloud_utils.storage import Storage
class TestStorage(unittest.TestCase):
    "Test Storage module"

    def test_get_abs_path(self):
        """Test get abs path"""
        http_mocked = HttpMockSequence([
            ({'status': '200','content-type':'application/json'},open('tests/mock/storage/first_result.json', 'rb').read())
            ])

        storage_test = Storage(bucket="teste", http=http_mocked)
        abs_path = storage_test.get_abs_path("path/something/inside/bucket")
        expected = "gs://teste/path/something/inside/bucket"

        self.assertEqual(abs_path, expected)

if __name__ == '__main__':
    unittest.main()