"""Test Storage Module"""
import unittest
import os
from google.cloud import storage
from gcloud_utils.storage import Storage

try:
    import mock
except ImportError:
    import unittest.mock as mock

class TestStorage(unittest.TestCase):
    "Test Storage module"

    def setUp(self):
        try:
            del os.environ[Storage.CREDENTIAL_ENV]
        except:
            pass
        Storage._MODEL_CLIENT = storage

    def test_get_abs_path(self):
        os.environ[Storage.CREDENTIAL_ENV] = "./tests/fixtures/mock_credentials.json"
        bucket_name = "teste_bucket"
        expected = "gs://{}/path/something/inside/bucket".format(bucket_name)
        bucket_mock = mock.Mock()
        bucket_mock.name = bucket_name
        client_mock = mock.Mock(**{"get_bucket.return_value": bucket_mock})
        Storage._MODEL_CLIENT.Client = mock.Mock(**{"from_service_account_json.return_value": client_mock})
        storage_test = Storage(bucket=bucket_name)
        abs_path = storage_test.get_abs_path("path/something/inside/bucket")

        self.assertEqual(abs_path, expected)
    
    def test_rename_files(self):
        expected = "gs://teste/path/something/inside/bucket"

        blob_mock1 = mock.Mock()
        blob_mock1.name = "ttt_est1"
        expected_full_name1 = "l_est1"
        blob_mock2 = mock.Mock()
        blob_mock2.name = "ttt_est2"
        expected_full_name2 = "l_est2"
        blob_mock3 = mock.Mock()
        blob_mock3.name = "ttt_est3"
        expected_full_name3 = "l_est3"
        
        bucket_name = "teste_bucket"
        bucket_mock = mock.Mock(**{"list_blobs.return_value": [blob_mock1, blob_mock2, blob_mock3]})

        client_mock = mock.Mock(**{"get_bucket.return_value": bucket_mock})

        storage_test = Storage(bucket_name, client_mock)
        storage_test.rename_files("ttt", "l")

        bucket_mock.list_blobs.assert_called_once_with(prefix="ttt")

        bucket_mock.rename_blob.assert_has_calls([
            mock.call(blob_mock1, expected_full_name1),
            mock.call(blob_mock2, expected_full_name2),
            mock.call(blob_mock3, expected_full_name3)
        ])