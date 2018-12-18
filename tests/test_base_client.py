"""Test BaseClient Module"""
import unittest
import os
from gcloud_utils.base_client import BaseClient
try:
    import mock
except ImportError:
    import unittest.mock as mock

class TestBaseClient(unittest.TestCase):
    "Test BaseClient module"

    def setUp(self):
        try: 
            del os.environ[BaseClient.CREDENTIAL_ENV]
        except:
            pass
        BaseClient._MODEL_CLIENT = None

    def test_make_instance_with_previous_client(self):
        client = mock.Mock()
        base = BaseClient(client)
        self.assertEqual(client, base._client)
   
    def test_make_instance_with_environ(self):
        env_path = "test_path"
        os.environ[BaseClient.CREDENTIAL_ENV] = env_path
        client_mock = mock.Mock()
        module_mock = mock.Mock()
        class_mock = mock.Mock(**{"from_service_account_json.return_value": client_mock})
        module_mock.Client = class_mock
        BaseClient._MODEL_CLIENT = module_mock
        base = BaseClient()
        self.assertEqual(client_mock, base._client)
        class_mock.from_service_account_json.assert_called_with(env_path)

    def test_with_no_client_and_env(self):
        with self.assertRaises(Exception) as context:
            BaseClient()
