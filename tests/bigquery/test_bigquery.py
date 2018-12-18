"""Test Bigquery Module"""
import unittest
import os
from gcloud_utils.bigquery.bigquery import Bigquery
from gcloud_utils.bigquery.query_builder import QueryBuilder
from google.cloud import bigquery
from mock.mock import MagicMock, patch, call
from more_itertools.more import side_effect
from google.api_core.exceptions import NotFound

try:
    import mock
except ImportError:
    import unittest.mock as mock

class TestBigquery(unittest.TestCase):
    "Test Bigquery module"

    def test_is_using_base_contract(self):
        self.assertEqual(bigquery, Bigquery._MODEL_CLIENT)

    def test_make_query(self):
        query = "select * from test"
        client = mock.Mock()
        bigquery = Bigquery(client)
        bigquery.query(query)
        client.query.assert_called_once_with(query=query)

    def test_make_query_with_object(self):
        query = QueryBuilder("select * from test")
        job_mock = mock.Mock()
        client_mock = mock.Mock(**{"query.return_value": job_mock})

        bigquery = Bigquery(client_mock)
        bigquery.query(query)
        client_mock.query.assert_called_once_with(query=query.query)
        job_mock.result.assert_called_once()

    def test_make_query_to_table(self):
        query = "select * from test"
        client_mock = mock.Mock()
        dataset_id = "test_dataset"
        table_id = "test_table"

        bigquery = Bigquery(client_mock)
        bigquery.query_to_table(query, dataset_id, table_id)
        client_mock.query.assert_called_once()

    def test_make_query_to_table_with_job_config(self):
        dataset_id = "test_dataset"
        table_id = "test_table"
        query = "select * from test"
        job_config_mock = mock.Mock()
        dataset_mock = mock.Mock(**{"table.return_value": table_id + "_name"})
        client_mock = mock.Mock(**{"dataset.return_value": dataset_mock})

        bigquery = Bigquery(client_mock)
        bigquery.query_to_table(query, dataset_id, table_id, job_config=job_config_mock)
        client_mock.query.assert_called_once_with(query=query, job_config=job_config_mock)
        self.assertEqual(table_id + "_name", job_config_mock.destination)
        self.assertEqual("WRITE_TRUNCATE", job_config_mock.write_disposition)

    def test_make_query_to_table_with_write_disposition(self):
        dataset_id = "test_dataset"
        table_id = "test_table"
        query = "select * from test"
        write_disposition = "test"
        job_config_mock = mock.Mock()
        client_mock = mock.Mock()

        bigquery = Bigquery(client_mock)
        bigquery.query_to_table(query, dataset_id, table_id, job_config=job_config_mock, write_disposition=write_disposition)
        client_mock.query.assert_called_once_with(query=query, job_config=job_config_mock)

        self.assertEqual(write_disposition, job_config_mock.write_disposition)

    def test_export_table_to_google_cloud(self):
        dataset_id = "test_dataset"
        table_id = "test_table"
        bucket_name = "test_bucket"
        bucket_filename = "test_filename"

        client_mock = mock.Mock()
        bigquery = Bigquery(client_mock)
        bigquery.table_to_cloud_storage(dataset_id, table_id, bucket_name, bucket_filename)

        client_mock.extract_table.assert_called_once()

    def test_export_table_to_google_cloud_with_wrong_file_type(self):
        dataset_id = "test_dataset"
        table_id = "test_table"
        bucket_name = "test_bucket"
        bucket_filename = "test_filename"

        client_mock = mock.Mock()
        bigquery = Bigquery(client_mock)

        with self.assertRaises(Exception) as context:
            bigquery.table_to_cloud_storage(dataset_id, table_id, bucket_name, bucket_filename, export_format="no_exists")

        client_mock.extract_table.assert_not_called()

    def test_export_table_to_google_cloud_with_wrong_compression_type(self):
        dataset_id = "test_dataset"
        table_id = "test_table"
        bucket_name = "test_bucket"
        bucket_filename = "test_filename"

        client_mock = mock.Mock()
        bigquery = Bigquery(client_mock)

        with self.assertRaises(Exception) as context:
            bigquery.table_to_cloud_storage(dataset_id, table_id, bucket_name, bucket_filename, compression_format="no_exists")

        client_mock.extract_table.assert_not_called()

    def test_export_table_to_google_cloud_with_wrong_compression_type_and_file_type(self):
        dataset_id = "test_dataset"
        table_id = "test_table"
        bucket_name = "test_bucket"
        bucket_filename = "test_filename"

        client_mock = mock.Mock()
        bigquery = Bigquery(client_mock)

        with self.assertRaises(Exception) as context:
            bigquery.table_to_cloud_storage(dataset_id, table_id, bucket_name, bucket_filename, compression_format="no_exists", export_format="no_exists")

        client_mock.extract_table.assert_not_called()

    def test_export_table_to_google_cloud_with_job_config(self):
        dataset_id = "test_dataset"
        table_id = "test_table"
        bucket_name = "test_bucket"
        bucket_filename = "test_filename"
        location = "test_US"
        expected_destination = "gs://test_bucket/test_filename_*.csv.gz"
        export_format = "csv"
        compression_format = "gz"

        job_config_mock = mock.Mock()
        dataset_mock = mock.Mock(**{"table.return_value": table_id + "_name"})
        client_mock = mock.Mock(**{"dataset.return_value": dataset_mock})

        bigquery = Bigquery(client_mock)
        bigquery.table_to_cloud_storage(
            dataset_id, table_id, bucket_name, bucket_filename,
            export_format=export_format, compression_format=compression_format,
            job_config=job_config_mock, location=location
        )

        client_mock.extract_table.assert_called_once_with(
            table_id + "_name",
            expected_destination,
            location=location,
            job_config=job_config_mock
        )
        self.assertEqual(bigquery.COMPRESSION_FORMATS[compression_format], job_config_mock.compression)
        self.assertEqual(bigquery.FILE_FORMATS[export_format], job_config_mock.destination_format)

    def test_export_table_to_google_cloud_with_job_config_and_extra_params(self):
        dataset_id = "test_dataset"
        table_id = "test_table"
        bucket_name = "test_bucket"
        bucket_filename = "test_filename"
        location = "test_US"
        expected_destination = "gs://test_bucket/test_filename_*.json"
        export_format = "json"
        compression_format = None
        xuxu = "test_xuxu"

        job_config_mock = mock.Mock()
        dataset_mock = mock.Mock(**{"table.return_value": table_id + "_name"})
        client_mock = mock.Mock(**{"dataset.return_value": dataset_mock})

        bigquery = Bigquery(client_mock)
        bigquery.table_to_cloud_storage(
            dataset_id, table_id, bucket_name, bucket_filename,
            export_format=export_format, compression_format=compression_format,
            job_config=job_config_mock, location=location, xuxuxu=xuxu
        )

        client_mock.extract_table.assert_called_once_with(
            table_id + "_name",
            expected_destination,
            location=location,
            job_config=job_config_mock,
            xuxuxu=xuxu
        )
        self.assertEqual(bigquery.COMPRESSION_FORMATS[compression_format], job_config_mock.compression)
        self.assertEqual(bigquery.FILE_FORMATS[export_format], job_config_mock.destination_format)
    
    def test_import_table_from_google_cloud(self):
        dataset_id = "test_dataset"
        table_id = "test_table"
        bucket_name = "test_bucket"
        bucket_filename = "test_filename"
        expected_source = "gs://test_bucket/test_filename"
        expected_table = "test_dataset.test_table"
        
        dataset_mock = mock.Mock(**{"table.return_value": mock.Mock(bigquery.Table)})
        client_mock = mock.Mock(**{"dataset.return_value": mock.Mock(bigquery.Dataset)})
        job_config_mock = mock.Mock()

        bq = Bigquery(client_mock)
        bq.cloud_storage_to_table(bucket_name, bucket_filename, dataset_id, table_id, job_config_mock)

        client_mock.load_table_from_uri.assert_called_once_with(
            expected_source,
            client_mock.dataset().table(),
            job_config=job_config_mock,
            location='US'
        )

    def test_table_exists_same_project(self):
        table = mock.Mock()

        dataset = mock.Mock()
        dataset.table = MagicMock(return_value=table)

        client = mock.Mock()
        client.get_table = MagicMock()
        client.dataset = MagicMock(return_value=dataset)

        bigquery = Bigquery(client)

        with patch("gcloud_utils.bigquery.bigquery.bigquery") as original_bigquery:
            original_bigquery.Client = MagicMock()

            assert bigquery.table_exists(table_id="my_table", dataset_id="my_dataset")
            assert original_bigquery.Client.call_args_list == []
            assert client.get_table.call_args_list == [call(table)]
            assert client.dataset.call_args_list == [call("my_dataset")]
            assert dataset.table.call_args_list == [call("my_table")]

    def test_table_exists_false_same_project(self):
        table = mock.Mock()

        dataset = mock.Mock()
        dataset.table = MagicMock(return_value=table)

        client = mock.Mock()
        client.get_table = MagicMock(side_effect=NotFound("xxx"))
        client.dataset = MagicMock(return_value=dataset)

        bigquery = Bigquery(client)

        with patch("gcloud_utils.bigquery.bigquery.bigquery") as original_bigquery:
            original_bigquery.Client = MagicMock()

            assert not bigquery.table_exists(table_id="my_table", dataset_id="my_dataset")
            assert original_bigquery.Client.call_args_list == []

    def test_table_exists_other_project(self):
        table = mock.Mock()

        dataset = mock.Mock()
        dataset.table = MagicMock(return_value=table)

        client = mock.Mock()
        client.get_table = MagicMock()
        client.dataset = MagicMock(return_value=dataset)

        other_client = mock.Mock()
        other_client.dataset = MagicMock(return_value=dataset)

        bigquery = Bigquery(client)

        with patch("gcloud_utils.bigquery.bigquery.bigquery") as original_bigquery:
            original_bigquery.Client = MagicMock(return_value=other_client)

            assert bigquery.table_exists(table_id="my_table", dataset_id="my_dataset", project_id="my_project")
            assert original_bigquery.Client.call_args_list == [call("my_project")]
            assert client.get_table.call_args_list == [call(table)]
            assert client.dataset.call_args_list == []
            assert other_client.dataset.call_args_list == [call("my_dataset")]
            assert dataset.table.call_args_list == [call("my_table")]

    def test_table_exists_false_other_project(self):
        table = mock.Mock()

        dataset = mock.Mock()
        dataset.table = MagicMock(return_value=table)

        client = mock.Mock()
        client.get_table = MagicMock(side_effect=NotFound("xxx"))
        client.dataset = MagicMock(return_value=dataset)

        other_client = mock.Mock()
        other_client.dataset = MagicMock(return_value=dataset)

        bigquery = Bigquery(client)

        with patch("gcloud_utils.bigquery.bigquery.bigquery") as original_bigquery:
            original_bigquery.Client = MagicMock(return_value=other_client)

            assert not bigquery.table_exists(table_id="my_table", dataset_id="my_dataset", project_id="my_project")
            assert original_bigquery.Client.call_args_list == [call("my_project")]
            assert other_client.dataset.call_args_list == [call("my_dataset")]
