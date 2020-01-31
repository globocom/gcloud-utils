"""Module to handle Google BigQuery Service"""

import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from gcloud_utils.base_client import BaseClient
from gcloud_utils.bigquery.query_builder import QueryBuilder


class Bigquery(BaseClient):
    """Google-Bigquery handler"""

    FILE_FORMATS = {
        "csv": "CSV",
        "json": "NEWLINE_DELIMITED_JSON",
        "avro": "AVRO",
        "parquet": "PARQUET",
        "orc": "ORC"
    }

    COMPRESSION_FORMATS = {
        None: "NONE",
        "gz": "GZIP",
        "snappy": "SNAPPY"
    }

    _MODEL_CLIENT = bigquery

    def __init__(self, client=None, log_level=logging.ERROR):
        super(Bigquery, self).__init__(client, log_level)
        self._query = None

    def query(self, query_or_object, **kwargs):
        """Execute a query"""
        if isinstance(query_or_object, QueryBuilder):
            kwargs["query"] = query_or_object.query
        else:
            kwargs["query"] = query_or_object

        self._query = kwargs["query"]
        return self._client.query(**kwargs).result()

    def query_to_table(self, query_or_object, dataset_id,
                       table_id, write_disposition="WRITE_TRUNCATE",
                       job_config=None, **kwargs):
        """Execute a query in a especific table"""
        job_config = job_config if job_config else bigquery.QueryJobConfig()
        table = self._client.dataset(dataset_id).table(table_id)

        job_config.destination = table
        job_config.write_disposition = write_disposition

        return self.query(query_or_object, job_config=job_config, **kwargs)

    def _complete_filename(self, filename, export_format, compression_format):
        if (self.COMPRESSION_FORMATS.get(compression_format) and
                self.FILE_FORMATS.get(export_format)):
            complete_filename = "{}_*.{}".format(filename, export_format)
            if compression_format:
                complete_filename = "{}.{}".format(
                    complete_filename, compression_format)
            return complete_filename

        raise Exception(
            "Only valid file formats: {}. Only valid compression formats: {}"
            .format(
                ",".join(self.FILE_FORMATS.keys()),
                ",".join(self.COMPRESSION_FORMATS.keys())
            )
        )

    def table_to_cloud_storage(self, dataset_id, table_id,
                               bucket_name, filename, job_config=None,
                               export_format="csv", compression_format="gz", location="US",
                               **kwargs):
        """Extract a table from BigQuery and send to GoogleStorage"""
        complete_filename = self._complete_filename(
            filename, export_format, compression_format)

        destination_uri = "gs://{}/{}".format(bucket_name, complete_filename)
        table = self._client.dataset(dataset_id).table(table_id)

        job_config = job_config if job_config else bigquery.ExtractJobConfig()

        job_config.compression = self.COMPRESSION_FORMATS.get(
            compression_format)
        job_config.destination_format = self.FILE_FORMATS.get(export_format)

        return self._client.extract_table(
            table,
            destination_uri,
            location=location,
            job_config=job_config, **kwargs).result()

    def create_dataset(self, dataset_id):
        """Create a dataset"""
        dataset = bigquery.Dataset(self._client.dataset(dataset_id))
        return self._client.create_dataset(dataset, True)

    def create_table(self, dataset_id, table_id):
        """Create a table based on dataset"""
        self.create_dataset(dataset_id)

        dataset_ref = self._client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref)
        return self._client.create_table(table, True)

    def cloud_storage_to_table(self, bucket_name, filename,
                               dataset_id, table_id, job_config=None,
                               import_format="csv", location="US", **kwargs):
        """Extract table from GoogleStorage and send to BigQuery"""
        self.create_table(dataset_id, table_id)

        dataset_ref = self._client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        job_config = job_config if job_config else bigquery.LoadJobConfig()
        job_config.source_format = self.FILE_FORMATS.get(import_format)

        return self._client.load_table_from_uri(
            "gs://{}/{}".format(bucket_name, filename),
            table_ref,
            job_config=job_config,
            location=location,
            **kwargs
        ).result()

    def table_exists(self, table_id, dataset_id, project_id=None):
        """Check if tables exists"""
        client = bigquery.Client(project_id) if project_id else self._client
        dataset = client.dataset(dataset_id)
        table = dataset.table(table_id)
        try:
            self._client.get_table(table)
            return True
        except NotFound:
            return False
