from google.cloud import bigquery
from gcloud_utils.bigquery.query_builder import QueryBuilder

class Bigquery(object):
    """Google-Bigquery handler"""

    FILE_FORMATS = {
        "csv": "CSV",
        "json": "NEWLINE_DELIMITED_JSON",
        "avro": "AVRO"
    }

    COMPRESSION_FORMATS = {
        None: "NONE",
        "gzip": "GZIP",
        "snappy": "SNAPPY"
    }

    def __init__(self, client):
        self._client = client
        self._query = None

    def query(self, query_or_object, **kwargs):
        if isinstance(query_or_object, QueryBuilder):
            kwargs["query"] = query_or_object.query
        else:
            kwargs["query"] = query_or_object

        self._query = kwargs["query"]
        return self._client.query(**kwargs).result()
    
    def query_to_table(self, query_or_object, dataset_id, table_id, write_disposition="WRITE_TRUNCATE", job_config=None, **kwargs):
        job_config = job_config if job_config else bigquery.QueryJobConfig()
        table = self._client.dataset(dataset_id).table(table_id)

        job_config.destination = table
        job_config.write_disposition = write_disposition

        return self.query(query_or_object, job_config=job_config, **kwargs)
    
    def _complete_filename(self, filename, export_format, compression_format):
        if self.COMPRESSION_FORMATS.get(compression_format) and self.FILE_FORMATS.get(export_format):
            complete_filename = "{}_*.{}".format(filename, export_format)
            if compression_format:
                complete_filename = "{}.{}".format(complete_filename, compression_format)
            return complete_filename
        else:
            raise Exception("Only valid file formats: {}. Only valid compression formats: {}".format(
                ",".join(self.FILE_FORMATS.keys()), ",".join(self.COMPRESSION_FORMATS.keys())
            ))

    def table_to_cloud_storage(self, dataset_id, table_id, bucket_name, filename, job_config=None, export_format="csv", compression_format="gzip", location="US", **kwargs):

        complete_filename = self._complete_filename(filename, export_format, compression_format)

        destination_uri = "gs://{}/{}".format(bucket_name, complete_filename)
        table = self._client.dataset(dataset_id).table(table_id)

        job_config = job_config if job_config else bigquery.ExtractJobConfig()

        job_config.compression = self.COMPRESSION_FORMATS.get(compression_format)
        job_config.destination_format = self.FILE_FORMATS.get(export_format)

        return self._client.extract_table(
            table,
            destination_uri,
            location=location,
            job_config=job_config, **kwargs).result()
