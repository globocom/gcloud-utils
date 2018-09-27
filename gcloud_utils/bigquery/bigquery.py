from gcloud import bigquery
from gcloud_utils.bigquery.query_builder import QueryBuilder

class Bigquery(object):
    """Google-Bigquery handler"""

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
    
    def query_to_table(self, query_or_object, dataset_id, table_id, write_disposition='WRITE_TRUNCATE', job_config=None, **kwargs):
        job_config = job_config if job_config else bigquery.QueryJobConfig()
        table = self._client.dataset(dataset_id).table(table_id)

        job_config.destination = table
        job_config.write_disposition = write_disposition

        return self.query(query, job_config, **kwargs)

