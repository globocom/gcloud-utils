from google.cloud import bigquery
import argparse


PROJECTS = {
    'project_id_horizon': 'valiant-circuit-129220',
    'project_id_GA': 'poc-ga',
    'project_id_datalake': 'datalake-bases',
    'project_id_datalake_dev': 'dev-datalake-globo-com'
}

def ensure_value(namespace, dest, default):
    stored = getattr(namespace, dest, None)
    if stored is None:
        return value
    return stored


class store_dict(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        vals = dict(ensure_value(namespace, self.dest, {}))
        k, _, v = values.partition('=')
        vals[k] = v
        setattr(namespace, self.dest, vals)


def export_to_gcs(
        client, bucket_name, filename, dataset_id, table_id, project, export_format):
    if export_format == 'CSV':
        filename = filename + '_*.csv.gz'
    else:
        filename = filename + '_*.json.gz'
    destination_uri = 'gs://{}/{}'.format(bucket_name, filename)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.ExtractJobConfig()
    job_config.compression = 'GZIP'
    job_config.destination_format = export_format

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location='US',
        job_config=job_config)  # API request
    extract_job.result()  # Waits for job to complete.

    print('Exported {}:{}.{} to {}'.format(
        project, dataset_id, table_id, destination_uri))
    return True

def query_to_table(
        client, filename, query_args, dataset_id, table_id, project):


    with open(filename) as query_file:
        query = query_file.read().format(**query_args)

    dataset = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table
    job_config.write_disposition= 'WRITE_TRUNCATE'
    job = client.query(query, job_config)
    job.result()

    print('Exported query to {}:{}.{}'.format(
        project, dataset_id, table_id))
    return True
