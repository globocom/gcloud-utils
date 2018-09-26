import os
import time
import argparse
from google.cloud import bigquery
from datetime import date, timedelta
from utils import export_to_gcs, PROJECTS

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        'project_id',
        help='BigQuery project id key.Supported: project_id_horizon, project_id_GA, project_id_datalake, project_id_datalake_dev'
    )

    parser.add_argument(
        'bigquery_dataset',
        help='Name of BigQuery dataset.'
    )

    parser.add_argument(
        'bigquery_tablename',
        help='Name of BigQuery table.'
    )

    parser.add_argument(
        'cloudstorage_bucket',
        help='Name of CloudStorage bucket.'
    )

    parser.add_argument(
        'cloudstorage_filename',
        help='Name file to store in CloudStorage.'
    )

    parser.add_argument(
        'gcs_key_json',
        help='Name of Json key.'
    )

    parser.add_argument(
        'time_delta',
        help='How many days ago you want to run.'
    )

    parser.add_argument(
        'export_format',
        help='Export Format.',
        default="CSV"
    )
    args = parser.parse_args()

    os.environ['TZ'] = 'America/Sao_Paulo'  # -- Use time in local timezone!!
    time.tzset()  # -- must run this to import the TZ

    project_id = PROJECTS[args.project_id]

    bq_client = bigquery.Client.from_service_account_json('./{}'.format(args.gcs_key_json))
    bq_dataset = args.bigquery_dataset

    cs_bucket = args.cloudstorage_bucket
    cs_filename = args.cloudstorage_filename

    dt = date.today() - timedelta(int(args.time_delta))

    bq_table_name = args.bigquery_tablename % dict(date=dt.strftime('%Y%m%d'))

    print("Start Process")

    export_to_gcs(
        bq_client, cs_bucket,
        cs_filename, bq_dataset,
        bq_table_name, project_id,
        args.export_format
    )

    print("Finish save in GCS")

main()
