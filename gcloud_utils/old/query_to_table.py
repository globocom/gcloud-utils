import os
import time
import argparse
from google.cloud import bigquery
from datetime import date, timedelta
from utils import query_to_table, PROJECTS, store_dict

def main():
    parser = argparse.ArgumentParser(description='Process table download.')
    parser.add_argument(
        'project_id',
        help='BigQuery project id key.Supported: project_id_horizon, project_id_GA, project_id_datalake, project_id_datalake_dev'
    )

    parser.add_argument(
        'bigquery_dataset',
        help='Name of BigQuery dataset.'
    )

    parser.add_argument(
        'bigquery_temp_tablename',
        help='Name of BigQuery temp table.'
    )

    parser.add_argument(
        'gcs_key_json',
        help='Name of Json key.'
    )

    parser.add_argument(
        'query_file',
        help='File with sql query to run'
    )

    parser.add_argument(
        '-D', default={}, action=store_dict
    )

    args = parser.parse_args()

    os.environ['TZ'] = 'America/Sao_Paulo'  # -- Use time in local timezone!!
    time.tzset()  # -- must run this to import the TZ

    project_id = PROJECTS[args.project_id]

    bq_client = bigquery.Client.from_service_account_json('./{}'.format(args.gcs_key_json))
    bq_dataset = args.bigquery_dataset

    filename = args.query_file

    query_args = args.D


    if query_args.get("days_ago"):
        dt = date.today() - timedelta(int(query_args.get("days_ago")))
        query_args["last_date"] = dt.strftime('%Y%m%d')

    bq_table_name = args.bigquery_temp_tablename

    print("Start Process")

    query_to_table(
        bq_client, './{}'.format(filename),
        query_args, bq_dataset,
        bq_table_name, project_id
    )

    print("Finish save in GCS")

main()
