# GCloud Utils

[Documentation](docs/build/rst/index.rst)

## BigQuery

You can use the CLI or the python module to run a query in [BigQuery](https://bigquery.cloud.google.com).

### Using CLI

#### Saving Query result in a BigQuery table

```
query_to_table dataset table json_key YYYYMMDD query_file -Aquery_arg1=arg -Aquery_arg2=arg
```

parameters:
- YYYMMMDD: date of the script
- -A: parameter to pass args to the query or the query's file
- json_key: credentials to bigquery service

Use -h to see other parameters options

The CLI default is use the current time. 

The CLI allows put some fixed variables in queries:

- **previous_date**: previous date of declared current date (YYYYMMDD)
- **start_date**: declared current date (YYYYMMDD)
- **next_date**: next date of declared current date (YYYYMMDD)


#### Saving BigQuery's Tabel in Cloud Storage

```
table_to_gcs dataset table bucket cloudstorage_filename json_key YYYYMMDD time_delta export_format compression_format
```

onde:
- YYYMMMDD: date of the script
- time_delta amount of days before current date to get the table
- json_key  credentials to bigquery service

Use -h to see other parameters options

#### Cloud Storage importing table in BigQuery

```
gcs_to_table bucket cloudstorage_filename dataset table json_key YYYYMMDD
```

onde:
- YYYMMMDD: date of the script
- json_key  credentials to bigquery service

Use -h to see other parameters options


### Using the module

#### Simple query

```
from google.cloud import bigquery
from gcloud_utils.bigquery.bigquery import Bigquery

query = "select * from bq_table"

client = bigquery.Client.from_service_account_json(args.gcs_key_json)
bq_client = Bigquery(client)
result = bq_client.query(self, query, **kwargs)
```

#### Query with parameters


```
from google.cloud import bigquery
from gcloud_utils.bigquery.bigquery import Bigquery
from gcloud_utils.bigquery.query_builder import QueryBuilder

query = QueryBuilder("select * from ${my_table}")

query.with_vars(my_table="bq_table")

client = bigquery.Client.from_service_account_json(args.gcs_key_json)
bq_client = Bigquery(client)
result = bq_client.query(self, query, **kwargs)
```

#### Saving Query in BigQuery


```
from google.cloud import bigquery
client = bigquery.Client.from_service_account_json(args.gcs_key_json)
bq_client = Bigquery(client)
bq_client.query_to_table(query_or_object, dataset_id, table_id, write_disposition="WRITE_TRUNCATE", job_config=None, **kwargs)
```

#### Saving BigQuery's table in Cloud Storage


```
from google.cloud import bigquery
client = bigquery.Client.from_service_account_json(args.gcs_key_json)
bq_client = Bigquery(client)
bq_client.table_to_cloud_storage(dataset_id, table_id, bucket_name, filename, job_config=None, export_format="csv", compression_format="gz", location="US", **kwargs)
```

#### Salving Cloud Storage in BigQuery's table


```
from google.cloud import bigquery
client = bigquery.Client.from_service_account_json(args.gcs_key_json)
bq_client = Bigquery(client)
bq_client.cloud_storage_to_table(bucket_name, filename, dataset_id, table_id, job_config=None, location="US", **kwargs)
```

### Publishing the module in Artifactory
First, write the file `~/.pypirc` like:
```
[distutils]
index-servers =
    pypi-local
    ipypi-local

[pypi-local]
repository: https://artifactory.globoi.com/artifactory/api/pypi/pypi-local
username: seu_username_do_artifactory_aqui

[ipypi-local]
repository: https://artifactory.globoi.com/artifactory/api/pypi/ipypi-local
username: seu_username_do_artifactory_aqui
```

Then, decide the version that will be published. 
Let's say that we will publish the version **3.2.1**.

In this case, run first `make install`, then  `VERSION=3.2.1 make release` and put your password when is asked.
