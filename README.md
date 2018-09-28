# GCloud Utils

## BigQuery

Para realizar query no BigQuery pode usar o cli ou através do módulo.

### Utilizando CLI

#### Query salvando em Tabela do BigQuery

```
    query_to_table dataset table json_key YYYYMMDD query_file -Aquery_arg1=arg -Aquery_arg2=arg
```

onde:
- YYYMMMDD é a data que está rodando o script
- -A serve para passar argumentos para a query ou arquivo de query
- json_key é a chave de acesso ao bigquery

Para mais parâmetros use o -h

#### Tabela do BigQuery salvando no Cloud Storage

```
    table_to_gcs dataset table bucket cloudstorage_filename json_key YYYYMMDD time_delta export_format compression_format
```

onde:
- YYYMMMDD é a data que está rodando o script
- time_delta é o número de dias para trás que a tabela será pega
- json_key é a chave de acesso ao bigquery

Para mais parâmetros use o -h


### Utilizando Módulo

#### Query normal

```
    from google.cloud import bigquery
    from gcloud_utils.bigquery.bigquery import Bigquery

    query = "select * from bq_table"

    client = bigquery.Client.from_service_account_json(args.gcs_key_json)
    bq_client = Bigquery(client)
    result = bq_client.query(self, query, **kwargs)
```

#### Query com parâmetros


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

#### Query salvando em Tabela do BigQuery


```
    from google.cloud import bigquery
    client = bigquery.Client.from_service_account_json(args.gcs_key_json)
    bq_client = Bigquery(client)
    bq_client.query_to_table(query_or_object, dataset_id, table_id, write_disposition="WRITE_TRUNCATE", job_config=None, **kwargs)
```

#### Tabela do BigQuery salvando no Cloud Storage


```
    from google.cloud import bigquery
    client = bigquery.Client.from_service_account_json(args.gcs_key_json)
    bq_client = Bigquery(client)
    bq_client.table_to_cloud_storage(dataset_id, table_id, bucket_name, filename, job_config=None, export_format="csv", compression_format="gzip", location="US", **kwargs)
```