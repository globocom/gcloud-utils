.. gcloud-utils documentation master file, created by
   sphinx-quickstart on Tue Dec 18 16:53:52 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to gcloud-utils's documentation!
========================================

.. toctree::
   :maxdepth: 3
   :caption: Contents:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Quick Start
===========

To install the package you must run 

.. code-block:: console
   :linenos:

   pip install gcloud_utils

User Guide
==========
There are two ways to use some functions with GCloud-utils: as CLI or as a python package. 

CLI
===
    The are some functions that can be used as a CLI. 

- **Saving query result in a BigQuery table**

.. code-block:: console

    query_to_table dataset table json_key YYYYMMDD query_file -Aquery_arg1=arg -Aquery_arg2=arg"

The command parameters are:
    - ``YYYMMMDD``: date of the script (current time is the default value).
    - ``-A``: parameter to pass args to the query or the query's file.
    - ``json_key``: credentials to bigquery service.

The CLI allows put some fixed variables in queries:

    - ``previous_date``: previous date of declared current date (YYYYMMDD)
    - ``start_date``: declared current date (YYYYMMDD)
    - ``next_date``: next date of declared current date (YYYYMMDD)


- **Importing table from BigQuery to Cloud Storage**

.. code-block:: console

    table_to_gcs dataset table bucket cloudstorage_filename json_key YYYYMMDD time_delta export_format compression_format

Where the parameters are:
    - ``dataset``: Name of the dataset save
    - ``bucket``: Name of the bucket save
    - ``cloudstorage_filename``: Path from Google Storage to save
    - ``json_key``: Path to the Google Credentials Application file
    - ``YYYMMMDD``: Date of the script
    - ``time_delta``: Amount of days before current date to get the table

- **Save table from BigQuery in GoogleStorage**

.. code-block:: console

    gcs_to_table bucket cloudstorage_filename dataset table json_key YYYYMMDD

Where the parameters are:
    - ``YYYMMMDD``: Date of the script
    - ``json_key``:  Credentials to bigquery service

Python package
==============

BigQuery
--------

**Simple query**

.. code-block:: python

   from google.cloud import bigquery
   from gcloud_utils.bigquery.bigquery import Bigquery

   query = "SELECT * FROM bq_table"

   client = bigquery.Client.from_service_account_json(args.gcs_key_json)
   bq_client = Bigquery(client)

   result = bq_client.query(self, query, **kwargs)

**Query with parameters**

.. code-block:: python

   from google.cloud import bigquery
   from gcloud_utils.bigquery.bigquery import Bigquery
   from gcloud_utils.bigquery.query_builder import QueryBuilder

   query = QueryBuilder("SELECT * FROM ${my_table}")
   query.with_vars(my_table="bq_table")

   client = bigquery.Client.from_service_account_json(args.gcs_key_json)
   bq_client = Bigquery(client)

   result = bq_client.query(self, query)

**Saving Query in BigQuery**

.. code-block:: python

   from google.cloud import bigquery
   
   client = bigquery.Client.from_service_account_json(args.gcs_key_json)
   bq_client = Bigquery(client)
   
   bq_client.query_to_table(
       query_or_object,
       dataset_id,
       table_id,
       write_disposition="WRITE_TRUNCATE",
       job_config=None,
   )

**Saving BigQuery's table in Cloud Storage**

.. code-block:: python

   from google.cloud import bigquery

   client = bigquery.Client.from_service_account_json(args.gcs_key_json)
   bq_client = Bigquery(client)

   bq_client.table_to_cloud_storage(
       dataset_id,
       table_id,
       bucket_name,
       filename,
       job_config=None,
       export_format="csv",
       compression_format="gz",
       location="US",
   )

**Salving Cloud Storage in BigQuery's table**

.. code-block:: python

   from google.cloud import bigquery

   client = bigquery.Client.from_service_account_json(args.gcs_key_json)
   bq_client = Bigquery(client)
   
   bq_client.cloud_storage_to_table(
       bucket_name,
       filename,
       dataset_id,
       table_id,
       job_config=None,
       location="US",
   )

Cloud Function
--------------

**Create a function**

.. code-block:: python

   from gcloud_utils.functions import Functions

   functions_handler = Functions('my-project', 'us-central1-a')

   function_name = 'my-function-name'
   function_path = '/path/to/function.py'
   function_runtime = 'python37'

   functions_handler.create_function(
       function_name,
       function_runtime,
       function_path,
   )

**List all functions**

.. code-block:: python

   from gcloud_utils.functions import Functions

   functions_handler = Functions('my-project', 'us-central1-a')

   for function in functions_handler.list_functions():
       print(function.name)

**Describe a specific function**

.. code-block:: python

   from gcloud_utils.functions import Functions

   functions_handler = Functions('my-project', 'us-central1-a')
   function_detail = functions_handler.describe_function('my-function-name')

   print('Status: {}'.format(function_detail.status))
   print('Last update: {}'.format(function_detail.updateTime))

**Trigger a function**

.. code-block:: python

   import json

   from gcloud_utils.functions import Functions

   functions_handler = Functions('my-project', 'us-central1-a')

   data = json.dumps({'example': 'example'})
   functions_handler.call_function('my-function-name', data)


API Reference
=============

MLEngine
--------

.. automodule:: gcloud_utils.ml_engine
   :members:

Compute
-------

.. automodule:: gcloud_utils.compute
   :members:

Cloud Function
--------------

.. automodule:: gcloud_utils.functions
   :members:

BigQuery
--------

.. automodule:: gcloud_utils.bigquery.bigquery
   :members:

Dataproc
--------

.. automodule:: gcloud_utils.dataproc
   :members:

Storage
-------

.. automodule:: gcloud_utils.storage
   :members:
