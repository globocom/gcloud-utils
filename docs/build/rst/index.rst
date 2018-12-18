
Welcome to gcloud-utils’s documentation!
****************************************


Indices and tables
******************

* `Index <genindex.rst>`_

* `Module Index <py-modindex.rst>`_

* `Search Page <search.rst>`_


MLEngine
********

Submit Job to ML Engine

**class gcloud_utils.ml_engine.MlEngine(project, bucket_name, region,
package_path='packages', job_dir='jobs', http=None)**

   Google-ml-engine handler

   **create_model_version(model_name, version, job_id)**

      Increase Model version

   **create_new_model(name, description='Ml model')**

      Create new model

   **delete_model_version(model_name, version)**

      Delete Model version

   **delete_older_model_versions(model_name, n_versions_to_keep)**

      Keep the most recents model versions and delete older ones. The
      number of models to keep is specified by the parameter
      n_versions_to_keep

   **get_job(job_id)**

      Describes a job

   **get_model_versions(model_name)**

      Return all versions

   **increase_model_version(model_name, job_id)**

      Increase Model version

   **list_jobs(filter_final_state='SUCCEEDED')**

      List all models in project

   **list_models()**

      List all models in project

   **set_version_as_default(model, version)**

      Set a model version as default

   **start_predict_job(job_id_prefix, model, input_path,
   output_path)**

      start a prediction job

   **start_training_job(job_id_prefix, package_name, module,
   extra_packages=[], runtime_version='1.0', python_version='',
   **args)**

      Start a training job

   **wait_job_to_finish(job_id, sleep_time=60)**

      Waits job to finish


Compute
*******

Module to handle Google Compute Service

**class gcloud_utils.compute.Compute(project, zone)**

   Google-compute-engine handler

   **start_instance(instance_name)**

      Start VM by name

   **stop_instance(instance_name)**

      Stop VM by name


BigQuery
********

Module to handle Google BigQuery Service

**class gcloud_utils.bigquery.bigquery.Bigquery(client=None,
log_level=40)**

   Google-Bigquery handler

   **cloud_storage_to_table(bucket_name, filename, dataset_id,
   table_id, job_config=None, import_format='csv', location='US',
   **kwargs)**

      Extract table from GoogleStorage and send to BigQuery

   **create_dataset(dataset_id)**

      Create a dataset

   **create_table(dataset_id, table_id)**

      Create a table based on dataset

   **query(query_or_object, **kwargs)**

      Execute a query

   **query_to_table(query_or_object, dataset_id, table_id,
   write_disposition='WRITE_TRUNCATE', job_config=None, **kwargs)**

      Execute a query in a especific table

   **table_exists(table_id, dataset_id, project_id=None)**

      Check if tables exists

   **table_to_cloud_storage(dataset_id, table_id, bucket_name,
   filename, job_config=None, export_format='csv',
   compression_format='gz', location='US', **kwargs)**

      Extract a table from BigQuery and send to GoogleStorage


Dataproc
********

Module to handle with Dataproc cluster

**class gcloud_utils.dataproc.Dataproc(project, region, http=None)**

   Module to handle with Dataproc cluster

   **delete_cluster(name)**

      Delete cluster by name

   **list_clusters()**

      List all clusters

   **submit_job(cluster_name, gs_bucket, jar_paths, main_class,
   list_args)**

      Submits the Spark job to the cluster, assuming jars at
      *jar_paths* list has already been uploaded to *gs_bucket*


Storage
*******

Module to download and use files from Google Storage

**class gcloud_utils.storage.Storage(bucket, client=None,
log_level=40)**

   Google-Storage handler

   **delete_file(storage_path)**

      Deletes a blob from the bucket.

   **delete_path(storage_path)**

      Deletes all the blobs with storage_path prefix

   **download_file(storage_path, local_path)**

      Download Storage file to local path, creating a path at
      local_path if nedded

   **download_files(path, local_path, filter_suffix=None)**

      Download all files in path

   **get_abs_path(storage_path)**

      get abs path from GStorage

   **get_file(file_path, local_path)**

      Get all files from Storage path

   **get_files_in_path(path, local_path)**

      Download all files from path in Google Storage and return a list
      with those files

   **list_files(path, filter_suffix=None)**

      List all blobs in path

   **ls(path)**

      List files directly under specified path

   **path_exists_storage(path)**

      Check if path exists on Storage

   **rename_files(storage_prefix, new_path)**

      Renames all the blobs with storage_prefix prefix

   **upload_file(storage_path, local_path)**

      Upload one local file to Storage

   **upload_path(storage_path_base, local_path_base)**

      Upload all filer from local path to Storage

   **upload_value(storage_path, value)**

      Upload a value to  Storage
