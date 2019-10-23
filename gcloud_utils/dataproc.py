"""
Module to handle with Dataproc cluster
"""

import time
import logging
import datetime
import os
import re
from googleapiclient import discovery

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')


class Dataproc(object):
    """Module to handle with Dataproc cluster"""

    def __init__(self, project, region, http=None):
        self.__project = project
        self.__region = region
        self.__logger = logging.getLogger(name=self.__class__.__name__)
        self.__client = discovery.build('dataproc', 'v1', http=http)

        self.__pattern = re.compile(r'[\W_]+')

    def __format_job_id(self, job_id):
        return self.__pattern.sub('_', job_id)

    def list_clusters(self):
        """List all clusters"""
        request = self.__client.projects().regions().clusters()

        result = request.list(projectId=self.__project,
                              region=self.__region).execute()
        cluster_list = []

        if 'clusters' in result:
            cluster_list.append(result['clusters'])

        while 'nextPageToken' in result:
            token = result['nextPageToken']
            result = request.list(
                projectId=self.__project,
                region=self.__region,
                pageToken=token).execute()

            if 'clusters' in result:
                cluster_list.append(result['clusters'])

        return cluster_list

    def __wait_midle_state(self, cluster_name, final_state, sleep_time=5):
        midle_state = True
        while midle_state:
            result = self.__client.projects()\
                .regions()\
                .clusters()\
                .get(clusterName=cluster_name, projectId=self.__project, region=self.__region)\
                .execute()
            midle_state = not result['status']['state'] == final_state
            self.__logger.info(
                "Cluster_Name:%s  Status:%s",
                cluster_name,
                result['status']['state'])
            time.sleep(sleep_time)
        return not midle_state

    def create_cluster(self, name, workers, workers_names=None,
                       image_version='1.2.54-deb8', disk_size_in_gb=10,
                       metadata=None,
                       initialization_actions=None):
        """Create a cluster"""
        if workers_names is None:
            workers_names = ["worker" + str(i) for i in range(1, workers+1)]

        # Create a cluster
        data_to_create = {
            "projectId": self.__project,
            "clusterName": name,
            "config": {
                "configBucket": "",
                "gceClusterConfig": {
                    "subnetworkUri": "default",
                    "zoneUri": "{}-b".format(self.__region)
                },
                "masterConfig": {
                    "numInstances": 1,
                    "instanceNames": [
                        "cluster-yarn-recsys-m"
                    ],
                    "machineTypeUri": "n1-standard-4",
                    "diskConfig": {
                        "bootDiskSizeGb": disk_size_in_gb,
                        "numLocalSsds": 0
                    }
                },
                "workerConfig": {
                    "numInstances": workers,
                    "instanceNames": workers_names,
                    "machineTypeUri": "n1-standard-4",
                    "diskConfig": {
                        "bootDiskSizeGb": disk_size_in_gb,
                        "numLocalSsds": 0
                    }
                },
                "softwareConfig": {
                    "imageVersion": image_version
                },
            }
        }

        if metadata:
            data_to_create['config']['gceClusterConfig'].update(
                {"metadata": metadata})

        if initialization_actions:
            data_to_create['config'].update(
                {"initializationActions": initialization_actions})

        result = self.__client.projects()\
            .regions()\
            .clusters()\
            .create(body=data_to_create, projectId=self.__project, region=self.__region)\
            .execute()

        self.__wait_midle_state(name, "RUNNING")

        return result

    def delete_cluster(self, name):
        """Delete cluster by name"""
        result = self.__client.projects()\
            .regions()\
            .clusters()\
            .delete(clusterName=name, projectId=self.__project, region=self.__region)\
            .execute()

        self.__wait_midle_state(name, "DELETING")

        return result

    def __submit_job(self, job_id, cluster_name, submit_dict):
        job_details = {
            "projectId": self.__project,
            "job": {
                "placement": {
                    "clusterName": cluster_name
                },
                "reference": {
                    "jobId": job_id
                }
            }
        }
        job_details["job"].update(submit_dict)
        result = self.__client.projects().regions().jobs().submit(
            projectId=self.__project,
            region=self.__region,
            body=job_details).execute()
        self.__logger.info("Submitted job ID %s", job_id)

        self.__wait_job_finish(job_id)
        return result

    def submit_pyspark_job(self, cluster_name, gs_bucket, list_args,
                           main_pyspark_file, python_files, archive_uris=None, properties=None):
        """Submit the pyspark job to cluster, assuming py files at `python_files` list has
        already been uploaded to `gs_bucket """

        gs_root = "gs://{}/".format(gs_bucket)
        datetime_now = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        main_python_file = os.path.join(gs_root, main_pyspark_file)
        job_id = "pyspark_{}_{}".format(
            os.path.basename(main_pyspark_file), datetime_now)
        job_id_formated = self.__format_job_id(job_id)
        gs_python_files = [os.path.join(gs_root, python_file)
                           for python_file in python_files]

        submit_dict = {
            "pysparkJob": {
                "mainPythonFileUri": main_python_file,
                "args": list_args,
                "pythonFileUris": gs_python_files
            }
        }
        if archive_uris:
            submit_dict["pysparkJob"].update({"archiveUris": archive_uris})

        if properties:
            submit_dict["pysparkJob"].update({"properties": properties})

        return self.__submit_job(job_id_formated, cluster_name, submit_dict)

    def submit_spark_job(self, cluster_name, gs_bucket, list_args,
                         jar_paths, main_class, properties=None):
        """Submits the Spark job to the cluster, assuming jars at `jar_paths` list has
        already been uploaded to `gs_bucket`"""
        gs_root = "gs://{}/".format(gs_bucket)
        datetime_now = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        jar_files = [os.path.join(gs_root, x) for x in jar_paths]
        main_class_formatted = self.__format_job_id(main_class)

        job_id = "spark_{}_{}".format(main_class_formatted, datetime_now)
        submit_dict = {
            "sparkJob": {
                "args": list_args,
                "mainClass": main_class,
                "jarFileUris": jar_files
            }
        }

        if properties:
            submit_dict["sparkJob"]["properties"] = properties
        return self.__submit_job(job_id, cluster_name, submit_dict)

    def __wait_job_finish(self, job_id, sleep_time=5):
        request = self.__client.projects().regions().jobs().get(
            projectId=self.__project,
            region=self.__region,
            jobId=job_id)

        result = request.execute()
        status = result['status']['state']
        self.__logger.info("JOB %s  -  STATUS:%s", job_id, status)

        while status not in  ('ERROR', 'DONE'):
            result = request.execute()
            status = result['status']['state']

            if status == 'ERROR':
                raise Exception(result['status']['details'])
            if status == 'DONE':
                self.__logger.info("Job finished.")
                return result

            self.__logger.info("JOB %s  -  STATUS:%s", job_id, status)
            # if 'yarnApplications' in result:
            #     progress = result['yarnApplications'][0]['progress']
            #     self.__logger.info("Progress: %s%%", progress*100)
            time.sleep(sleep_time)
