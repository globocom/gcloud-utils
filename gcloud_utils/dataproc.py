"""
Module to handle with Dataproc cluster
"""

import time
import logging
import datetime
import os
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

    def list_clusters(self):
        """List all clusters"""
        request = self.__client.projects().regions().clusters()

        result = request.list(projectId=self.__project, region=self.__region).execute()
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
                       image_version='1.2.54-deb8', disk_size_in_gb=10):
        if workers_names is None:
            workers_names = ["worker" + str(i) for i in range(1, workers+1)]

        "Create a cluster"
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

    def submit_job(self, cluster_name, gs_bucket, list_args,
                   main_pyspark_file=None,python_files=None,
                   jar_paths=None, main_class=None ,properties=None):
        """Submits the Spark job to the cluster, assuming jars at `jar_paths` list has
        already been uploaded to `gs_bucket`"""

        gs_root = "gs://{}/".format(gs_bucket)
        datetime_now = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        if jar_paths:
            jar_files = [os.path.join(gs_root, x) for x in jar_paths]
            main_class_formatted = main_class.replace('.', '_')
            job_id = "{}_{}".format(main_class_formatted, datetime_now)
            spark_job_details = {'sparkJob': {
                'args': list_args,
                'mainClass': main_class,
                'jarFileUris': jar_files
                }
            }

        elif main_pyspark_file:
            main_python_file = os.path.join(gs_root, main_pyspark_file)
            python_files = [os.path.join(gs_root, x) for x in python_files]
            job_id = "{}_{}".format(os.path.basename(main_pyspark_file),datetime_now)
            spark_job_details = {'pysparkJob': {
                'mainPythonFileUri': main_python_file,
                'args': list_args,
                "pythonFileUris": python_files
                }
            }
        else:
            raise KeyError("Must pass a pyspark_file or a jar_paths")


        job_details = {
            'projectId': self.__project,
            'job': {
                'placement': {
                    'clusterName': cluster_name
                },
                'reference': {
                    'jobId': job_id
                },
                # 'sparkJob': {
                #     'args': list_args,
                #     'mainClass': main_class,
                #     'jarFileUris': jar_files
                # }
            }
        }
        job_details['job'].update(spark_job_details)

        if properties is not None:
            job_details['job']['sparkJob']['properties'] = properties

        result = self.__client.projects().regions().jobs().submit(
            projectId=self.__project,
            region=self.__region,
            body=job_details).execute()

        self.__logger.info('Submitted job ID %s', job_id)

        self.__wait_job_finish(job_id)
        return result

    def __wait_job_finish(self, job_id, sleep_time=5):
        request = self.__client.projects().regions().jobs().get(
            projectId=self.__project,
            region=self.__region,
            jobId=job_id)

        result = request.execute()
        status = result['status']['state']
        self.__logger.info("JOB %s  -  STATUS:%s", job_id, status)

        while status != 'ERROR' and status != 'DONE':
            result = request.execute()
            status = result['status']['state']

            if status == 'ERROR':
                raise Exception(result['status']['details'])
            elif status == 'DONE':
                self.__logger.info("Job finished.")
                return result

            self.__logger.info("JOB %s  -  STATUS:%s", job_id, status)
            # if 'yarnApplications' in result:
            #     progress = result['yarnApplications'][0]['progress']
            #     self.__logger.info("Progress: %s%%", progress*100)
            time.sleep(sleep_time)
