"""
Module to handle with Dataproc cluster
"""

import time
import os
import logging
from googleapiclient import discovery
import httplib2

REGION = "us-east1"
PROJECT = "recomendacao-gcom"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')


class Dataproc(object):
    "Module to handle with Dataproc cluster"
    def __init__(self, project=PROJECT, region=REGION, http=None):
        self.__project = project
        self.__region = region
        self.__logger = logging.getLogger(name=self.__class__.__name__)
        self.__client = discovery.build('dataproc', 'v1', http=http)

    def list_clusters(self):
        "List all clusters"
        result = self.__client.projects()\
            .regions()\
            .clusters()\
                .list(projectId=self.__project, region=self.__region).execute()
        return result

    def __wait_midle_state(self, cluster_name, sleep_time=5):
        midle_state = True
        while midle_state:
            result = self.__client.projects()\
                .regions()\
                .clusters()\
                .get(clusterName=cluster_name, projectId=self.__project, region=self.__region)\
                .execute()
            midle_state = not result['status']['state'] == u'RUNNING'
            self.__logger.info(
                "Cluster Name : %s  Status : %s",
                cluster_name,
                result['status']['state'])
            time.sleep(sleep_time)
        return not midle_state

    def create_cluster(self, name, workers, workers_names):
        "Create a cluster"
        data_to_create = {
            "projectId":self.__project,
            "clusterName":name,
            "config": {
                "configBucket": "",
                "gceClusterConfig": {
                    "subnetworkUri": "default",
                    "zoneUri": "us-east1-b"
                    },
                "masterConfig": {
                    "numInstances": 1,
                    "instanceNames": [
                        "cluster-yarn-recsys-m"
                    ],
                    "machineTypeUri": "n1-standard-4",
                    "diskConfig": {
                        "bootDiskSizeGb": 10,
                        "numLocalSsds": 0

                    }
                },
                "workerConfig": {
                    "numInstances": workers,
                    "instanceNames": workers_names,
                    "machineTypeUri": "n1-standard-4",
                    "diskConfig": {
                        "bootDiskSizeGb": 10,
                        "numLocalSsds": 0
                    }
                }
            }
        }
        result = self.__client.projects()\
            .regions()\
            .clusters()\
            .create(body=data_to_create, projectId=self.__project, region=self.__region)

        return result

    def delete_cluster(self, name):
        "Delete cluster by name"
        result = self.__client.projects()\
            .regions()\
            .clusters()\
            .delete(clusterName=name, projectId=self.__project, region=self.__region)

        return result