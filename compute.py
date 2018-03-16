"""Start gcloud instance"""
import time
import os
import logging
from googleapiclient import discovery


# os.environ["HTTPS_PROXY"] = "proxy.globoi.com:3128"


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

ZONE = "us-west1-b"
REGION = "us-west1"
PROJECT = "recomendacao-gcom"

class Compute(object):
    """Google-compute-engine handler"""

    def __init__(self, project=PROJECT, zone=ZONE):
        self.project=project
        self.zone=zone
        self.logger = logging.getLogger(name=self.__class__.__name__)
        self.client = discovery.build('compute', 'v1')
        self.__update_instances(self.client)
        

    def __request_instances_info(self):
        instances =  self.client.instances().list(project=self.project, zone=self.zone).execute()
        return instances[u'items']

    def __update_instances(self, client):
        intances_itens = self.__request_instances_info()
        result = {}
        for i in intances_itens:
            result.update({i[u'name'] : i[u'status']})
        self.intances = result

    def __check_status(self, instance_name, expected_status, max_time=400):
        sleep_time = 10
        max_check = max_time/sleep_time
        i = 0
        while (not self.intances[instance_name] == expected_status) and (i <= max_check):
            self.logger.info("waiting to %s ...", expected_status)
            self.__update_instances(self.client)
            time.sleep(sleep_time)
            i += 1

        return True

    def __change_status(self, instance_name, action, expecte_status):
        try:
            status = self.intances[instance_name]
            self.logger.debug("%s status: %s", instance_name, status)

            if not status == expecte_status:
                action(zone=ZONE, project=PROJECT, instance=instance_name).execute()
                self.__check_status(instance_name, expecte_status)

        except KeyError:
            raise Exception("Instence %s doesn't exists"%instance_name)

    def start_instance(self, instance_name):
        """Start VM by name"""
        self.__change_status(instance_name, self.client.instances().start, "RUNNING")

    def stop_instance(self, instance_name):
        """Stop VM by name"""
        self.__change_status(instance_name, self.client.instances().stop, "TERMINATED")

# c = Compute()
# c.start_instance("instance-7")
# time.sleep(10)
# c.stop_instance("instance-7")