"""Module to handle Google Cloud Functions Service"""

import os
import logging
from googleapiclient import discovery

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')


class Functions(object):
    """Google-Cloud-Functions handler"""

    def __init__(self, project, zone):
        self.project = project
        self.zone = zone
        self.logger = logging.getLogger(name=self.__class__.__name__)
        self.client = discovery.build('cloudfunctions', 'v1')
        self.functions = self.__get_functions_resource(self.client)

    def __get_functions_resource(self, client):
        return client.projects().locations().functions()

    def __build_function(self, path):
        pass


    def __execute_request(self, request):
        return request.execute()

    def create_function(self, name, runtime, path=os.getcwd()):
        """Create a Cloud Function"""
        # location = "projects/{}/locations/{}".format(self.project, self.zone)
        # request = self.functions.create(location=location)
        # return self.__execute_request(request)
        return("runtime: " + runtime + " path: " + path)

    def call_function(self, name, data):
        """Call a Cloud Function"""
        function = "projects/{}/locations/{}/functions/{}".format(
            self.project, self.zone, name)

        body = {"data": data}
        request = self.functions.call(name=function, body=body)

        return self.__execute_request(request)
