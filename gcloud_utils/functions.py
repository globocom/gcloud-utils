"""Module to handle Google Cloud Functions Service"""

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
    

    def __get_functions_resource(self, client):
        return client.projects()
                    .locations()
                    .functions()


    def create_function(self, name, location, body):
        """Create a Cloud Function"""
        self.__get_functions_resource(self.client).create(location, body)
    

    def call_function(self, name, body):
        """Call a Cloud Function"""
        self.__get_functions_resource(self.client).call(name, body)
