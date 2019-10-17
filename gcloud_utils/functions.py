"""Module to handle Google Cloud Functions Service"""

import os
import json
import logging
from googleapiclient import discovery
from googleapiclient.errors import HttpError

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')


class Functions(object):
    """Google-Cloud-Functions handler"""

    def __init__(self, project, zone):
        self.project = project
        self.zone = zone
        self.logger = logging.getLogger(name=self.__class__.__name__)
        self.client = discovery.build('cloudfunctions', 'v1beta2')
        self.functions = self.__get_functions_resource(self.client)
        self.parent = self.__build_parent()

    def __build_parent(self):
        return 'projects/{}/locations/{}'.format(self.project, self.zone)

    def __get_functions_resource(self, client):
        return client.projects().locations().functions()

    def __get_upload_url(self):
        generate_upload_url_request = self.functions.generateUploadUrl(
            parent=self.parent)
        res = self.__execute_request(generate_upload_url_request)
        return res['uploadUrl']

    def __upload_funtion(path):
        pass

    def __build_function(self, name, runtime, path,  trigger):
        upload_url = self.__get_upload_url()
        body = {
            "entryPoint": name,
            "runtime": runtime,
            "sourceUploadUrl": upload_url,
            "name": '{}/functions/{}'.format(self.parent, name)
        }

        return self.functions.create(location=self.parent, body=body)

    def __execute_request(self, request):
        return request.execute()

    def create_function(self, name, runtime, trigger, path=os.getcwd()):
        request = self.__build_function(name, runtime, path, trigger)

        try:
            res = self.__execute_request(request)
        except HttpError as err:
            header, body = err.args
            err_message = json.loads(body.decode('utf-8'))['error']['message']
            logger.info('[ERROR] ' + err_message)
        return res

    def list_functions(self):
        request = self.functions.list(location=self.parent)
        return self.__execute_request(request)

    def describe_function(self, name):
        request = self.functions.get(
            name='{}/functions/{}'.format(self.parent, name))
        return self.__execute_request(request)

    def call_function(self, name, data):
        """Call a Cloud Function"""

        function = 'projects/{}/locations/{}/functions/{}'.format(
            self.project, self.zone, name)

        request = self.functions.call(name=function, body={'data': data})

        return self.__execute_request(request)
