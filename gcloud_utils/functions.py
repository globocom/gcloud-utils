"""Module to handle Google Cloud Functions Service"""

import os
import re
import json
import logging
import zipfile
import requests
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
        self.functions = self.__get_functions_resource()
        self.parent = self.__build_parent()

    def __build_parent(self):
        return 'projects/{}/locations/{}'.format(self.project, self.zone)

    def __get_functions_resource(self):
        return self.client.projects().locations().functions()

    def __get_upload_url(self):
        generate_upload_url_request = self.functions.generateUploadUrl(
            parent=self.parent)
        res = self.__execute_request(generate_upload_url_request)
        return res['uploadUrl']

    def __execute_request(self, request):
        return request.execute()

    def __upload_function(self, path, filename, upload_url):
        name, extension = os.path.splitext(filename)
        self.__compress_function(path, name, extension)

        zip_filename = '{}.zip'.format(name)
        with open(os.path.join(path, zip_filename), 'rb') as zip_file:
            headers = {
                'content-type': 'application/zip',
                'x-goog-content-length-range': '0,104857600'
            }

            return requests.put(upload_url, data=zip_file, headers=headers)

    def __get_filename(self, runtime):
        filename = ""
        if re.match(r'^nodejs', runtime):
            filename = "index.js"
        elif runtime == "python37":
            filename = "main.py"
        return filename

    def __build_function(self, name, runtime, path):
        upload_url = self.__get_upload_url()

        filename = self.__get_filename(runtime)
        self.__upload_function(path, filename, upload_url)

        body = {
            "entryPoint": name,
            "runtime": runtime,
            "sourceUploadUrl": upload_url,
            "httpsTrigger": {},
            "name": '{}/functions/{}'.format(self.parent, name)
        }

        return self.functions.create(location=self.parent, body=body)

    def __compress_function(self, path, filename, extension):
        self.logger.info("Compressing File %s", filename)
        zip_file = zipfile.ZipFile('{}/{}.zip'.format(path, filename), 'w')
        zip_file.write(os.path.join(path, filename + extension),
                       compress_type=zipfile.ZIP_DEFLATED,
                       arcname=(filename + extension))
        zip_file.close()

    def create_function(self, name, runtime, path=os.getcwd()):
        """Create and Deploy a Cloud Function"""
        request = self.__build_function(name, runtime, path)

        try:
            res = self.__execute_request(request)
            return res
        except HttpError as err:
            body = err.args[1]
            err_message = json.loads(body.decode('utf-8'))['error']['message']
            self.logger.error(err_message)

    def list_functions(self):
        """List the cloud functions"""
        request = self.functions.list(location=self.parent)
        return self.__execute_request(request)

    def describe_function(self, name):
        """Describe a function"""
        request = self.functions.get(
            name='{}/functions/{}'.format(self.parent, name))
        return self.__execute_request(request)

    def call_function(self, name, data):
        """Call a Cloud Function"""

        function = 'projects/{}/locations/{}/functions/{}'.format(
            self.project, self.zone, name)

        request = self.functions.call(name=function, body={'data': data})

        return self.__execute_request(request)
