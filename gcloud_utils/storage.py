"""Start gcloud instance"""
import time
import os
import logging
from gcloud import storage

class Storage(object):
    """Google-Storage handler"""
    def __init__(self, bucket="rec-alg", tmp_path=".tmp"):
        self.logger = logging.getLogger(name=self.__class__.__name__)

        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket)
        self.tmp_path = os.path.join(os.getcwd(), tmp_path)
        self.__setup_tmp_path(self.tmp_path)

    def __setup_tmp_path(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def __path_as_local(self,path):
        return os.path.join(self.tmp_path,path)

    def path_exists_local(self,storage_path):
        """Check if path exists local"""
        local_path = self.__path_as_local(storage_path)
        print("PATH EXISTS? %s"%os.path.exists(local_path))
        return os.path.exists(local_path)

    def path_exists_storage(self,path):
        """Check if path exists on Storage"""
        return  self.bucket.blob(path).exists()

    def download_file(self, storage_path):
        """ Download Storage file to local tmp path"""
        obj = self.bucket.blob(storage_path)
        local_path = self.__path_as_local(storage_path)
        path_to_create = "/".join(local_path.split("/")[:-1])
        self.__setup_tmp_path(path_to_create)
        with open(self.tmp_path+"/"+storage_path, 'wb') as tmp_file:
            obj.download_to_file(tmp_file)

    def __filter_suffix_files(self, blobs, suffix):
        return [x for x in blobs if x.name.endswith(suffix)]

    def list_files(self, path, filter_suffix=None):
        """List all blobs in path"""
        blobs = self.bucket.list_blobs(prefix=path)
        blobs_files = [x for x in blobs if not x.name.endswith("/")]

        if filter_suffix is not None:
            return self.__filter_suffix_files(blobs_files, filter_suffix)
        else:
            return blobs_files

    def download_files(self, path, filter_suffix=None):
        """Download all files in path"""
        list_paths = self.list_files(path, filter_suffix=filter_suffix)
        for path_to_download in list_paths:
            self.download_file(path_to_download.name)

    def get_file(self, file_path):
        """Get all files from Storage path"""
        path = "/".join(file_path.split("/")[:-1])
        local_file_path = self.__path_as_local(file_path)
        local_path = self.__path_as_local(path)
        self.__setup_tmp_path(local_path)

        if self.path_exists_local(file_path):
            self.logger.debug("Get local...")
            return open(local_file_path)
        else:
            self.logger.debug("Download file...")
            self.download_file(file_path)
            return self.get_file(file_path)

    def get_files_in_path(self,path):
        files = self.list_files(path)
        result = []
        for file_blob in files:
            result.append(self.get_file(file_blob.name))
        return result

    def upload_value(self, storage_path, value):
        """Upload a value to  Storage"""
        self.bucket.blob(storage_path).upload_from_string(value)

    def upload_file(self, storage_path, local_path):
        """Upload one local file to Storage"""
        with open(local_path) as loc:
            self.logger.debug("Upload file %s to %s",local_path, storage_path)
            self.bucket.blob(storage_path).upload_from_file(loc)

    def __local_subfiles(self, local_path_base):
        list_files = os.walk(local_path_base)
        for a in list_files:
            if len(a[2]) > 0:
                for i in a[2]:
                    yield "%s/%s"%(a[0],i)

    def upload_path(self,storage_path_base, local_path_base):
        """Upload all filer from local path to Storage"""
        for file_path in self.__local_subfiles(local_path_base):
            file_path_to_save = file_path[len(local_path_base)+1:]
            storage_path = os.path.join(storage_path_base,file_path_to_save)
            self.upload_file(storage_path,file_path)