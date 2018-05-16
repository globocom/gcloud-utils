"""Module to download and use files from Google Storage"""
import os
import logging
from gcloud import storage

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')


class Storage(object):
    """Google-Storage handler"""
    def __init__(self, bucket="rec-alg", http=None):
        self.logger = logging.getLogger(name=self.__class__.__name__)
        self.client = storage.Client(http=http)
        self.bucket = self.client.get_bucket(bucket)

    def __filter_suffix_files(self, blobs, suffix):
        return [x for x in blobs if x.name.endswith(suffix)]

    def __get_file_name_from_path(self, full_path):
        if not os.path.isfile(full_path):
            raise ValueError("{} is not a file".format(full_path))
        return full_path.split("/")[-1]

    def __prepare_path(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def download_file(self, storage_path, local_path):
        """Download Storage file to local path, creating a path at local_path if nedded"""
        obj = self.bucket.blob(storage_path)
        local_file_full_path = os.path.join(local_path, obj.name)
        self.__prepare_path(os.path.dirname(local_file_full_path))
        with open(local_file_full_path, 'wb') as local_file:
            obj.download_to_file(local_file)
        return local_file_full_path

    def download_files(self, path, local_path, filter_suffix=None):
        """Download all files in path"""
        list_paths = self.list_files(path, filter_suffix=filter_suffix)
        for path_to_download in list_paths:
            self.download_file(path_to_download.name, local_path)

    def get_abs_path(self, storage_path):
        """get abs path from GStorage"""
        bucket_path = "gs://{}/".format(self.bucket.name)
        return os.path.join(bucket_path, storage_path)

    def get_file(self, file_path, local_path):
        """Get all files from Storage path"""
        self.logger.debug("Download file...")
        full_path = self.download_file(file_path, local_path)
        return open(full_path)

    def get_files_in_path(self, path, local_path):
        """Download all files from path in Google Storage and return a list with those files"""
        files = self.list_files(path)
        result = []
        for file_blob in files:
            result.append(self.get_file(file_blob.name, "{}/{}".format(local_path, file_blob.name)))
        return result

    def list_files(self, path, filter_suffix=None):
        """List all blobs in path"""
        blobs = self.bucket.list_blobs(prefix=path)
        blobs_files = [x for x in blobs if not x.name.endswith("/")]

        if filter_suffix is not None:
            return self.__filter_suffix_files(blobs_files, filter_suffix)
        else:
            return blobs_files

    def path_exists_storage(self, path):
        """Check if path exists on Storage"""
        return  self.bucket.blob(path).exists()

    def upload_file(self, storage_path, local_path):
        """Upload one local file to Storage"""
        with open(local_path) as loc:
            self.logger.debug("Upload file %s to %s", local_path, storage_path)
            self.bucket.blob(storage_path).upload_from_file(loc)

    def upload_path(self, storage_path_base, local_path_base):
        """Upload all filer from local path to Storage"""
        for root, _, files in os.walk(local_path_base):
            for file_to_upload in files:
                full_path_upload = os.path.join(root, file_to_upload)
                storage_path = os.path.join(storage_path_base, file_to_upload)
                self.upload_file(storage_path, full_path_upload)

    def upload_value(self, storage_path, value):
        """Upload a value to  Storage"""
        self.bucket.blob(storage_path).upload_from_string(value)
