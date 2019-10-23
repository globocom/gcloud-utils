"""Module to download and use files from Google Storage"""
import os
import logging
from google.cloud import storage
from gcloud_utils.base_client import BaseClient


def _filter_suffix_files(blobs, suffix):
    return [x for x in blobs if x.name.endswith(suffix)]

def _prepare_path(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path

class Storage(BaseClient):
    """Google-Storage handler"""

    _MODEL_CLIENT = storage

    def __init__(self, bucket, client=None, log_level=logging.ERROR):
        super(Storage, self).__init__(client, log_level)
        self._bucket = self._client.get_bucket(bucket)

    def download_file(self, storage_path, local_path):
        """Download Storage file to local path, creating a path at local_path if nedded"""
        obj = self._bucket.get_blob(storage_path)
        local_file_full_path = os.path.join(local_path, obj.name)
        _prepare_path(os.path.dirname(local_file_full_path))
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
        bucket_path = "gs://{}/".format(self._bucket.name)
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
            result.append(self.get_file(file_blob.name,
                                        "{}/{}".format(local_path, file_blob.name)))
        return result

    def list_files(self, path, filter_suffix=None):
        """List all blobs in path"""
        blobs = self._bucket.list_blobs(prefix=path)
        blobs_files = [x for x in blobs if not x.name.endswith("/")]

        if filter_suffix is not None:
            return _filter_suffix_files(blobs_files, filter_suffix)
        return blobs_files

    def path_exists_storage(self, path):
        """Check if path exists on Storage"""
        return self._bucket.blob(path).exists()

    def upload_file(self, storage_path, local_path):
        """Upload one local file to Storage"""
        with open(local_path) as loc:
            self.logger.debug("Upload file %s to %s", local_path, storage_path)
            self._bucket.blob(storage_path).upload_from_file(loc)

    def upload_path(self, storage_path_base, local_path_base):
        """Upload all filer from local path to Storage"""
        for root, _, files in os.walk(local_path_base):
            for file_to_upload in files:
                full_path_upload = os.path.join(root, file_to_upload)
                storage_path = os.path.join(storage_path_base, file_to_upload)
                self.upload_file(storage_path, full_path_upload)

    def upload_value(self, storage_path, value):
        """Upload a value to  Storage"""
        self._bucket.blob(storage_path).upload_from_string(value)

    def delete_file(self, storage_path):
        """Deletes a blob from the bucket."""
        self._bucket.blob(storage_path).delete()

    def delete_path(self, storage_path):
        """Deletes all the blobs with storage_path prefix"""
        blobs = self.list_files(storage_path)

        for blob in blobs:
            blob.delete()
            self.logger.info("Blob %s deleted", blob.name)

    def rename_files(self, storage_prefix, new_path):
        """Renames all the blobs with storage_prefix prefix"""
        blobs = self.list_files(storage_prefix)

        for blob in blobs:
            new_name = blob.name.replace(storage_prefix, "")
            new_full_name = new_path + new_name
            self._bucket.rename_blob(blob, new_full_name)
            self.logger.info("Blob %s renamed to %s", blob.name, new_full_name)

    def ls(self, path):
        """List files directly under specified path"""
        blobs = self._bucket.list_blobs(prefix=path)
        items = []
        for blob in blobs:
            relative_path = blob.name.replace(path, "")
            if relative_path.startswith("/"):
                relative_path = relative_path.replace("/", "", 1)
            item = relative_path.split('/')[0]
            if item not in items:
                items.append(item)
        self.logger.info(items)

        return items
