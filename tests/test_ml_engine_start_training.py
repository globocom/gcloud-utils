"""Test compute.py"""
import copy
import json
import unittest
from freezegun import freeze_time
from googleapiclient.http import HttpMock, HttpMockSequence
from gcloud_utils import ml_engine
from mock import patch

class TestMlEngine(unittest.TestCase):
    """Test Compute Class"""
    maxDiff = None

    def setUp(self):
        http = HttpMock('tests/fixtures/ml_engine/first_result.json', {'status': '200'})
        self.ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION",http=http)
        self.main_body = {"trainingInput": {
                            "runtimeVersion": "1.0",
                            "region": "REGION",
                            "pythonModule": "PACOTE.MODULO",
                            "jobDir": "gs://BUCKET_NAME/jobs/PRODUTO_MODULO_1994_04_27_12_00_01",
                            "packageUris": ["gs://BUCKET_NAME/packages/PACOTE"],
                            "args": []
                            },
                        "jobId": "PRODUTO_MODULO_1994_04_27_12_00_01"}

    @freeze_time("1994-04-27 12:00:01")
    def test_create_training_job_default_post(self):
        self.main_body["trainingInput"]["args"] = [ "--train-file", "gs://rec-alg/recommendation/matrix_prefs/PRODUTO/train_one_file/part-00000",
                                            "--test-file", "gs://rec-alg/recommendation/matrix_prefs/PRODUTO/test_one_file/part-00000",
                                            "--metadata-file", "gs://rec-alg/recommendation/matrix_prefs/PRODUTO/metadata/part-00000" ]

        post_to_create = self.ml_engine_test.start_training_job("PRODUTO", "PACOTE", "PACOTE.MODULO",
            train_file="gs://rec-alg/recommendation/matrix_prefs/PRODUTO/train_one_file/part-00000",
            test_file="gs://rec-alg/recommendation/matrix_prefs/PRODUTO/test_one_file/part-00000",
            metadata_file="gs://rec-alg/recommendation/matrix_prefs/PRODUTO/metadata/part-00000")

        # Test Method
        self.assertEqual(post_to_create.method, "POST")
        # Test API
        self.assertEqual(post_to_create.uri, "https://ml.googleapis.com/v1/projects/PROJECT/jobs?alt=json")
        # Test Body Post
        self.assertDictEqual(json.loads(post_to_create.body), self.main_body)

    @freeze_time("1994-04-27 12:00:01")
    def test_create_training_job_with_packages(self):
        self.main_body["trainingInput"]["packageUris"] = ["gs://BUCKET_NAME/packages/PACOTE.EXTRA", "gs://BUCKET_NAME/packages/PACOTE"]

        post_to_create = self.ml_engine_test.start_training_job("PRODUTO", "PACOTE", "PACOTE.MODULO", ["PACOTE.EXTRA"])

        # Test Body Post
        self.assertDictEqual(json.loads(post_to_create.body), self.main_body)

    @freeze_time("1994-04-27 12:00:01")
    def test_create_training_job_with_runtime_version(self):
        self.main_body["trainingInput"]["runtimeVersion"] = "1.8"

        post_to_create =self. ml_engine_test.start_training_job("PRODUTO", "PACOTE", "PACOTE.MODULO", runtime_version="1.8")

        # Test Body Post
        self.assertDictEqual(json.loads(post_to_create.body), self.main_body)

    @freeze_time("1994-04-27 12:00:01")
    def test_create_training_job_with_python_version(self):
        self.main_body["trainingInput"]["pythonVersion"] = "3.5"

        post_to_create = self.ml_engine_test.start_training_job("PRODUTO", "PACOTE", "PACOTE.MODULO", python_version="3.5")
        # Test Body Post
        self.assertDictEqual(json.loads(post_to_create.body), self.main_body)
