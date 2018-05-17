"""Test compute.py"""
import unittest
from freezegun import freeze_time
from googleapiclient.http import HttpMock, HttpMockSequence
from gcloud_utils import ml_engine
from mock import patch

class TestMlEngine(unittest.TestCase):
    """Test Compute Class"""

    @freeze_time("1994-04-27 12:00:01")
    def test_create_training_job(self):
        """"Teste request to start a training job"""
        http = HttpMock('tests/mock/ml_engine/first_result.json', {'status': '200'})
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION",http=http)
        post_to_create = ml_engine_test.start_training_job("PRODUTO", "PACOTE", "PACOTE.MODULO",
        train_file="gs://rec-alg/recommendation/matrix_prefs/PRODUTO/train_one_file/part-00000",
        test_file="gs://rec-alg/recommendation/matrix_prefs/PRODUTO/test_one_file/part-00000",
        metadata_file="gs://rec-alg/recommendation/matrix_prefs/PRODUTO/metadata/part-00000")
        # Test Method
        self.assertEqual(post_to_create.method, "POST")
        # Test API
        self.assertEqual(post_to_create.uri, "https://ml.googleapis.com/v1/projects/PROJECT/jobs?alt=json")
        # Test Body Post
        expected = """{"trainingInput": {"runtimeVersion": "1.0", "region": "REGION", "args": ["--train-file", "gs://rec-alg/recommendation/matrix_prefs/PRODUTO/train_one_file/part-00000", "--test-file", "gs://rec-alg/recommendation/matrix_prefs/PRODUTO/test_one_file/part-00000", "--metadata-file", "gs://rec-alg/recommendation/matrix_prefs/PRODUTO/metadata/part-00000"], "pythonModule": "PACOTE.MODULO", "jobDir": "gs://BUCKET_NAME/jobs/PRODUTO_MODULO_1994_04_27_12_00_01", "packageUris": ["gs://BUCKET_NAME/packages/PACOTE"]}, "jobId": "PRODUTO_MODULO_1994_04_27_12_00_01"}"""
        self.assertEqual(post_to_create.body, expected)

    def test_create_new_model(self):
        """Test request to create a new model"""
        http = HttpMock('tests/mock/ml_engine/first_result.json', {'status': '200'})
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION",http=http)
        post_to_create_model = ml_engine_test.create_new_model("MODEL", "DESCRIPTION")

        # Test Method
        self.assertEqual(post_to_create_model.method, "POST")
        # Test API
        self.assertEqual(post_to_create_model.uri, "https://ml.googleapis.com/v1/projects/PROJECT/models?alt=json")
        # Test Body Post
        expected = """{"name": "MODEL", "description": "DESCRIPTION"}"""
        self.assertEqual(post_to_create_model.body, expected)

    
    def test_create_new_model_version_4_5(self):
        """Test the creation of new model"""
        http = HttpMockSequence([
            ({'status': '200'},open('tests/mock/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'},open('tests/mock/ml_engine/versions_models.json', 'rb').read())
        
        ])
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION",http=http)

        post_to_create_model = ml_engine_test.create_new_model_version("MODEL","JOB_ID")
        self.assertEqual(post_to_create_model.body,'{"deploymentUri": "gs://BUCKET_NAME/jobs/JOB_ID/export", "name": "v4_5"}')

    def test_create_new_model_version_5_0(self):
        """Test the creation of new model"""
        http = HttpMockSequence([
            ({'status': '200'},open('tests/mock/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'},open('tests/mock/ml_engine/versions_models_4_9.json', 'rb').read())
        
        ])
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION",http=http)

        post_to_create_model = ml_engine_test.create_new_model_version("MODEL","JOB_ID")
        self.assertEqual(post_to_create_model.body,'{"deploymentUri": "gs://BUCKET_NAME/jobs/JOB_ID/export", "name": "v5_0"}')

    def test_create_new_model_version_empty(self):
        """Test the creation of new model"""
        http = HttpMockSequence([
            ({'status': '200'},open('tests/mock/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'},open('tests/mock/ml_engine/versions_models_empty.json', 'rb').read())
        
        ])
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION",http=http)

        post_to_create_model = ml_engine_test.create_new_model_version("MODEL","JOB_ID")
        self.assertEqual(post_to_create_model.body,'{"deploymentUri": "gs://BUCKET_NAME/jobs/JOB_ID/export", "name": "v0_1"}')

    def test_set_model_version_as_default(self):
        """"Test method that set version as default"""
        http = HttpMockSequence([
            ({'status': '200'},open('tests/mock/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'},open('tests/mock/ml_engine/versions_models_empty.json', 'rb').read())
        ])
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION",http=http)
        result = ml_engine_test.set_version_as_default("model_teste","v1_0")

        self.assertEqual(result.body, '{}')
        self.assertEqual(result.uri, "https://ml.googleapis.com/v1/projects/PROJECT/models/model_teste/versions/v1_0:setDefault?alt=json")

    @freeze_time("1994-04-27 12:00:01")
    def test_start_prediciton_job(self):
        """"Teste request to start a training job"""
        http = HttpMock('tests/mock/ml_engine/first_result.json', {'status': '200'})

        input_file = "gs://BUCKET/recommendation/matrix_prefs/PRODUTO/input/part-00000"
        output_file="gs://BUCKET/recommendation/matrix_prefs/PRODUTO/output/part-00000"

        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION",http=http)

        post_to_create = ml_engine_test.start_predict_job("PRODUTO", "MODEL_NAME", [input_file], output_file)

        body_expected = """{"predictionInput": {"outputPath": "gs://BUCKET/recommendation/matrix_prefs/PRODUTO/output/part-00000", "region": "us-east1", "inputPaths": ["gs://BUCKET/recommendation/matrix_prefs/PRODUTO/input/part-00000"], "modelName": "projects/PROJECT/models/MODEL_NAME", "dataFormat": "JSON"}, "jobId": "PRODUTO_MODEL_NAME_1994_04_27_12_00_01_prediction"}"""
        method_expected = "POST"
        uri_expected = "https://ml.googleapis.com/v1/projects/PROJECT/jobs?alt=json"

        # Test Method
        self.assertEqual(post_to_create.method, method_expected)
        # Test API
        self.assertEqual(post_to_create.uri, uri_expected)
        # Test Body Post
        self.assertEqual(post_to_create.body, body_expected)

    @freeze_time("1994-04-27 12:00:01")
    def test_start_prediciton_job_exception_input_type_invalid(self):
        """"Teste create a prediciton job and input_file type isn't a list"""
        http = HttpMock('tests/mock/ml_engine/first_result.json', {'status': '200'})

        input_file = "gs://BUCKET/recommendation/matrix_prefs/PRODUTO/input/part-00000"
        output_file="gs://BUCKET/recommendation/matrix_prefs/PRODUTO/output/part-00000"

        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION",http=http)

        self.assertRaises(TypeError ,ml_engine_test.start_predict_job, "PRODUTO", "MODEL_NAME", input_file, output_file)

