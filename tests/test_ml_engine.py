"""Test compute.py"""
import unittest
from freezegun import freeze_time
from googleapiclient.http import HttpMock, HttpMockSequence
from gcloud_utils import ml_engine
from mock import patch, Mock
import json
class TestMlEngine(unittest.TestCase):
    """Test Compute Class"""
    maxDiff = None

    def test_create_new_model(self):
        """Test request to create a new model"""
        http = HttpMock('tests/fixtures/ml_engine/first_result.json', {'status': '200'})
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)
        post_to_create_model = ml_engine_test.create_new_model("MODEL", "DESCRIPTION")

        # Test Method
        self.assertEqual(post_to_create_model.method, "POST")
        # Test API
        self.assertEqual(post_to_create_model.uri, "https://ml.googleapis.com/v1/projects/PROJECT/models?alt=json")
        # Test Body Post
        expected = {"name": "MODEL", "description": "DESCRIPTION"}
        self.assertDictEqual(json.loads(post_to_create_model.body), expected)

    def test_create_new_model_version_4_5(self):
        """Test the creation of new model"""
        http = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/ml_engine/versions_models.json', 'rb').read())
        
        ])
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)

        (post_to_create_model, new_version) = ml_engine_test.increase_model_version("MODEL", "JOB_ID")

        self.assertDictEqual(
            json.loads(post_to_create_model.body),
            {"deploymentUri": "gs://BUCKET_NAME/jobs/JOB_ID/export", "name": "v4_5"}
        )
        self.assertEqual(new_version, "v4_5")

    def test_create_new_model_version_5_0(self):
        """Test the creation of new model"""
        http = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/ml_engine/versions_models_4_9.json', 'rb').read())
        
        ])
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)

        (post_to_create_model, new_version) = ml_engine_test.increase_model_version("MODEL", "JOB_ID")

        self.assertDictEqual(
            json.loads(post_to_create_model.body),
            {"deploymentUri": "gs://BUCKET_NAME/jobs/JOB_ID/export", "name": "v5_0"}
        )
        self.assertEqual(new_version,"v5_0")

    def test_create_new_model_version_empty(self):
        """Test the creation of new model"""
        http = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/ml_engine/versions_models_empty.json', 'rb').read())
        
        ])
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)

        (post_to_create_model, new_version) = ml_engine_test.increase_model_version("MODEL", "JOB_ID")

        self.assertEqual(
            json.loads(post_to_create_model.body),
            {"deploymentUri": "gs://BUCKET_NAME/jobs/JOB_ID/export", "name": "v0_1"}
        )
        self.assertEqual(new_version, "v0_1")

    def test_create_new_model_version_empty_with_python_runtime_and_framework(self):
        """Test the creation of new model version with specific python version, runtime and framework"""
        http = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/ml_engine/versions_models_empty.json', 'rb').read())
        ])
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)

        (post_to_create_model, new_version) = ml_engine_test.increase_model_version("MODEL", "JOB_ID", "3.5", "1.14", "TENSORFLOW")

        self.assertEqual(
            json.loads(post_to_create_model.body),
            {
                "deploymentUri": "gs://BUCKET_NAME/jobs/JOB_ID/export",
                "name": "v0_1",
                "pythonVersion": "3.5",
                "runtimeVersion": "1.14",
                "framework": "TENSORFLOW"
            }
        )
        self.assertEqual(new_version, "v0_1")

    def test_set_model_version_as_default(self):
        """"Test method that set version as default"""
        http = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/ml_engine/versions_models_empty.json', 'rb').read())
        ])
        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)
        result = ml_engine_test.set_version_as_default("model_teste", "v1_0")

        self.assertEqual(result.body, '{}')
        self.assertEqual(
            result.uri,
            "https://ml.googleapis.com/v1/projects/PROJECT/models/model_teste/versions/v1_0:setDefault?alt=json"
        )

    @freeze_time("1994-04-27 12:00:01")
    def test_start_prediciton_job(self):
        """"Teste request to start a training job"""
        http = HttpMock('tests/fixtures/ml_engine/first_result.json', {'status': '200'})

        input_file = "gs://BUCKET/recommendation/matrix_prefs/PRODUTO/input/part-00000"
        output_file="gs://BUCKET/recommendation/matrix_prefs/PRODUTO/output/part-00000"

        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)

        post_to_create = ml_engine_test.start_predict_job("PRODUTO", "MODEL_NAME", [input_file], output_file)

        body_expected = {
            "predictionInput": 
                {
                    "outputPath": "gs://BUCKET/recommendation/matrix_prefs/PRODUTO/output/part-00000",
                    "region": "us-east1",
                    "inputPaths": [
                        "gs://BUCKET/recommendation/matrix_prefs/PRODUTO/input/part-00000"
                        ],
                    "modelName": "projects/PROJECT/models/MODEL_NAME",
                    "dataFormat": "JSON"
                },
            "jobId": "PRODUTO_MODEL_NAME_1994_04_27_12_00_01_prediction"
        }
        method_expected = "POST"
        uri_expected = "https://ml.googleapis.com/v1/projects/PROJECT/jobs?alt=json"

        # Test Method
        self.assertEqual(post_to_create.method, method_expected)
        # Test API
        self.assertEqual(post_to_create.uri, uri_expected)
        # Test Body Post
        self.assertDictEqual(json.loads(post_to_create.body), body_expected)

    @freeze_time("1994-04-27 12:00:01")
    def test_start_prediciton_job_exception_input_type_invalid(self):
        """"Teste create a prediciton job and input_file type isn't a list"""
        http = HttpMock('tests/fixtures/ml_engine/first_result.json', {'status': '200'})

        input_file = "gs://BUCKET/recommendation/matrix_prefs/PRODUTO/input/part-00000"
        output_file = "gs://BUCKET/recommendation/matrix_prefs/PRODUTO/output/part-00000"

        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)

        self.assertRaises(TypeError, ml_engine_test.start_predict_job, "PRODUTO", "MODEL_NAME", input_file, output_file)

    def test_job_list(self):
        """TEST JOB LIST AND FILTER BY FINAL STATE"""
        jobs_mock = open('tests/fixtures/ml_engine/jobs.json', 'rb').read()

        http = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'}, jobs_mock),
            ({'status': '200'}, jobs_mock),
            ({'status': '200'}, jobs_mock)
        ])

        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)
        self.assertEqual(len(ml_engine_test.list_jobs()), 19)
        self.assertEqual(len(ml_engine_test.list_jobs(filter_final_state=None)), 20)
        self.assertEqual(len(ml_engine_test.list_jobs(filter_final_state="STATE_WRONG")), 0)

    def test_get_job(self):
        """Test getting a job"""

        http = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/ml_engine/get_job_succeeded.json', 'rb').read()),
        ])

        job_id = "globoplay_autoencoder_keras_2018_08_06_10_00_37"

        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)
        job = ml_engine_test.get_job(job_id)
        self.assertEqual(job['jobId'], job_id)

    def test_wait_job_to_finish(self):
        """Test waiting job to finish execution"""

        http = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/ml_engine/get_job_running.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/ml_engine/get_job_succeeded.json', 'rb').read())
        ])

        job_id = "globoplay_autoencoder_keras_2018_08_06_10_00_37"

        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)
        state = ml_engine_test.wait_job_to_finish(job_id, sleep_time=0)
        self.assertEqual(state, "SUCCEEDED")

    @patch('gcloud_utils.ml_engine.discovery')
    def test_delete_model_version(self, mock_discovery):
        """Test delete model version"""
        mock_resource = Mock()
        mock_discovery.build.return_value = mock_resource
        mock_resource.projects.return_value = mock_resource
        mock_resource.models.return_value = mock_resource
        mock_resource.versions.return_value = mock_resource
        mock_resource.delete.return_value = mock_resource

        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION")
        ml_engine_test.delete_model_version("MODEL", "v4_8")

        mock_resource.delete.assert_called_with(name="projects/PROJECT/models/MODEL/versions/v4_8")

    def test_delete_older_model_versions(self):
        """Test delete older model_versions.

        The test is loading 7 versions and should keep 5 of them, meaning that the 2 older ones should be deleted.
        In this case, v3 and v4 are the older ones
        """

        http = HttpMockSequence([
            ({'status': '200'}, open('tests/fixtures/ml_engine/first_result.json', 'rb').read()),
            ({'status': '200'}, open('tests/fixtures/ml_engine/versions_models.json', 'rb').read()),
        ])

        ml_engine_test = ml_engine.MlEngine("PROJECT", "BUCKET_NAME", "REGION", http=http)

        with patch.object(ml_engine_test, 'delete_model_version') as mocked:
            ml_engine_test.delete_older_model_versions("autoencoder_model", 5)
            mocked.assert_any_call("autoencoder_model", "v3")
            mocked.assert_any_call("autoencoder_model", "v4")
