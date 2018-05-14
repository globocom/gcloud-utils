"""Submit Job to ML Engine"""
import logging
import datetime
import re
from googleapiclient import discovery

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

REGION = "us-east1"
PROJECT = "recomendacao-gcom"
BUCKET_NAME = "rec-alg"
PACKAGE_PATH = "packages"
JOB_DIR = "jobs"


class MlEngine(object):
    """Google-ml-engine handler"""
    def __init__(self, project, bucket_name, region, package_path=PACKAGE_PATH, job_dir=JOB_DIR, http=None):
        self.project = project
        self.parent = "projects/{}".format(self.project)
        self.bucket_name = bucket_name
        self.region = region
        self.package_full_path = "gs://{}/{}".format(self.bucket_name, package_path)
        self.job_dir_suffix = "gs://{}/{}".format(self.bucket_name, job_dir)
        self.__logger = logging.getLogger(name=self.__class__.__name__)
        self.client = discovery.build('ml', 'v1', http=http)

    def list_models(self):
        """List all models in project"""
        request = self.client.projects().models()\
        .list(parent=self.parent)\
        .execute()
        return request

    def start_training_job(self, product, package_name, module):
        """Start a training job"""

        package_uri = "{}/{}".format(self.package_full_path, package_name)
        suffix_module = module.split(".")[-1]
        date_formated = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        job_id = "{prod}_{mod}_{date}".format(prod=product, mod=suffix_module, date=date_formated)
        request = self.client.projects()
        job_dir = "{}/{}".format(self.job_dir_suffix, job_id)

        body_request = {
            "jobId" : job_id,
            "trainingInput":{
                "packageUris": [package_uri],
                "pythonModule": module,
                "args": [
                    "--train-file",
                    "gs://rec-alg/recommendation/matrix_prefs/{}/train_one_file/part-00000"
                    .format(product),
                    "--test-file",
                    "gs://rec-alg/recommendation/matrix_prefs/{}/test_one_file/part-00000"
                    .format(product),
                    "--metadata-file",
                    "gs://rec-alg/recommendation/matrix_prefs/{}/metadata/part-00000"
                    .format(product)
                ],
                "region": self.region,
                "runtimeVersion": "1.0",
                "jobDir": job_dir
            }
        }
        job = request.jobs().create(parent=self.parent, body=body_request)
        return job

    def create_new_model(self, name):
        """Create new model"""
        request_dict = {
            'name': name,
            'description': 'This is a machine learning model entry.'
            }
        request = self.client.projects().models().create(parent=self.parent, body=request_dict)
        return request

    def __increase_version(self, version):
        pattern = r"^v\d_\d$"
        result = re.match(pattern, version)
        if result is None:
            raise ValueError(
                'Parameter "version" value "{}" does not match the pattern "{}"'
                .format(version, pattern)
                )

        float_value = float(version[1:].replace("_", "."))
        new_version = str(float_value + 0.1)
        return "v{}".format(new_version.replace(".", "_"))

    def __parent_model_name(self, model_name):
        return self.parent+"/models/"+model_name

    def __get_versions(self, model_name):
        parent_model = self.__parent_model_name(model_name)
        version_list = self.client\
        .projects()\
        .models()\
        .versions()\
        .list(parent=parent_model)\
        .execute()

        if len(version_list)>0:
            return [x['name'].split("/")[-1] for x in version_list['versions']]
        else:
            return ["v0_0"]

    def create_new_model_version(self, model_name, job_id):
        """Increase Model version """

        versions = self.__get_versions(model_name)
        last_version = max(versions)
        new_version = self.__increase_version(last_version)
        parent_model = self.__parent_model_name(model_name)
        body_request = {
            "name": new_version,
            "deploymentUri": "{}/{}/export".format(self.job_dir_suffix, job_id)
            }

        request = self.client\
         .projects()\
         .models()\
         .versions()\
         .create(body=body_request, parent=parent_model)

        return request
