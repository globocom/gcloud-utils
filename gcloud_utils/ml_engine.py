"""Submit Job to ML Engine"""
import datetime
import logging
import re
import time
import pickle
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

PACKAGE_PATH = "packages"
JOB_DIR = "jobs"


class MlEngine(object):
    """Google-ml-engine handler"""

    def __init__(self, project, bucket_name,
                 region, package_path=PACKAGE_PATH,
                 job_dir=JOB_DIR, http=None, credentials_path=None):

        self.project = project
        self.parent = "projects/{}".format(self.project)
        self.bucket_name = bucket_name
        self.region = region
        self.package_full_path = "gs://{}/{}".format(
            self.bucket_name, package_path)
        self.job_dir_suffix = "gs://{}/{}".format(self.bucket_name, job_dir)
        self.__logger = logging.getLogger(name=self.__class__.__name__)

        credentials = (
            None
            if not credentials_path
            else GoogleCredentials.from_stream(credentials_path)
        )

        self.client = discovery.build(
            'ml', 'v1', http=http, credentials=credentials)

    def __get_model_versions_with_metadata(self, model_name):
        parent_model = self.__parent_model_name(model_name)
        request = self.client\
            .projects()\
            .models()\
            .versions()

        version_list = request.list(parent=parent_model).execute()

        result = []
        try:
            result = version_list['versions']
        except KeyError as error:
            self.__logger.error(
                "Error on get version: %s\nVersions list is empty", error)

        while 'nextPageToken' in version_list:
            token = version_list['nextPageToken']
            version_list = request.list(
                parent=parent_model, pageToken=token).execute()
            page_result = version_list['versions']
            result.extend(page_result)

        return result

    def get_model_versions(self, model_name):
        """Return all versions"""
        model_versions = self.__get_model_versions_with_metadata(model_name)
        if model_versions:
            return [x['name'].split("/")[-1] for x in model_versions]
        return ["v0_0"]

    def __get_last_version(self, versions):
        versions_float = [float(version[1:].replace("_", "."))
                          for version in versions]
        last_version = str(max(versions_float))
        return "v{}".format(last_version.replace(".", "_"))

    def __increase_version(self, version):
        pattern = r"^v\d+_\d$"
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
        return self.parent + "/models/" + model_name

    def __parse_start_training_args(self, args):
        formated_args = []
        for key in args:
            formated_args.append("--{}".format(key.replace("_", "-")))
            formated_args.append(args[key])
        return formated_args

    def create_new_model(self, name, description='Ml model'):
        """Create new model"""
        request_dict = {
            'name': name,
            'description': description
        }
        request = self.client.projects().models().create(
            parent=self.parent, body=request_dict)
        return request

    def export_model(self, clf, model_path="model.pkl"):
        """
        Export a classifier/pipeline to model path.
        Frameworks supported : XGBoost booster, Scikit-learn estimator and
        pipelines.
        """

        try:
            with open(model_path, 'wb') as model_file:
                pickle.dump(clf, model_file)
        except Exception as error:
            logging.error("Failed to export model to %s.", model_path)
            raise error

    def predict_json(self, project, model, instances, version=None):
        """Send json data to a deployed model for prediction."""
        service = discovery.build('ml', 'v1')
        name = 'projects/{}/models/{}'.format(project, model)

        if version is not None:
            name += '/versions/{}'.format(version)

        response = service.projects().predict(
            name=name,
            body={'instances': instances}
        ).execute()

        if 'error' in response:
            raise RuntimeError(response['error'])

        return response['predictions']

    def create_model_version(self, model_name, version, job_id,
                             python_version="", runtime_version="",
                             framework=""):
        """Increase Model version"""
        parent_model = self.__parent_model_name(model_name)
        body_request = {
            "name": version,
            "deploymentUri": "{}/{}/export".format(self.job_dir_suffix, job_id)
        }

        if python_version:
            body_request["pythonVersion"] = python_version

        if runtime_version:
            body_request["runtimeVersion"] = runtime_version

        if framework:
            body_request["framework"] = framework

        request = self.client\
            .projects()\
            .models()\
            .versions()\
            .create(body=body_request, parent=parent_model)

        return request

    def delete_model_version(self, model_name, version):
        """Delete Model version"""
        self.__logger.info("Deleting version %s of model %s",
                           version, model_name)

        name = "{}/versions/{}".format(
            self.__parent_model_name(model_name), version)
        request = self.client \
            .projects() \
            .models() \
            .versions() \
            .delete(name=name) \
            .execute()

        return request

    def delete_older_model_versions(self, model_name, n_versions_to_keep):
        """Keep the most recents model versions and delete older ones.
        The number of models to keep is specified by the parameter n_versions_to_keep"""

        def _get_use_time(version):
            use_time = version.get('lastUseTime') or version.get('createTime')
            return time.strptime(use_time, "%Y-%m-%dT%H:%M:%SZ")

        versions = self.__get_model_versions_with_metadata(model_name)
        versions.sort(key=_get_use_time, reverse=True)
        versions_name = [x['name'].split("/")[-1] for x in versions]
        remove_versions = versions_name[n_versions_to_keep:]

        for version in remove_versions:
            self.delete_model_version(model_name, version)

    def increase_model_version(self, model_name, job_id, python_version="",
                               runtime_version="", framework=""):
        """Increase Model version"""

        versions = self.get_model_versions(model_name)
        last_version = self.__get_last_version(versions)
        new_version = self.__increase_version(last_version)

        request = self.create_model_version(
            model_name, new_version, job_id,
            python_version, runtime_version, framework)

        return (request, new_version)

    def set_version_as_default(self, model, version):
        """Set a model version as default"""
        version_full_path = "{}/models/{}/versions/{}".format(
            self.parent, model, version)
        request = self.client\
            .projects()\
            .models()\
            .versions()\
            .setDefault(body={}, name=version_full_path)
        return request

    def list_jobs(self, filter_final_state='SUCCEEDED'):
        """List all models in project"""
        jobs = self.client.projects().jobs().list(parent=self.parent).execute()
        list_of_jobs = jobs['jobs']
        if filter_final_state:
            list_of_jobs = [
                x for x in list_of_jobs if x['state'] == filter_final_state]
        return [x['jobId'] for x in list_of_jobs]

    def list_models(self):
        """List all models in project"""
        request = self.client.projects().models()\
            .list(parent=self.parent)\
            .execute()
        return request

    def get_job(self, job_id):
        """Describes a job"""
        name = "{}/jobs/{}".format(self.parent, job_id)
        job = self.client.projects().jobs().get(name=name).execute()
        return job

    def wait_job_to_finish(self, job_id, sleep_time=60):
        """Waits job to finish"""

        job = self.get_job(job_id)
        state = job['state']
        self.__logger.info("%s state: %s", job_id, state)

        while state not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            time.sleep(sleep_time)
            job = self.get_job(job_id)
            state = job['state']
            self.__logger.info("%s state: %s", job_id, state)

        self.__logger.info("Job finished with status %s", state)
        return state

    def start_training_job(self, job_id_prefix, package_name, module,
                           extra_packages=None, runtime_version="1.0",
                           python_version="", scale_tier="", master_type="",
                           worker_type="", parameter_server_type="",
                           worker_count="", parameter_server_count="", **args):
        """Start a training job"""

        if extra_packages is None:
            extra_packages = []

        main_package_uri = "{}/{}".format(self.package_full_path, package_name)
        packages_uris = ["{}/{}".format(self.package_full_path, ep)
                         for ep in extra_packages]
        packages_uris.append(main_package_uri)

        suffix_module = module.split(".")[-1]
        date_formated = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        job_id = "{prefix}_{mod}_{date}".format(
            prefix=job_id_prefix,
            mod=suffix_module,
            date=date_formated)
        request = self.client.projects()
        job_dir = "{}/{}".format(self.job_dir_suffix, job_id)

        formated_args = self.__parse_start_training_args(args)

        body_request = {
            "jobId": job_id,
            "trainingInput": {
                "packageUris": packages_uris,
                "pythonModule": module,
                "args": formated_args,
                "region": self.region,
                "runtimeVersion": runtime_version,
                "jobDir": job_dir
            }
        }

        if python_version:
            body_request["trainingInput"]["pythonVersion"] = python_version

        if scale_tier:
            body_request["trainingInput"]["scaleTier"] = scale_tier

        if master_type:
            body_request["trainingInput"]["masterType"] = master_type

        if worker_type:
            body_request["trainingInput"]["workerType"] = worker_type

        if parameter_server_type:
            body_request["trainingInput"]["parameterServerType"] = parameter_server_type

        if worker_count:
            body_request["trainingInput"]["workerCount"] = worker_count

        if parameter_server_count:
            body_request["trainingInput"]["parameterServerCount"] = parameter_server_count

        job = request.jobs().create(parent=self.parent, body=body_request)
        return job

    def start_predict_job(self, job_id_prefix, model, input_path, output_path):
        """start a prediction job"""
        if not isinstance(input_path, list):
            raise TypeError("input_path must be a list")

        request = self.client.projects()
        model_full_path = "{}/models/{}".format(self.parent, model)

        date_formated = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        job_id = "{}_{}_{}_prediction".format(
            job_id_prefix, model, date_formated)

        body_request = {
            "jobId": job_id,
            "predictionInput": {
                "modelName": model_full_path,
                "dataFormat": "JSON",
                "inputPaths": input_path,
                "outputPath": output_path,
                "region": "us-east1"
            }
        }
        job = request.jobs().create(parent=self.parent, body=body_request)
        return job
