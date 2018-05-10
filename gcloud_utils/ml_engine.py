"""Submit Job to ML Engine"""
import logging
import datetime
from googleapiclient import discovery

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

REGION = "us-east1"
PROJECT = "recomendacao-gcom"
BUCKET_NAME = "rec-alg"
PACKAGE_PATH = "packages"

class MlEngine(object):
    """Google-ml-engine handler"""
    def __init__(self, project, bucket_name,region, package_path=PACKAGE_PATH):
        self.project = project
        self.parent = "projects/{}".format(self.project)
        self.bucket_name = bucket_name
        self.region = region
        self.package_full_path = "gs://{}/{}".format(self.bucket_name, package_path)
        self.__logger = logging.getLogger(name=self.__class__.__name__)
        self.client = discovery.build('ml', 'v1')

    def list_models(self):
        """List all models in project"""
        request = self.client.projects().models()\
        .list(parent=self.parent)\
        .execute()
        return request

    def start_training_job(self,product,package_name,module):
        """Start a training job"""

        package_uri = "{}/{}".format(self.package_full_path,package_name)
        suffix_module = module.split(".")[-1]
        date_formated = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        job_id = "{prod}_{mod}_{date}".format(prod=product,mod=suffix_module,date=date_formated)
        request = self.client.projects()

        body_request = {
            "jobId" : job_id,
            "trainingInput":{
            "packageUris": [package_uri],
            "pythonModule": module,
            "args": [
                "--train-file",
                "gs://rec-alg/recommendation/matrix_prefs/{}/train_one_file/part-00000".format(product),
                "--test-file",
                "gs://rec-alg/recommendation/matrix_prefs/{}/test_one_file/part-00000".format(product),
                "--metadata-file",
                "gs://rec-alg/recommendation/matrix_prefs/{}/metadata/part-00000".format(product)
            ],
            "region": self.region,
            "runtimeVersion": "1.0",
            "jobDir": "gs://rec-alg/jobs/{}".format(job_id)
            }
        }
        job = request.jobs().create(parent=self.parent, body=body_request)
        result = job.execute()
        print result
        return 0
        
    def create_model(self):
        """Create new model"""
        # request_dict = {'name': 'modelTeste',
        # 'description': 'This is a machine learning model entry.'}
        request = self.client.projects().models()
        response = request.execute()
        print(response)
        
ml = MlEngine(PROJECT,BUCKET_NAME,REGION)
ml.start_training_job("globoplay","autoencoder-1.0.tar.gz","trainer.autoencoder_keras")
    