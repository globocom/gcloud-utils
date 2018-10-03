import os

class BaseClient(object):
    CREDENTIAL_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
    MODEL_CLIENT = None

    def __init__(self, client=None):
        if client:
            self._client = client
        elif os.environ.get(self.CREDENTIAL_ENV):
            self._client = self.MODEL_CLIENT.Client.from_service_account_json(
                os.environ.get(self.CREDENTIAL_ENV)
            )
        else:
            raise Exception("Need a client or {} environment of your credentials".format(self.CREDENTIAL_ENV))
            