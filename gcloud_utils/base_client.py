"""base client to google services"""

import os
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

class BaseClient(object):
    """ BaseClient BaseClient to google services"""

    CREDENTIAL_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
    MODEL_CLIENT = None

    def __init__(self, client=None):
        self.logger = logging.getLogger(name=self.__class__.__name__)
        if client:
            self._client = client
        elif os.environ.get(self.CREDENTIAL_ENV):
            self._client = self.MODEL_CLIENT.Client.from_service_account_json(
                os.environ.get(self.CREDENTIAL_ENV)
            )
        else:
            raise Exception(
                "Need a client or {} environment of your credentials"
                .format(self.CREDENTIAL_ENV)
                )
            