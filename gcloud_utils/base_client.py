"""base client to google services"""

import os
import logging

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')


class BaseClient(object):
    """ BaseClient BaseClient to google services"""

    CREDENTIAL_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
    _MODEL_CLIENT = None

    def __init__(self, client=None, log_level=logging.ERROR):
        self.logger = logging.getLogger(name=self.__class__.__name__)
        self.logger.setLevel(log_level)
        if client:
            self._client = client
        elif os.environ.get(self.CREDENTIAL_ENV):
            self._client = self._MODEL_CLIENT.Client.from_service_account_json(
                os.environ.get(self.CREDENTIAL_ENV)
            )
        else:
            raise Exception(
                "Need a client or {} environment of your credentials"
                .format(self.CREDENTIAL_ENV)
            )
