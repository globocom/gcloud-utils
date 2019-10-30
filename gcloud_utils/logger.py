"""
Google logging.
"""

import logging as logger

from google.api_core.exceptions import NotFound
from google.cloud.logging._helpers import LogSeverity
from google.cloud import logging


def _format_text(text, args):
    """
    Format text.
    """

    if args:
        return text % args

    return str(text)


def getLogger(name, logger_id="gcloud_utils"):
    """
    Get logger.
    """

    log_form = '%(asctime)s - %(levelname)s - {} - %(message)s'.format(name)
    logger.basicConfig(level=logger.INFO, format=log_form)
    return Logger(logger_id)


class Logger(object):
    """
    Logger using the Google logging API.
    """

    _logging_client = None

    def __init__(self, log_name, log_level=logger.INFO):
        self.log_name = log_name
        self._console_logger = logger.getLogger(log_name)
        self._console_logger.setLevel(log_level)

    def _log_console(self, text, severity):
        """
        Display log in console.
        """

        if severity in (LogSeverity.INFO, "INFO"):
            self._console_logger.info(text)
        elif severity in (LogSeverity.ERROR, "ERROR"):
            self._console_logger.error(text)
        elif severity in (LogSeverity.WARNING, "WARNING"):
            self._console_logger.warning(text)
        elif severity in (LogSeverity.DEBUG, "DEBUG"):
            self._console_logger.debug(text)

    @property
    def logging_client(self):
        """
        Builds a client to the logging client API.
        """

        # Create logging client if it is not created yet.
        if self._logging_client is None:
            client = logging.Client()
            self._logging_client = client.logger(self.log_name)

        return self._logging_client

    def _log(self, text, severity, args):
        """
        Log text.
        """

        text = _format_text(text, args)
        self.logging_client.log_text(text, severity=severity)
        self._log_console(text, severity)

    def info(self, text, *args):
        """
        Log information.
        """

        self._log(text, LogSeverity.INFO, args)

    def error(self, text, *args):
        """
        Log error.
        """

        self._log(text, LogSeverity.ERROR, args)

    def warning(self, text, *args):
        """
        Log warning.
        """

        self._log(text, LogSeverity.WARNING, args)

    def warn(self, text, *args):
        """
        Log warning.
        """

        self.warning(text, *args)

    def debug(self, text, *args):
        """
        Log debug.
        """

        self._log(text, LogSeverity.DEBUG, args)

    def list_entries(self):
        """
        List all log entries.
        """

        try:
            for i in self.logging_client.list_entries():
                self._log_console(i.payload, i.severity)
        except NotFound:
            self._console_logger.warning("Log %s not found.", self.log_name)

    def delete(self):
        """
        Delete all log entries.
        """

        self._console_logger.info("Deleted all logging entries for %s", self.log_name)

        try:
            self.logging_client.delete()
        except NotFound:
            self._console_logger.warning("Log %s already deleted or does not exist.", self.log_name)

    def setLevel(self, log_level):
        """
        Setting log level.
        """

        self._console_logger.setLevel(log_level)
