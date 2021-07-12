import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

log_name = 'name-of-log'
appname = 'name-of-app'

class AppFilter(logging.Filter):
    def filter(self, record):
        record.app_name = appname
        return True

class Logger(object):
    def getLogger(self, prefix, project):
        global appname
        appname = prefix
        client = google.cloud.logging.Client(project=project)
        handler = CloudLoggingHandler(client, name=log_name)
        cloud_logger = logging.getLogger('cloudLogger')

        formatter = logging.Formatter('%(app_name)s : %(message)s')
        handler.setFormatter(formatter)

        cloud_logger.addFilter(AppFilter())
        cloud_logger.addHandler(handler)
        cloud_logger.setLevel(logging.INFO)
        return cloud_logger