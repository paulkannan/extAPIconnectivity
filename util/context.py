from .logger import Logger
from .settings import Settings

class Context(object):
    def __init__(self,prefix):
        self.settings = Settings()
        self.logger = Logger().get_logger(prefix, self.settings.get('GOOGLE_PROJECT_ID_GCS'))