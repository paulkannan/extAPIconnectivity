import yaml


class Settings(object):
    def __init__(self):
        self.data = self._load_config()

    def get(self, key: str):
        if key not in self.data:
            raise Exception("{} not found. Pls add to config/config.yml".format(key))
        return self.data[key]

    def safe_get(self, key: str):
        if key in self.data:
            return self.data[key]
        else:
            return None

    def _load_config(self, file: str="config/config.yml"):
        with open(file, 'r') as f:
            data = yaml.safe_load(f)
        return data