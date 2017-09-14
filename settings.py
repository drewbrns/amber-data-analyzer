import os
import yaml
from urllib.parse import urlparse

BASE_DIR = lambda *x: os.path.join(
    os.path.dirname(os.path.dirname(__file__)), *x)


# load the application configuration file
APP_CONFIG_FILE = BASE_DIR('data_pipeline', 'app_config.yaml')

try:
    APP_CONFIG = yaml.load(open(APP_CONFIG_FILE, 'r'))['APP']
    MONGODB_URL = urlparse(APP_CONFIG.get('MONGODB_URL'))
    OSM_DATABASE_URL = urlparse(APP_CONFIG.get('OSM_DATABASE_URL'))
except IOError:
    raise RuntimeError(
        """
            Configuration file missing.
            To create one, make a copy of `app_config.sample.yaml` and rename it to app_config.yaml.
        """
    )
