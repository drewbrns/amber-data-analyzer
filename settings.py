import os
from urllib.parse import urlparse

MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://mongo:27017/traffic')
OSM_HOST = os.environ.get('OSM_HOST', 'db')
OSM_USER = os.environ.get('OSM_USER', 'postgres')
OSM_DATABASE = os.environ.get('OSM_DATABASE', 'gis')

options = {
    'MONGODB_URI': MONGODB_URI,
    'OSM_HOST': OSM_HOST,
    'OSM_USER': OSM_USER,
    'OSM_DATABASE': OSM_DATABASE
}
