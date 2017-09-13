import pickle
import pathlib
from pymongo import MongoClient, DESCENDING
from bson.json_util import loads
from urllib.parse import urlparse
import time


class MongoDBUtil(object):

    def __init__(self, url):
        self.url = url
        self.client = MongoClient(self.url.geturl())
        self.mark = None

    def __enter__(self):
        return self

    def aggregate(self):
        db = self.client.traffic
        collection = db.raw

        cursor = collection.find({}).sort('timestamp', DESCENDING)
        for entry in cursor:
            self.mark = entry
            break

        return [loads(doc['value']) for doc in cursor]

    def store(self, documents):
        db = self.client.traffic
        collection = db.speed_profile
        collection.insert_many(documents)

    def _cleanup(self):
        db = self.client.traffic
        collection = db.raw
        collection.delete_many({
            '_id': {'$lte': self.mark['_id']}
        })

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()
        self.client.close()
