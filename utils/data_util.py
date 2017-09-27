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

        return [loads(doc['value'], strict=False) for doc in cursor]
        # return [
        #    loads(doc['value'], strict=False) if type(doc) is not dict else doc['value'] for doc in cursor
        # ]

    def store(self, documents, col='speed_profile'):
        if (len(documents) > 0):
            db = self.client.traffic
            collection = db[col]
            collection.insert_many(documents)

    def _cleanup(self):
        if self.mark is not None:
            db = self.client.traffic
            collection = db.raw
            collection.delete_many({
                '_id': {'$lte': self.mark['_id']}
            })

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()
        self.client.close()
