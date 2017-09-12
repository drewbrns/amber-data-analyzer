from pymongo import MongoClient
from bson.json_util import loads
from urllib.parse import urlparse


class MongoDBUtil(object):

    def __init__(self, url):
        self.url = url
        self.client = MongoClient(self.url.geturl())

    def __enter__(self):
        return self

    def aggregate(self):
        db = self.client.traffic_raw
        collection = db.data_store

        documents = collection.aggregate([
            {'$match': {'topic': 'users/trafficbot/trafficdata'}},
            {'$project': {'_id': 0, 'value': '$value'}}
        ])

        return [loads(d['value']) for d in documents]

    def store(self, documents):
        db = self.client.traffic
        collection = db.speed_profile
        collection.insert_many(documents)

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()
