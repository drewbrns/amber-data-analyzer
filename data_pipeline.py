""" """
import time
import logging

from settings import MONGODB_URL
from settings import OSM_DATABASE_URL

from utils.nearest_road_util import NearestRoad
from utils.data_util import MongoDBUtil
from utils.congestion_util import SpeedProfiler


class DataPineline():

    """ DataPineline class defines the processes each traffic datapoint collected goes 
        through until it can produce traffic congestion.
    """

    def __init__(self):
        pass

    def start(self):
        """ Begin data pipeline process """

        nearest_road_util = NearestRoad(data=OSM_DATABASE_URL)

        with MongoDBUtil(url=MONGODB_URL) as mongodb_util:
            print("start time: ", time.time())
            datapoints = mongodb_util.aggregate()
            datapoints = list(map(nearest_road_util.match, datapoints))
            speed_profiler = SpeedProfiler(datapoints)
            results = speed_profiler.start()
            mongodb_util.store(results)
            print("endtime: ", time.time())


    def stop(self):
        """ Stop data pipeline process """
        return



if __name__ == '__main__':
    pipeline = DataPineline()
    pipeline.start()

    # transformed_datapoints = map(nearest_road.match, datapoints)
    # mongodb_util.insert_processed_data(transformed_datapoints)
