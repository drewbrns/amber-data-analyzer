""" """
import time
# import logging

from settings import MONGODB_URL
from settings import OSM_DATABASE_URL
from settings import TRAFFIC_CSV_OUTPUT

from utils.utils import convert_to_date, convert_to_hour
from utils.nearest_road_util import NearestRoad
from utils.data_util import MongoDBUtil
from utils.congestion_util import SpeedProfiler, SpeedFileGenerator


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
            # Generate and store new speed profiles.
            datapoints = mongodb_util.aggregate()
            if isinstance(datapoints, list) and len(datapoints) > 0:
                datapoints = list(map(nearest_road_util.match, datapoints))
                speed_profiler = SpeedProfiler(datapoints)
                results = speed_profiler.start()
                mongodb_util.store(results)
                mongodb_util.store(datapoints, col='heatmap')


            # Generate Speed file needed for routing.
            t = time.time()
            date = convert_to_date(t)
            hour = convert_to_hour(t)

            speed_profiles = mongodb_util.fetch(
                'speed_profile', 
                {'date':'{}'.format(date), 'hour': '{}'.format(hour)}
            )     
            generator = SpeedFileGenerator(speed_profiles)
            way_speeds = generator.make_speed_file()
            way_speeds.to_csv(TRAFFIC_CSV_OUTPUT, index=False)


    def stop(self):
        """ Stop data pipeline process """
        return


if __name__ == '__main__':
    pipeline = DataPineline()
    pipeline.start()
