import time
from settings import options
from utils.mongodb_util import MongoDBUtil

from .nearest_road import NearestRoad
from .way_speeds import WaySpeeds
from .speed_profiler import SpeedProfiler

def _match_nearest_roads(datapoints):
    if isinstance(datapoints, list) and len(datapoints) > 0:    
        nr = NearestRoad(
            host=options['OSM_HOST'],
            user=options['OSM_USER'],          
            dbname=options['OSM_DATABASE']
        )
        return list(map(nr.match, datapoints))
    else:
        raise Exception('Expected input to be of list type or not empty')
        

def _generate_speed_profile(datapoints):
    if isinstance(datapoints, list) and len(datapoints) > 0:
        sp = SpeedProfiler(datapoints)
        return sp.generate()
    else:
        raise Exception('Expected input to be of list type or not empty')


def listen():
    
    try:
        mongodb_util = MongoDBUtil(options['MONGODB_URI'])
        
        # Grab the data
        datapoints = mongodb_util.aggregate()

        # Match Road to given coordinates
        datapoints = _match_nearest_roads(datapoints)

        # Generate Speed Profiles
        results = _generate_speed_profile(datapoints)

        #store results
        mongodb_util.store(results, col='speed_profile')
        mongodb_util.store(datapoints, col='heatmap')

    except Exception as e:
        print( e )

    time.sleep(60 * 5) # Sleep for 5 minutes and restart the process
    listen()
