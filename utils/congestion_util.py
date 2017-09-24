import pandas as pd 
import time
import json


# Utility functions
def convert_to_datetime(x):
    if len("{}".format(x)) > 10:
        x = x / 1000.0    
    return time.strftime("%d-%m-%Y %H:%M:%S %p", time.localtime(x))

def convert_to_date(x):
    if len("{}".format(x)) > 10:
        x = x / 1000.0
    return time.strftime("%Y-%m-%d", time.localtime(x))

def convert_to_hour(x):
    if len("{}".format(x)) > 10:
        x = x / 1000.0

    start = time.localtime(x)
    end = time.localtime(x + 60*60)
    return "{} - {}".format(
        time.strftime("%I:00%p", start),
        time.strftime("%I:00%p", end)
    )

def convert_to_day(x):
    return time.strftime("%a", time.localtime(x))


class SpeedProfiler(object):

    def __init__(self, data):
        self.dataframe = pd.DataFrame(data)

    def _convert_times(self):
        """ Private method. Convert epoch time to date & hourly interval"""
        self.dataframe['date'] = self.dataframe['timestamp'].apply(convert_to_date)
        self.dataframe['hour'] = self.dataframe['timestamp'].apply(convert_to_hour)

    def start(self):
        """ Start speed profiler """
        #To do:
        # Optimize this code! I suspect this code would have a lot of performance issues!
        # Because of the volumes of data involved.
        self._convert_times()
        sp = self.dataframe['speed'].groupby([
            self.dataframe['road'],
            self.dataframe['date'],
            self.dataframe['hour']
        ]).mean()
        sp = sp.to_frame().reset_index()
        sp = sp.to_json(orient='records')
        return json.loads(sp)
    