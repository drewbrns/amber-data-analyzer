import pandas as pd 
import numpy as np
import time
import json

from utils.utils import convert_to_date, convert_to_hour, convert_to_minute


class SpeedProfiler(object):

    def __init__(self, data):
        self.dataframe = pd.DataFrame(data)

    def _convert_times(self):
        """ Private method. Convert epoch time to date, hourly interval & minute"""
        self.dataframe['date'] = self.dataframe['timestamp'].apply(convert_to_date)
        self.dataframe['hour'] = self.dataframe['timestamp'].apply(convert_to_hour)
        self.dataframe['minute'] = self.dataframe['timestamp'].apply(convert_to_minute)

    def start(self):
        """ Start speed profiler """
        #To do:
        # Optimize this code! I suspect this code would have a lot of performance issues!
        # Because of the volumes of data involved.
        self._convert_times()
        sp = self.dataframe['speed'].groupby([
            self.dataframe['road'],
            self.dataframe['date'],
            self.dataframe['hour'],
            self.dataframe['minute']
        ]).mean()
        sp = sp.to_frame().reset_index()
        sp = sp.to_json(orient='records')
        return json.loads(sp)


class SpeedFileGenerator(object):

    def __init__(self, data):        
        self.dataframe = pd.DataFrame(data)

    def make_speed_file(self):
        """ Speed File Generator """
        #To do:
        # Optimize this code! I suspect this code would have a lot of performance issues!
        # Because of the volumes of data involved.
        
        # Remove un needed fields
        del self.dataframe['date']
        del self.dataframe['hour']

        # Convert minute and speed columns to int for ease of use. Replace all speed of 0s to 1s
        # Zeros have special meanings in routing graphs.         
        self.dataframe['minute'] = self.dataframe['minute'].astype('int')
        self.dataframe['speed'] = self.dataframe['speed'].astype('uint8')        
        self.dataframe['speed'] = self.dataframe['speed'].replace(0, 1)

        group_by_road = self.dataframe.groupby(['road'])
        group_by_min  = group_by_road['minute']
        max_mins      = group_by_min.max()
        way_speeds = self.dataframe.loc[self.dataframe['minute'].isin(max_mins.values)].copy()
        way_speeds['road'] = way_speeds['road'].astype('int32')
        del way_speeds['minute']
        
        # Convert dataframe to speed file format needed by walhalla
        way_speeds.rename(columns={'road':'wayid', 'speed':'forward'}, inplace=True)
        way_speeds['reverse'] = np.zeros(len(way_speeds), dtype=np.uint8)

        return way_speeds
    