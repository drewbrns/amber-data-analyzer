import pandas as pd 
import numpy as np
import time


class WaySpeeds(object):
    
    def __init__(self, data):        
        self.dataframe = pd.DataFrame(data)

    def generate(self):
        """ Speed File Generator """

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
        way_speeds    = self.dataframe.loc[self.dataframe['minute'].isin(max_mins.values)].copy()
        way_speeds['road'] = way_speeds['road'].astype('uint64')
        del way_speeds['minute']

        # Convert dataframe to speed file format needed by walhalla
        way_speeds.rename(columns={'road':'wayid', 'speed':'forward'}, inplace=True)
        way_speeds['reverse'] = np.zeros(len(way_speeds), dtype=np.uint8)

        return way_speeds
    