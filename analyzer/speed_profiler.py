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

    def generate(self):
        """ Generate speed profiles """
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
