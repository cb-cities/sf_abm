### CHECK FACTS: https://www.epa.gov/greenvehicles/greenhouse-gas-emissions-typical-passenger-vehicle
### FUEL ECONOMY: 22 miles per gallon
### 8,887 grams CO2/ gallon
### 404 grams per mile per car
import json
import sys
import numpy as np
import scipy.sparse 
import scipy.io as sio 
import time 
import os
import logging
import datetime
import warnings
import pandas as pd 
import sf_abm
#import matplotlib.pyplot as plt

pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))
folder = 'sf_overpass'
scenario = 'original'


def eco_incentivize_analysis(budget):

    day = 4
    random_seed = 0
    probe_ratio = 0.01
    for year in range(10):
        aad_vht = 0
        for hour in range(3, 5):
            hour_volume_df = pd.read_csv(absolute_path+'/output/edges_df_abm/edges_df_b{}_y{}_DY{}_HR{}_r{}_p{}.csv'.format(budget, year, day, hour, random_seed, probe_ratio))
            hour_volume_df['net_vol'] = hour_volume_df['hour_flow'] - hour_volume_df['carryover_flow']
            hour_volume_df['net_vht'] = hour_volume_df['net_vol'] * hour_volume_df['t_avg']/3600
            aad_vht += np.sum(hour_volume_df['net_vht'])
        print('aad_vht {}'.format(aad_vht))

if __name__ == '__main__':
    budget = 500
    eco_incentivize_analysis(budget)

