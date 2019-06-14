### Scale the TNC demand by supervisorial shares

import pandas as pd 
import geopandas as gpd 
import json 
import sys 
import matplotlib.path as mpltPath
import numpy as np 
from collections import Counter
import random 
import itertools 
import os 
import logging
import datetime
from math import radians

absolute_path = os.path.dirname(os.path.abspath(__file__))
np.random.seed(0)
random.seed(0)

def main(year = 1):
    ### 0: Golden Gate Bridge daily traffic per direction
    ### 1: Bay Bridge
    ### 2: Highway No. 1
    ### 3: SFO
    gate_id = [0, 1, 2, 3]
    gate_traffic = [55000, 180000, 100000, 70000]
    start_node = ['65282392', '65310547', '65284849', '252992403']
    end_node = ['295487932', '645559609', '65286997', '252992433']
    ### Growth rate obtained from ConnectSF trip pattern data analysis.
    ### E.g., east bay pick up growth rate = ((all trips starting from east bay in 2050, excluding those also end in east bay) - (all trips starting from east bay in 2015, excluding those also end in east bay))**(1/35)
    growth_rate = [1.00525, 1.00569, 1.00644, 1.00644]
    gate_traffic_proj = [gate_traffic[i]*growth_rate[i]**year for i in range(len(gate_traffic))]

    ### Trip distribution
    ### The probability of trips from each origin (to each destination) is proporional to the # trip origins (destinations) of that node devided by the total origins (destinations) in all TAZs.
    daily_demand = sum(gate_traffic_proj)
    OD_prob = [traffic/daily_demand for traffic in gate_traffic_proj]
    ### Hourly traffic distribution is based on the intercity (Uber/Lyft) hourly distributions
    hourly_traffic = {3:0.00423, 4:0.00509, 5:0.00928, ### HR3-5
        6:0.01799, 7:0.04513, 8:0.05846, 9:0.05400, 10:0.03345, ### HR6-10
        11:0.03114, 12:0.03250, 13:0.03250, 14:0.03364, 15:0.03811, ### HR11-15
        16:0.04468, 17:0.07687, 18:0.09450, 19:0.09957, 20:0.06338, ### HR16-20
        21:0.05934, 22:0.05477, 23:0.04415, 24:0.03158, 25:0.02263, 26:0.01294} ### HR21-26
    
    for hour in range(3, 27):
        hourly_demand = int(daily_demand*hourly_traffic[hour])
        hourly_OD_list = [] ### A list holding all the OD pairs
        step = 0
        while (len(hourly_OD_list) < hourly_demand):### While not having generated enough OD pairs
            #print(hourly_demand, len(hourly_OD_list))
            step_demand = int(1.3*(hourly_demand-len(hourly_OD_list)))
            O_list = np.random.choice(gate_id, step_demand, replace=True, p=OD_prob)
            D_list = np.random.choice(gate_id, step_demand, replace=True, p=OD_prob)
            step_OD_list = list(zip(O_list, D_list))
            step_OD_list = [pair for pair in step_OD_list if pair[0]!=pair[1]]
            hourly_OD_list += step_OD_list
            step += 1

        hourly_OD_list = hourly_OD_list[0:hourly_demand]
        hourly_OD_osmid_list = [(start_node[pair[0]], end_node[pair[1]]) for pair in hourly_OD_list]
        hourly_OD_osmid_df = pd.DataFrame(hourly_OD_osmid_list, columns=['O', 'D'])
        print(year, hour, hourly_OD_osmid_df.shape)
        hourly_OD_osmid_df.to_csv(absolute_path+'/../output/OD_tables_growth/intercity/intercity_YR{}_HR{}.csv'.format(year, hour))

def check():

    intercity_OD_df = pd.read_csv(absolute_path+'/../output/{}/{}/intercity/intercity_HR{}.csv'.format(folder, scenario, 3))
    for hour in range(4, 27):
        hourly_OD_df = pd.read_csv(absolute_path+'/../output/{}/{}/intercity/intercity_HR{}.csv'.format(folder, scenario, hour))
        intercity_OD_df = pd.concat([intercity_OD_df, hourly_OD_df], ignore_index=True)
    intercity_OD_df_grp = intercity_OD_df.groupby(['O', 'D']).size()
    print(intercity_OD_df_grp)


if __name__ == '__main__':
    for year in range(1, 11):
        main(year=year)
    #check()

