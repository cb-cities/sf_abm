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
folder = 'sf_overpass'

################################################################
### Estabilish relationship between OSM/graph nodes and TAZs ###
################################################################

def find_in_nodes(row, points, nodes_df):
    ### return the indices of points in nodes_df that are contained in row['geometry']
    ### this function is called by TAZ_nodes()
    if row['geometry'].type == 'MultiPolygon':
        return []
    else:
        path = mpltPath.Path(list(zip(*row['geometry'].exterior.coords.xy)))
        in_index = path.contains_points(points)
        return nodes_df['osmid'].loc[in_index].tolist()

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    From: https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
    """

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000

def TAZ_pair_distance(taz_gdf):
    ### Limit TAZ-level OD to those TAZs whose centroids are 2.5km away to avoid walking.
    taz_gdf['lon'] = taz_gdf.geometry.centroid.x
    taz_gdf['lat'] = taz_gdf.geometry.centroid.y
    taz_gdf['lon'] = taz_gdf['lon'].map(radians)
    taz_gdf['lat'] = taz_gdf['lat'].map(radians)

    taz_pair = list(itertools.combinations(taz_gdf['TAZ'], 2))
    taz_pair_df = pd.DataFrame(taz_pair, columns=['TAZ_small', 'TAZ_big'])
    taz_pair_df = pd.merge(taz_pair_df, taz_gdf[['TAZ', 'lon', 'lat']], how='left', left_on='TAZ_small', right_on='TAZ')
    taz_pair_df = pd.merge(taz_pair_df, taz_gdf[['TAZ', 'lon', 'lat']], how='left', left_on='TAZ_big', right_on='TAZ', suffixes=['_1', '_2'])
    taz_pair_df['distance'] = haversine(taz_pair_df['lat_1'], taz_pair_df['lon_1'], taz_pair_df['lat_2'], taz_pair_df['lon_2'])
    taz_pair_df[['TAZ_small', 'TAZ_big', 'distance']].to_csv(absolute_path+'/../output/{}/TAZ_pair_distance.csv'.format(folder), index=False)

def TAZ_nodes():
    ### Find corresponding nodes for each TAZ
    ### Input 1: TAZ polyline
    taz_gdf = gpd.read_file(absolute_path+'/../input/TAZ981/TAZ981.shp')
    taz_gdf = taz_gdf.to_crs({'init': 'epsg:4326'})
    #TAZ_pair_distance(taz_gdf) ### output a distance matrix between each TAZ pairs
    #sys.exit(0)

    ### Input 2: OSM nodes coordinate
    nodes_df = pd.read_csv(absolute_path+'/../../0_network/data/{}/nodes.csv'.format(folder))
    points = nodes_df[['lon', 'lat']].values
    taz_gdf['in_nodes'] = taz_gdf.apply(lambda row: find_in_nodes(row, points, nodes_df), axis=1)
    taz_nodes_dict = {row['TAZ']:row['in_nodes'] for index, row in taz_gdf.iterrows()}
    
    ### [{'taz': 1, 'in_nodes': '[...]''}, ...]
    with open(absolute_path+'/../output/{}/taz_nodes.json'.format(folder), 'w') as outfile:
        json.dump(taz_nodes_dict, outfile, indent=2)


################################################################
######### Sample nodes as OD based on TAZ-level results ########
################################################################


def TAZ_nodes_OD(day, hour, count=50000):

    logger = logging.getLogger('TAZ_nodes_OD')

    logger.debug('************* DAY {}, HOUR {} **************'.format(day, hour))

    ### 0. READING
    taz_travel_df = pd.read_csv(absolute_path+'/../input/TNC_pickups_dropoffs.csv') ### TNC ODs from each TAZ
    taz_scale_df = pd.read_csv(absolute_path+'/../input/TAZ_supervisorial.csv') ### Scaling factors for each TAZ
    taz_pair_dist_df = pd.read_csv(absolute_path+'/../output/{}/taz_pair_distance.csv'.format(folder)) ### Centroid coordinates of each TAZ

    ### 1. FILTERING to time of interest
    ### OD_df: pickups and dropoffs by TAZ from TNC study
    ### [{'taz':1, 'day':1, 'hour':10, 'pickups': 80, 'dropoffs': 100}, ...]
    ### Monday is 0 -- Sunday is 6. Hour is from 3am-26am(2am next day)
    hour_taz_travel_df = taz_travel_df[(taz_travel_df['day_of_week']==day) & (taz_travel_df['hour']==hour)].reset_index()

    ### 2. SCALING
    ### Scale the TNC ODs to the total TAZ ODs using scaling factors that vary spatially (by superdistricts) and temporally (by AM, PM and off-peak)
    if hour in [7, 8, 9]: share_col = 'AMshare'
    elif hour in [17, 18, 19]: share_col = 'PMshare'
    else: share_col = 'OPshare'

    hour_taz_travel_df = pd.merge(hour_taz_travel_df[['taz', 'pickups', 'dropoffs']], taz_scale_df, how='left', left_on='taz', right_on='TAZ')
    hour_taz_travel_df['veh_pickups'] = hour_taz_travel_df.apply(lambda row: row['pickups']/row[share_col], axis=1)
    hour_taz_travel_df['veh_dropoffs'] = hour_taz_travel_df.apply(lambda row: row['dropoffs']/row[share_col], axis=1)
    hour_taz_travel_df = hour_taz_travel_df.dropna(subset=['veh_pickups', 'veh_dropoffs']) ### drop the rows that have NaN total_pickups and total_dropoffs

    O_counts = np.sum(hour_taz_travel_df['veh_pickups'])
    D_counts = np.sum(hour_taz_travel_df['veh_dropoffs'])
    logger.debug('sum total_pickups {}; sum total_dropoffs {}. They may not be equal'.format(O_counts, D_counts))

    ### 3. TAZ-level OD pairs
    ### The probability of trips from each origin (to each destination) TAZ is proporional to the # trip origins (destinations) of that TAZ devided by the total origins (destinations) in all TAZs.
    hour_demand = int(0.5 * (O_counts + D_counts))
    logger.debug('DY{}, HR{}, total number of OD pairs: {}'.format(day, hour, hour_demand))
    O_prob = hour_taz_travel_df['veh_pickups']/O_counts
    D_prob = hour_taz_travel_df['veh_dropoffs']/D_counts
    
    OD_list = [] ### A list holding all the TAZ level OD pairs
    step = 0
    while (len(OD_list) < hour_demand):### While not having generated enough OD pairs
        step_demand = max(10, int(hour_demand*(0.3**step))) ### step 0: step_demand = total_demand; step 1: step_demand = 0.3*total_demand ...
        O_list = np.random.choice(hour_taz_travel_df['TAZ'], step_demand, replace=True, p=O_prob)
        D_list = np.random.choice(hour_taz_travel_df['TAZ'], step_demand, replace=True, p=D_prob)
        step_OD_df = pd.DataFrame({'O': O_list, 'D': D_list})
        step_OD_df = pd.merge(step_OD_df, taz_pair_dist_df, how='left', left_on=['O', 'D'], right_on=['TAZ_small', 'TAZ_big'])
        step_OD_df = pd.merge(step_OD_df, taz_pair_dist_df, how='left', left_on=['D', 'O'], right_on=['TAZ_small', 'TAZ_big'], suffixes=['_1', '_2'])
        step_OD_df = step_OD_df.fillna(value={'distance_1': 0, 'distance_2':0})
        step_OD_df = step_OD_df.loc[step_OD_df['distance_1'] + step_OD_df['distance_2']>2500].reset_index() ### half an hour if walking at 5km/h
        OD_list += list(zip(step_OD_df['O'], step_OD_df['D']))
        logger.debug('step demand {}, length of OD at step {}: {}'.format(step_demand, step, len(OD_list)))
        step += 1
    #sys.exit(0)

    OD_list = OD_list[0:hour_demand]
    OD_counter = Counter(OD_list) ### get dictionary of list item count

    ### 4. Nodal-level OD pairs
    ### Now sample the nodes for each TAZ level OD pair
    taz_nodes_dict = json.load(open(absolute_path+'/../output/{}/taz_nodes.json'.format(folder)))
    #node_osmid2graphid_dict = json.load(open(absolute_path+'/../0_network/data/sf/node_osmid2graphid.json'))
    nodal_OD = []
    for k, v in OD_counter.items():
        taz_O = k[0]
        taz_D = k[1]
        
        ### use the nodes from the next TAZ if there is no nodes in the current TAZ
        ### Scenario 1: TAZ=741. It is in downtown, with many pickups and dropoffs. However, all the nodes are on the boundary (no node in TAZ=741). Then we use the nodes from nearby TAZs.
        if len(taz_nodes_dict[str(taz_O)])==0: taz_O += 1
        if len(taz_nodes_dict[str(taz_D)])==0: taz_D += 1

        try:
            nodal_OD_pairs = random.choices(list(itertools.product(taz_nodes_dict[str(taz_O)], taz_nodes_dict[str(taz_D)])), k=v)
            ### random.choices generate k elements from the population with replacement
        except IndexError:
            #print(taz_O, taz_D) ### 868-872 does not have nodes
            continue
        nodal_OD_counter = Counter(nodal_OD_pairs)

        for nodal_k, nodal_v in nodal_OD_counter.items():
            #nodal_OD.append([node_osmid2graphid_dict[nodal_k[0]], node_osmid2graphid_dict[nodal_k[1]], nodal_v])
            nodal_OD.append([nodal_k[0], nodal_k[1], nodal_v])

    nodal_OD_df = pd.DataFrame(nodal_OD, columns=['O', 'D', 'flow'])
    #print(nodal_OD_df.head())

    nodal_OD_df.to_csv(absolute_path+'/../output/{}/DY{}/SF_OD_DY{}_HR{}.csv'.format(folder, day, day, hour))

    return hour_demand


if __name__ == '__main__':

    logging.basicConfig(filename=absolute_path+'/../output/{}/OD.log'.format(folder), level=logging.DEBUG)
    logger = logging.getLogger('main')
    logger.info('{} \n'.format(datetime.datetime.now()))

    ### Run TAZ_nodes() once to generate 'output/taz_coordinates.csv' and 'output/taz_nodes.csv'
    #TAZ_nodes()
    #sys.exit(0)

    ### Based on the output of TAZ_nodes(), generate hourly node-level ODs by setting "day_of_week" and "hour".
    daily_demand = 0

    for day_of_week in [0, 1, 2, 3, 4, 5, 6]: ### 4 for Friday
        for hour in range(3, 27): ### 24 hour-slices per day. Monday is 0 -- Sunday is 6. Hour is from 3am-26am(2am next day)
            hour_demand = TAZ_nodes_OD(day_of_week, hour)
            daily_demand += hour_demand
        logger.info('DY {} total demand {}'.format(day_of_week, daily_demand))
