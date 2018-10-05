import pandas as pd 
import geopandas as gpd 
import json 
import sys 
import matplotlib.path as mpltPath
import numpy as np 
import scipy.sparse 
from collections import Counter
import random 
import itertools 
import os 
import logging
import datetime

absolute_path = os.path.dirname(os.path.abspath(__file__))

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
        return nodes_df['index'].loc[in_index].tolist()

def TAZ_nodes():
    ### Find corresponding nodes for each TAZ
    ### Input 1: TAZ polyline
    taz_gdf = gpd.read_file(absolute_path+'/TAZ981/TAZ981.shp')
    taz_gdf = taz_gdf.to_crs({'init': 'epsg:4326'})

    ### Input 2: OSM nodes coordinate
    nodes_dict = json.load(open(absolute_path+'/../0_network/data/sf/nodes.json'))
    nodes_df = pd.DataFrame.from_dict(nodes_dict, orient='index', columns=['lat', 'lon']).reset_index()

    points = nodes_df[['lon', 'lat']].values
    taz_gdf['in_nodes'] = taz_gdf.apply(lambda row: find_in_nodes(row, points, nodes_df), axis=1)
    taz_nodes_dict = {row['TAZ']:row['in_nodes'] for index, row in taz_gdf.iterrows()}
    
    ### [{'taz': 1, 'in_nodes': '[...]''}, ...]
    with open(absolute_path+'/output/taz_nodes.json', 'w') as outfile:
        json.dump(taz_nodes_dict, outfile, indent=2)


################################################################
######### Sample nodes as OD based on TAZ-level results ########
################################################################

def OD_iterations(OD, target_O, target_D):
    ### one iteration of finding matrix elements based on row sums and column sums

    #print(np.min(target_D), np.max(target_D))
    col_sum = OD.sum(axis=0)
    D_coef = target_D/(col_sum+0.001)
    OD = OD*D_coef
    #print(np.max(col_sum), np.min(D_coef), np.max(D_coef), np.min(OD), np.max(OD))

    row_sum = OD.sum(axis=1)
    O_coef = target_O/(row_sum+0.001)
    OD = (OD.T*O_coef).T

    col_sum = OD.sum(axis=0)
    errors = np.sum(np.abs(target_D - col_sum))

    #print(np.count_nonzero(np.isnan(errors)))

    return OD, errors


def TAZ_nodes_OD(day, hour, count=50000):

    print('************* DAY {}, HOUR {} **************'.format(day, hour))

    ### 1. FILTERING
    ### Input 1: pickups and dropoffs by TAZ from TNC study
    ### [{'taz':1, 'day':1, 'hour':10, 'pickups': 80, 'dropoffs': 100}, ...]
    ### Monday is 0 -- Sunday is 6. Hour is from 3am-26am(2am next day)
    OD_df = pd.read_csv(absolute_path+'/TNC_pickups_dropoffs.csv')
    hour_OD_df = OD_df[(OD_df['day_of_week']==day) & (OD_df['hour']==hour)].reset_index()

    ### 2. SCALING
    ### Get total travel demand
    if hour in [7, 8, 9]: 
        share_col = 'AMshare'
    elif hour in [17, 18, 19]: 
        share_col = 'PMshare'
    else: 
        share_col = 'OPshare'

    OD_scale_df = pd.read_csv(absolute_path+'/TAZ981/TAZ_supervisorial.csv')
    hour_OD_df = pd.merge(hour_OD_df[['taz', 'pickups', 'dropoffs']], OD_scale_df, how='left', left_on='taz', right_on='TAZ')
    hour_OD_df['veh_pickups'] = hour_OD_df.apply(lambda row: row['pickups']/row[share_col], axis=1)
    hour_OD_df['veh_dropoffs'] = hour_OD_df.apply(lambda row: row['dropoffs']/row[share_col], axis=1)
    hour_OD_df = hour_OD_df.dropna(subset=['veh_pickups', 'veh_dropoffs']) ### drop the rows that have NaN total_pickups and total_dropoffs

    total_pickup_counts = np.sum(hour_OD_df['veh_pickups'])
    total_dropoff_counts = np.sum(hour_OD_df['veh_dropoffs'])

    print('sum total_pickups {} sum total_dropoffs {}'.format(total_pickup_counts, total_dropoff_counts))

    ### 3. TAZ-level OD pairs
    ### OD_matrix element represent the probability (float). Need to sample pairs based on the probability

    total_demand = int(0.5 * (total_pickup_counts + total_dropoff_counts))
    print('total number of OD pairs: ', total_demand)

    #return total_pickup_counts, total_dropoff_counts

    O_prob = hour_OD_df['veh_pickups']/total_pickup_counts
    D_prob = hour_OD_df['veh_dropoffs']/total_dropoff_counts
    OD_list = []
    step = 0

    while (len(OD_list) < total_demand) & (step < 10):
        step_demand = max(10, int(total_demand*(0.1**step))) ### step 0: step_demand = total_demand; step 1: step_demand = 0.1*total_demand ...
        O_list = np.random.choice(hour_OD_df['TAZ'], step_demand, replace=True, p=O_prob)
        D_list = np.random.choice(hour_OD_df['TAZ'], step_demand, replace=True, p=D_prob)
        step_OD_list = zip(O_list, D_list)
        step_OD_list = [OD for OD in step_OD_list if OD[0]!=OD[1]]
        OD_list += step_OD_list
        print('length of OD at step {}: {}'.format(step, len(OD_list)))
        step += 1

    OD_list = OD_list[0:total_demand]
    OD_counter = Counter(OD_list) ### get dictionary of list item count

    ### 4. Nodal-level OD pairs
    ### Now sample the nodes for each TAZ level OD pair
    taz_nodes_dict = json.load(open(absolute_path+'/output/taz_nodes.json'))
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
        except IndexError:
            #print(taz_O, taz_D) ### 868-872 does not have nodes
            continue
        nodal_OD_counter = Counter(nodal_OD_pairs)

        for nodal_k, nodal_v in nodal_OD_counter.items():
            #nodal_OD.append([node_osmid2graphid_dict[nodal_k[0]], node_osmid2graphid_dict[nodal_k[1]], nodal_v])
            nodal_OD.append([nodal_k[0], nodal_k[1], nodal_v])

    nodal_OD_df = pd.DataFrame(nodal_OD, columns=['O', 'D', 'flow'])
    #print(nodal_OD_df.head())

    nodal_OD_df.to_csv(absolute_path+'/output_scaled/DY{}/SF_OD_DY{}_HR{}.csv'.format(day, day, hour))

    return total_pickup_counts, total_dropoff_counts


if __name__ == '__main__':

    logging.basicConfig(filename=absolute_path+'/OS2csv_supervisorial.log', level=logging.DEBUG)
    logger = logging.getLogger('main')
    logger.info('{} \n'.format(datetime.datetime.now()))

    #TAZ_nodes()
    #sys.exit(0)

    daily_origins = 0
    daily_destins = 0

    for day_of_week in [0]: ### 4 for Friday
        for hour in range(3, 27): ### 24 hour-slices per day
            ### Monday is 0 -- Sunday is 6. Hour is from 3am-26am(2am next day)
            hourly_origns, hourly_destins = TAZ_nodes_OD(day_of_week, hour)
            daily_origins += hourly_origns
            daily_destins += hourly_destins
        logger.info('DY {} daily_origins {}, daily_destins {}'.format(day_of_week, np.sum(daily_origins), np.sum(daily_destins)))
