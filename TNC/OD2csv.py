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

def find_in_nodes(row, points, nodes_df):
    if row['geometry'].type == 'MultiPolygon':
        return []
    else:
        path = mpltPath.Path(list(zip(*row['geometry'].exterior.coords.xy)))
        in_index = path.contains_points(points)
        return nodes_df['index'].loc[in_index].tolist()

def TAZ_nodes():
    ### Find corresponding nodes for each TAZ
    ### Input 1: TAZ polyline
    taz_gdf = gpd.read_file('TAZ981/TAZ981.shp')
    taz_gdf = taz_gdf.to_crs({'init': 'epsg:4326'})

    ### Input 2: OSM nodes coordinate
    nodes_dict = json.load(open('tagged_alloneway_nodes.json'))
    nodes_df = pd.DataFrame.from_dict(nodes_dict, orient='index', columns=['lat', 'lon']).reset_index()

    points = nodes_df[['lon', 'lat']].values
    taz_gdf['in_nodes'] = taz_gdf.apply(lambda row: find_in_nodes(row, points, nodes_df), axis=1)
    taz_nodes_dict = {row['TAZ']:row['in_nodes'] for index, row in taz_gdf.iterrows()}
    
    ### [{'taz': 1, 'in_nodes': '[...]''}, ...]
    with open('taz_nodes.json', 'w') as outfile:
        json.dump(taz_nodes_dict, outfile, indent=2)

def OD_iterations(OD, target_O, target_D):

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


def TAZ_nodes_OD(day, hour, count):

    ################## FILTERING ###################
    ### Input 1: pickups and dropoffs by TAZ
    ### [{'taz':1, 'day':1, 'hour':10, 'pickups': 80, 'dropoffs': 100}, ...]
    ### Monday is 0 -- Sunday is 6. Hour is from 3am-26am(2am next day)
    OD_df = pd.read_csv('taz_OD.csv')
    hour_OD_df = OD_df[(OD_df['day_of_week']==day) & (OD_df['hour']==hour)]

    ################## BALANCING ###################
    ### Get row sums and column sums
    target_O = hour_OD_df.sort_values(by=['taz'])['pickups']
    target_D = hour_OD_df.sort_values(by=['taz'])['dropoffs']
    print('sum of target_O, target_D', sum(target_O), sum(target_D))

    ### Get OD matrix elements constrained by row sums and column sums
    OD_matrix = np.ones((len(target_O), len(target_D)))
    errors_list = []
    for i in range(20):
        OD_matrix, errors = OD_iterations(OD_matrix, np.array(target_O), np.array(target_D))
        errors_list.append(errors)
        print('errors at iteration {}: {}'.format(i, errors))
    print('sum of OD matrix elements', np.sum(OD_matrix), 'max', np.max(OD_matrix), 'min', np.min(OD_matrix), 'trace', np.trace(OD_matrix))
    ### As we are going to ignore inter-TAZ trips (assuming they are not by car), the trace of the OD matrix should not be too big compared to the total sum of the matrix elements
    # print(target_O)
    # np.savetxt('tmp2.txt', OD_matrix, fmt='%.1f', newline='\n')
    # sys.exit(0)

    ################## TAZ-level OD pairs ###################
    ### OD_matrix element represent the probability (float). Need to sample pairs based on the probability
    OD_keys = list(range(len(target_O)*len(target_D)))   ### O = value//len(target_O), D = value % len(target_O)
    np.fill_diagonal(OD_matrix, 0) ### Set inter-TAZ trip probability to 0
    OD_probs = OD_matrix.flatten()
    print('sum of OD_prob elements', np.sum(OD_probs), 'max', np.max(OD_probs), 'min', np.min(OD_probs))
    OD_probs /= np.sum(OD_probs)
    print('sum of OD matrix elements', np.sum(OD_probs), 'max', np.max(OD_probs), 'min', np.min(OD_probs))
    OD_list = np.random.choice(OD_keys, count, replace=True, p=OD_probs)
    OD_counter = Counter(OD_list) ### get dictionary of list item count

    ################## Nodal-level OD pairs ###################
    ### Now sample the nodes for each TAZ level OD pair
    taz_nodes_dict = json.load(open('taz_nodes.json'))
    node_osmid2graphid_dict = json.load(open('node_osmid2graphid.json'))
    nodal_OD = []
    for k, v in OD_counter.items():
        taz_O = k//len(target_O)+1 ### TAZ index starts from 1
        taz_D = k%len(target_O)+1
        
        ### use the nodes from the next TAZ if there is no nodes in the current TAZ
        ### Scenario 1: TAZ=741. It is in downtown, with many pickups and dropoffs. However, all the nodes are on the boundary (no node in TAZ=741). Then we use the nodes from nearby TAZs.
        ### Scenario 2: TAZ=384, 385. There is no nodes in these TAZs because they are small islands. There should be no OD pairs sampled from these TAZs either.
        if len(taz_nodes_dict[str(taz_O)])==0: taz_O += 1
        if len(taz_nodes_dict[str(taz_D)])==0: taz_D += 1

        nodal_OD_pairs = random.choices(list(itertools.product(taz_nodes_dict[str(taz_O)], taz_nodes_dict[str(taz_D)])), k=v)
        nodal_OD_counter = Counter(nodal_OD_pairs)

        for nodal_k, nodal_v in nodal_OD_counter.items():
            nodal_OD.append([node_osmid2graphid_dict[nodal_k[0]], node_osmid2graphid_dict[nodal_k[1]], nodal_v])


    nodal_OD_df = pd.DataFrame(nodal_OD, columns=['O', 'D', 'flow'])
    print(nodal_OD_df.head())

    nodal_OD_df.to_csv('OD_csv/SF_graph_DY{}_HR{}_OD_{}.csv'.format(day, hour, count))


if __name__ == '__main__':
    #taz_nodes_df = TAZ_nodes()
    #sys.exit(0)

    for day_of_week in [1]: ### Two typical days, 1 for Tuesday (weekday) and 6 for Sunday (weekend)
        for hour in range(9,10):#[3]: ### 24 hour-slices per day
            ### Monday is 0 -- Sunday is 6. Hour is from 3am-26am(2am next day)
            TAZ_nodes_OD(day_of_week, hour, 50000)