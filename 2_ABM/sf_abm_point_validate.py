### Based on https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
import json
import sys
import igraph
import numpy as np
from multiprocessing import Pool 
import time 
import os
import pandas as pd 
import geopandas as gpd
from ctypes import *
import random 
import scipy.io as sio 

absolute_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, absolute_path+'/../')
sys.path.insert(0, '/Users/bz247/')
from sp import interface 

folder = 'sf_osmnx'
scenario = 'sf_osmnx2'

def sssp_travel_time(row, g, taz_nodes_df, benchmark_df, sio_g_node_count):
    ### Find shortest path for each unique origin --> one destination
    ### In the future change to multiple destinations
    
    print(row)

    origin_mtx = getattr(row, 'node_id') + 1
    sp = g.dijkstra(origin_mtx, -1)
    sp_dist = []
    for destin in range(sio_g_node_count):
        destin_mtx = destin+1
        sp_dist.append(sp.distance(destin_mtx))

    ### group the results by taz -- movement_id
    sp_dist_df = pd.DataFrame({'destin_id': list(range(sio_g_node_count)), 'destin_dist': sp_dist})
    sp_dist_df = sp_dist_df.loc[sp_dist_df['destin_dist']<10e5]
    sp_dist_df = pd.merge(sp_dist_df, taz_nodes_df, how='left', left_on='destin_id', right_on='node_id')
    taz_dist_df = sp_dist_df.groupby(['taz', 'movement_id'])['destin_dist'].agg([np.mean, np.std, 'count']).reset_index()
    print(taz_dist_df.head())
    #sys.exit(0)

    taz_dist_df = pd.merge(taz_dist_df, benchmark_df[benchmark_df['sourceid']==getattr(row, 'movement_id')].reset_index(drop=True), how='left', left_on='movement_id', right_on='dstid')
    #print(taz_dist_df.iloc[108])

    return taz_dist_df

def df2geojson(taz_dist_df, taz_gdf, day, hour, node_osmid):
    output_gdf = taz_gdf.merge(taz_dist_df, how='right', on=['movement_id'])
    output_gdf = output_gdf[['taz', 'mean', 'std', 'count', 'sourceid', 'dstid', 'mean_travel_time', 'standard_deviation_travel_time', 'geometry']]
    output_gdf.columns = ['taz', 'sim_mean', 'sim_std', 'sim_count', 'sourceid', 'dstid', 'uber_mean', 'uber_std', 'geometry']
    output_gdf['time_diff'] = output_gdf['sim_mean'] - output_gdf['uber_mean']
    #output_gdf['var_diff'] = np.sqrt(output_gdf['sim_std']**2/output_gdf['sim_count'] - output_gdf['uber_std']**2/output_gdf['sim_count'])
    # print(output_gdf.iloc[1])
    # sys.exit(0)
    print('output day {}, hour {}, node {}'.format(day, hour, node_osmid))
    output_gdf.to_file(absolute_path+'/../4_validation/output/{}/uber_benchmark_DY{}_HR{}_node{}.json'.format(scenario, day, hour, node_osmid), driver='GeoJSON')

def main():

    t_main_0 = time.time()

    ### The initial graph
    sio_g = sio.mmread(absolute_path+'/../0_network/data/{}/{}/network_sparse.mtx'.format(folder, scenario))
    sio_g_node_count = sio_g.shape[0]

    ### Read in the "taz -- movement_id -- node_osmid" table
    ### Convert to "taz -- movement_id -- node_osmid -- node_graphid" table
    taz_nodes_df = pd.read_csv(absolute_path+'/../4_validation/input/{}/uber_taz_nodes.csv'.format(folder))
    nodes_df = pd.read_csv(absolute_path+'/../0_network/data/{}/sf_simplified_nodes.csv'.format(folder))
    nodes_df['node_id'] = range(nodes_df.shape[0])
    taz_nodes_df = pd.merge(taz_nodes_df, nodes_df[['node_id', 'osmid']], how='left', left_on='node_osmid', right_on='osmid')
    print(taz_nodes_df.shape)
    print(taz_nodes_df.head())

    ### Table of the various ids of some sample nodes
    sample_nodes_osmid = [65289916, 65349387, 65346569, 1580501214]
    sample_taz_nodes_df = taz_nodes_df.loc[taz_nodes_df['node_osmid'].isin(sample_nodes_osmid)]
    sample_taz_nodes_df = sample_taz_nodes_df[['taz', 'movement_id', 'node_osmid', 'node_id']]
    print(sample_taz_nodes_df.shape)
    print(sample_taz_nodes_df.head())

    ### Read in benchmark data
    # uber_df = pd.read_csv(absolute_path+'/../4_validation/uber_movement/san_francisco-taz-2016-4-OnlyWeekdays-hourlyAggregate.csv')
    # uber_df = uber_df[['sourceid', 'dstid', 'hod', 'mean_travel_time', 'standard_deviation_travel_time']]
    # sample_uber_df = uber_df.loc[uber_df['sourceid'].isin(sample_taz_nodes_df['movement_id'])]
    # sample_uber_df.to_csv(absolute_path+'/../4_validation/uber_movement//{}sample_sf-taz-2016-4-OnlyWeekdays-hourlyAggregate.csv'.format(folder), index=False)
    # sys.exit(0)
    sample_uber_df = pd.read_csv(absolute_path+'/../4_validation/uber_movement/{}/sample_sf-taz-2016-4-OnlyWeekdays-hourlyAggregate.csv'.format(folder))
    print(sample_uber_df.shape, np.unique(sample_uber_df['sourceid']))

    ### TAZ spatial
    taz_gdf = gpd.read_file(absolute_path+'/../4_validation/uber_movement/san_francisco_taz.json')
    taz_gdf = taz_gdf.to_crs({'init': 'epsg:4326'})
    taz_gdf = taz_gdf[['TAZ', 'MOVEMENT_ID', 'geometry']]
    taz_gdf['movement_id'] = taz_gdf['MOVEMENT_ID'].astype(int)

    day = 0
    hour = 8
    g = interface.readgraph(bytes(absolute_path+'/output/{}/network_DY{}_HR{}.mtx'.format(scenario, day, hour), encoding='utf-8'))
    benchmark_df = sample_uber_df.loc[sample_uber_df['hod']==hour].reset_index()
    for row in sample_taz_nodes_df.itertuples():
        print(getattr(row, 'node_id'))
        taz_dist_df = sssp_travel_time(row, g, taz_nodes_df, benchmark_df, sio_g_node_count)
        df2geojson(taz_dist_df, taz_gdf, day, hour, getattr(row, 'node_osmid'))
    sys.exit(0)

if __name__ == '__main__':
    main()

