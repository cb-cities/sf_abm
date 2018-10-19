### Based on https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
import json
import sys
import igraph
import numpy as np
from multiprocessing import Pool 
import time 
import os
import logging
import datetime
import warnings
import pandas as pd 
from ctypes import *
import random 
import scipy.io as sio 

absolute_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, absolute_path+'/../')
sys.path.insert(0, '/Users/bz247/')
from sp import interface 

def map_travel_time(origin):
    ### Find shortest path for each unique origin --> one destination
    ### In the future change to multiple destinations
    
    origin_mtx = origin + 1

    results = []
    sp = g.dijkstra(origin_mtx, -1)

    sp_dist = []
    for destin in range(simulation_info['node_count']):
        destin_mtx = destin+1
        sp_dist.append(sp.distance(destin_mtx))

    sp_dist_df = pd.DataFrame({'destin_graphid': list(range(simulation_info['node_count'])), 'destin_dist': sp_dist})
    sp_dist_df = sp_dist_df.loc[sp_dist_df['destin_dist']<10e5]
    sp_dist_df = pd.merge(sp_dist_df, taz_nodes_df, how='left', left_on='destin_graphid', right_on='node_graphid')
    taz_dist_df = sp_dist_df.groupby(['taz', 'movement_id'])['destin_dist'].agg([np.mean, np.std]).reset_index()
    #print(sp_dist_df_grp.head())

    origin_movement_id = int(taz_nodes_df.loc[taz_nodes_df['node_graphid']==origin, 'movement_id'])
    #print(origin_movement_id)
    taz_dist_df = pd.merge(taz_dist_df, benchmark_df[benchmark_df['sourceid']==origin_movement_id].reset_index(), how='left', left_on='movement_id', right_on='dstid')
    print(taz_dist_df.iloc[0])

    return origin, sp_dist


def reduce_edge_flow_pd(L, day, hour, incre_id):
    ### Reduce (count the total traffic flow per edge) with pandas groupby

    logger = logging.getLogger('reduce')
    t0 = time.time()
    flat_L = [edge_pop_tuple for sublist in L for edge_pop_tuple in sublist]
    df_L = pd.DataFrame(flat_L, columns=['start_mtx', 'end_mtx', 'flow'])
    df_L_flow = df_L.groupby(['start_mtx', 'end_mtx']).sum().reset_index()
    t1 = time.time()
    logger.debug('DY{}_HR{} INC {}: reduce find {} edges, {} sec w/ pd.groupby'.format(day, hour, incre_id, df_L_flow.shape[0], t1-t0))

    # print(df_L_flow.head())
    #        start_mtx    end_mtx  flow
    # 0      1      35200    29
    # 1      1      35201    24
    # 2      2      38960     1
    # 3      3      23600     1
    # 4      3      36959     1
    
    #df_L_pop.to_csv('edge_volume_pq.csv')
    
    return df_L_flow

def map_reduce_uber_movements(day, hour):
    ### One time step of ABM simulation
    
    logger = logging.getLogger('map_reduce')

    ### Build a pool
    process_count = 4
    pool = Pool(processes=process_count)

    ### Find shortest pathes
    unique_origin = sample_taz_nodes_df['node_graphid'].tolist()[0:10]
    t_pool_0 = time.time()
    res = pool.imap_unordered(map_travel_time, unique_origin)

    ### Close the pool
    pool.close()
    pool.join()
    t_pool_1 = time.time()

    ### Collapse into edge total population dictionary
    origin_list, dist_list = zip(*res)
    print(len(origin_list), len(dist_list))
    #print(origin_list[0:10])
    sys.exit(0)

    #edge_volume = reduce_edge_flow(edge_flow_tuples, day, hour)
    edge_volume = reduce_edge_flow_pd(edge_flow_tuples, day, hour, incre_id)

    return edge_volume, travel_time_list_incre


def main():

    logging.basicConfig(filename=absolute_path+'/sf_abm_mp_validate.log', level=logging.INFO)
    logger = logging.getLogger('main')
    logger.info('{} \n'.format(datetime.datetime.now()))
    logger.info('validate against Uber Movement data')

    t_main_0 = time.time()

    ### Initialising a global variable to represent the network
    global g
    global simulation_info
    origina_g = sio.mmread(absolute_path+'/../0_network/data/sf/network_sparse.mtx')
    simulation_info = {'node_count': origina_g.shape[0]}

    ### Read in the {TAZ: nodes} dictionary
    global taz_nodes_df
    global sample_taz_nodes_df
    taz_nodes_df = pd.read_csv(absolute_path+'/../4_validation/input/uber_taz_nodes.csv')
    node_osmid2graphid_dict = json.load(open(absolute_path+'/../0_network/data/sf/node_osmid2graphid.json'))
    taz_nodes_df['node_graphid'] = taz_nodes_df.apply(lambda row: node_osmid2graphid_dict[str(row['node_osmid'])], axis=1)
    print(taz_nodes_df.shape)
    print(taz_nodes_df.head())
    sample_taz_nodes_df = taz_nodes_df.sample(frac=1).reset_index(drop=True).groupby('taz').head(1).reset_index(drop=True).sort_values(by='taz').reset_index(drop=True)
    print(sample_taz_nodes_df.shape)
    print(sample_taz_nodes_df.head())

    ### Read in benchmark data
    uber_df = pd.read_csv(absolute_path+'/../4_validation/uber_movement/san_francisco-taz-2016-4-OnlyWeekdays-hourlyAggregate.csv')
    uber_df = uber_df[['sourceid', 'dstid', 'hod', 'mean_travel_time', 'standard_deviation_travel_time']]
    global benchmark_df

    ### Loop through days and hours
    for day in [0]:
        for hour in range(3, 4):
            ### In each hour, there are hundreds of ODSPs (starting from a few nodes per TAZ) to parallel

            logger.info('*************** DY{} HR{} ***************'.format(day, hour))
            t_hour_0 = time.time()

            ### Read in the hourly graph output from ABM
            g = interface.readgraph(bytes(absolute_path+'/output/network_DY{}_HR{}.mtx'.format(day, hour), encoding='utf-8'))
            benchmark_df = uber_df.loc[uber_df['hod']==hour].reset_index()

            map_reduce_uber_movements(day, hour)
            sys.exit(0)

            t_hour_1 = time.time()
            logger.info('DY{}_HR{}: {} sec \n'.format(day, hour, t_hour_1-t_hour_0))

            network_attr_df[['start_mtx', 'end_mtx', 'cum_flow']].to_csv(absolute_path+'/output/edge_flow_DY{}_HR{}.csv'.format(day, hour), index=False)

            with open(absolute_path + '/output/travel_time_DY{}_HR{}.txt'.format(day, hour), 'w') as f:
                for travel_time_item in travel_time_list:
                    f.write("%s\n" % travel_time_item)

            #g.writegraph(bytes(absolute_path+'/output_incre/network_result_DY{}_HR{}.mtx'.format(day, hour), encoding='utf-8'))

    t_main_1 = time.time()
    logger.info('total run time: {} sec \n\n\n\n\n'.format(t_main_1 - t_main_0))

if __name__ == '__main__':
    main()

