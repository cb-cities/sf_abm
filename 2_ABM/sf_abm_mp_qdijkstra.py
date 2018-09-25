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

sys.path.insert(0, '/Users/bz247')
from sp import interface 

absolute_path = os.path.dirname(os.path.abspath(__file__))

def map_edge_flow(row):
    ### Find shortest path for each unique origin --> one destination
    ### In the future change to multiple destinations
    
    origin_ID = int(OD['O'].iloc[row]) + 1 ### origin's ID to graph.mtx node ID
    destin_ID = int(OD['D'].iloc[row]) + 1 ### destination's ID to graph.mtx node ID
    traffic_flow = int(OD['flow'].iloc[row]) ### number of travellers with this OD

    results = []
    sp = g.dijkstra(origin_ID, destin_ID)
    if sp.distance(destin_ID) > 10e7:
        return [], 0
    else:
        sp_route = sp.route(destin_ID)
        results = [(edge[0], edge[1], traffic_flow) for edge in sp_route]
        return results, 1

def reduce_edge_flow(L, day, hour):
    ### Reduce (count the total traffic flow per edge)

    logger = logging.getLogger('main.one_step.reduce_edge_flow')
    t0 = time.time()
    edge_volume = {}
    for sublist in L:
        for p in sublist: ### p[0], p[1] is start/end ID of graph.mtx node, p[2] is edge flow
            try:
                edge_volume[(p[0], p[1])] += p[2]
            except KeyError:
                edge_volume[(p[0], p[1])] = p[2]
    t1 = time.time()
    logger.info('DY{}_HR{}: # edges to be updated {}, taking {} seconds'.format(day, hour, len(edge_volume), t1-t0))
    
    ### Convert key to str((p[0], p[1])) before saving
    # with open('edge_volume.json', 'w') as outfile:
    #     json.dump(edge_volume, outfile, indent=2)

    return edge_volume

def reduce_edge_flow_pd(L, day, hour):
    ### Reduce (count the total traffic flow per edge) with pandas groupby

    logger = logging.getLogger('main.one_step.reduce_edge_flow_pd')
    t0 = time.time()
    flat_L = [edge_pop_tuple for sublist in L for edge_pop_tuple in sublist]
    df_L = pd.DataFrame(flat_L, columns=['start', 'end', 'flow'])
    df_L_flow = df_L.groupby(['start', 'end']).sum().reset_index()
    t1 = time.time()
    logger.info('DY{}_HR{}: {} edges to be updated, taking {} seconds with pandas groupby'.format(day, hour, df_L_flow.shape[0], t1-t0))

    # print(df_L_flow.head())
    #        start    end  flow
    # 0      1      35200    29
    # 1      1      35201    24
    # 2      2      38960     1
    # 3      3      23600     1
    # 4      3      36959     1
    
    #df_L_pop.to_csv('edge_volume_pq.csv')
    
    return df_L_flow

def one_step(day, hour):
    ### One time step of ABM simulation
    
    logger = logging.getLogger('main.one_step')

    ### Read the OD table of this time step
    global OD
    OD = pd.read_csv(absolute_path+'/../1_OD/output/DY{}/SF_OD_DY{}_HR{}.csv'.format(day, day, hour))

    ### Number of processes
    process_count = 4
    logger.debug('number of process is {}'.format(process_count))

    ### Build a pool
    pool = Pool(processes=process_count)
    logger.debug('pool initialized')

    ### Find shortest pathes
    unique_origin = 5000 # OD.shape[0]
    logger.info('DY{}_HR{}: {} OD pairs (unique origins)'.format(day, hour, unique_origin))
    t_odsp_0 = time.time()
    res = pool.imap_unordered(map_edge_flow, range(unique_origin))

    ### Close the pool
    pool.close()
    pool.join()
    t_odsp_1 = time.time()
    logger.debug('shortest_path running time is {}'.format(t_odsp_1 - t_odsp_0))

    ### Collapse into edge total population dictionary
    edge_flow_tuples, destination_counts = zip(*res)
    logger.info('DY{}_HR{}: {} destinations'.format(day, hour, sum(destination_counts)))
    #edge_volume = reduce_edge_flow(edge_flow_tuples, day, hour)
    edge_volume = reduce_edge_flow_pd(edge_flow_tuples, day, hour)

    return edge_volume

def main():
    logging.basicConfig(filename=absolute_path+'/sf_abm_mp.log', level=logging.DEBUG)
    logger = logging.getLogger('main')
    logger.info('{} \n\n'.format(datetime.datetime.now()))

    t_start = time.time()

    global g
    g = interface.readgraph(bytes(absolute_path+'/../0_network/data/sf/network_sparse.mtx', encoding='utf-8'))

    network_attr_df = pd.read_csv(absolute_path+'/../0_network/data/sf/network_attributes.csv')
    network_attr_df['fft'] = network_attr_df['sec_length']/network_attr_df['maxmph']*2.23694 ### mph to m/s

    for day in [4]:
        for hour in range(3, 5):

            logger.info('*************** DY{} HR{} ***************'.format(day, hour))

            t0 = time.time()
            edge_volume = one_step(day, hour)
            t1 = time.time()
            logger.info('DY{}_HR{}: running time {}'.format(day, hour, t1-t0))

            ### Update graph
            t_update_0 = time.time()
            ### if edge_volume is a pandas data frame
            logger.info('DY{}_HR{}: max link volume {}'.format(day, hour, max(edge_volume['flow'])*10))
            edge_volume = pd.merge(edge_volume, network_attr_df, how='left', left_on=['start', 'end'], right_on=['start_mtx', 'end_mtx'])
            edge_volume['t_new'] = edge_volume['fft']*(1.2+0.78*(edge_volume['flow']*10/edge_volume['capacity'])**4)
            for index, row in edge_volume.iterrows():
                g.update_edge(int(row['start_mtx']), int(row['end_mtx']), c_double(row['t_new']))
            ### if edge_volume is a python dictionary
            # for key, value in edge_volume.items():
            #     g.update_edge(key[0], key[1], c_double(value))
            t_update_1 = time.time()
            logger.info('DY{}_HR{}: updating time {}'.format(day, hour, t_update_1-t_update_0))

            #write_geojson(g, day, hour)

    t_end = time.time()
    logger.info('total run time is {} seconds \n\n\n\n\n'.format(t_end-t_start))

if __name__ == '__main__':
    main()

