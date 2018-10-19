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

absolute_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, absolute_path+'/../')
#sys.path.insert(0, '/Users/bz247/')
from sp import interface 

def map_edge_flow(row):
    ### Find shortest path for each unique origin --> one destination
    ### In the future change to multiple destinations
    
    origin_ID = int(OD_incre['graph_O'].iloc[row]) + 1 ### origin's ID to graph.mtx node ID
    destin_ID = int(OD_incre['graph_D'].iloc[row]) + 1 ### destination's ID to graph.mtx node ID
    traffic_flow = int(OD_incre['flow'].iloc[row]) ### number of travellers with this OD

    results = []
    sp = g.dijkstra(origin_ID, destin_ID)
    sp_dist = sp.distance(destin_ID)
    if sp_dist > 10e7:
        return [], 0, 0 ### empty path; not reach destination; travel time 0
    else:
        sp_route = sp.route(destin_ID)
        results = [(edge[0], edge[1], traffic_flow) for edge in sp_route]
        return results, 1, sp_dist ### non-empty path; 1 reaches destination; travel time


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

def map_reduce_edge_flow(day, hour, incre_id):
    ### One time step of ABM simulation
    
    logger = logging.getLogger('map')

    ### Build a pool
    process_count = 32
    pool = Pool(processes=process_count)

    ### Find shortest pathes
    unique_origin = OD_incre.shape[0]
    t_odsp_0 = time.time()
    res = pool.imap_unordered(map_edge_flow, range(unique_origin))

    ### Close the pool
    pool.close()
    pool.join()
    t_odsp_1 = time.time()

    ### Collapse into edge total population dictionary
    edge_flow_tuples, destination_counts, travel_time_list_incre = zip(*res)

    logger.debug('DY{}_HR{} INC {}: {} O --> {} D found, dijkstra pool {} sec on {} processes'.format(day, hour, incre_id, unique_origin, sum(destination_counts), t_odsp_1 - t_odsp_0, process_count))

    #edge_volume = reduce_edge_flow(edge_flow_tuples, day, hour)
    edge_volume = reduce_edge_flow_pd(edge_flow_tuples, day, hour, incre_id)

    return edge_volume, travel_time_list_incre

def update_graph(edge_volume, network_attr_df, day, hour, incre_id):
    ### Update graph

    logger = logging.getLogger('update')
    t_update_0 = time.time()

    ### first update the cumulative flow in the current time step
    network_attr_df = pd.merge(network_attr_df, edge_volume, how='left', on=['start_mtx', 'end_mtx'])
    network_attr_df = network_attr_df.fillna(value={'flow': 0}) ### fill flow for unused edges as 0
    network_attr_df['cum_flow'] += network_attr_df['flow'] ### update the cumulative flow
    edge_update_df = network_attr_df.loc[network_attr_df['flow']>0].copy().reset_index() ### extract rows that are actually being used in the current increment
    edge_update_df['t_new'] = edge_update_df['fft'].values*(1.2+0.78*(edge_update_df['cum_flow'].values/edge_update_df['capacity'].values)**4)

    for row in edge_update_df.itertuples():
        g.update_edge(getattr(row,'start_mtx'), getattr(row,'end_mtx'), c_double(getattr(row,'t_new')))

    t_update_1 = time.time()
    logger.info('DY{}_HR{} INC {}: max volume {}, max_delay {}, updating time {}'.format(day, hour, incre_id, max(edge_update_df['flow']), max(edge_update_df['t_new']/edge_update_df['fft']), t_update_1-t_update_0))

    network_attr_df = network_attr_df.drop(columns=['flow'])
    #print(network_attr_df.loc[0])
    return network_attr_df

def read_OD(day, hour):
    ### Read the OD table of this time step

    logger = logging.getLogger('read_OD')
    t_OD_0 = time.time()

    OD = pd.read_csv(absolute_path+'/../1_OD/output/output_scaled_buffered/DY{}/SF_OD_DY{}_HR{}.csv'.format(day, day, hour))
    node_osmid2graphid_dict = json.load(open(absolute_path+'/../0_network/data/sf/node_osmid2graphid.json'))
    OD['graph_O'] = OD.apply(lambda row: node_osmid2graphid_dict[str(row['O'])], axis=1)
    OD['graph_D'] = OD.apply(lambda row: node_osmid2graphid_dict[str(row['D'])], axis=1)
    OD = OD[['graph_O', 'graph_D', 'flow']]
    OD = OD.sample(frac=1).reset_index(drop=True) ### randomly shuffle rows

    t_OD_1 = time.time()
    logger.debug('DY{}_HR{}: {} sec to read {} OD pairs \n'.format(day, hour, t_OD_1-t_OD_0, OD.shape[0]))

    return OD

def main():

    logging.basicConfig(filename=absolute_path+'/sf_abm_mp.log', level=logging.INFO)
    logger = logging.getLogger('main')
    logger.info('{} \n'.format(datetime.datetime.now()))
    logger.info('scaled and buffered OD generation')

    t_main_0 = time.time()

    ### Read in the initial network and make it a global variable
    global g
    g = interface.readgraph(bytes(absolute_path+'/../0_network/data/sf/network_sparse.mtx', encoding='utf-8'))

    ### Read in the edge attribute for volume delay calculation later
    network_attr_df = pd.read_csv(absolute_path+'/../0_network/data/sf/network_attributes.csv')
    network_attr_df['fft'] = network_attr_df['sec_length']/network_attr_df['maxmph']*2.23694 ### mph to m/s
    network_attr_df = network_attr_df.drop(columns=['start', 'end'])

    ### Prepare to split the hourly OD into increments
    global OD_incre
    incre_p_list = [0.1 for i in range(10)]
    incre_id_list = [i for i in range(10)]
    logger.info('{} increments'.format(10))

    ### Loop through days and hours
    for day in [0]:
        for hour in range(3, 27):

            logger.debug('*************** DY{} HR{} ***************'.format(day, hour))
            t_hour_0 = time.time()

            OD = read_OD(day, hour)
            OD_msk = np.random.choice(incre_id_list, size=OD.shape[0], p=incre_p_list)

            network_attr_df['cum_flow'] = 0 ### Reset the hourly cumulative traffic flow to zero at the beginning of each time step. This cumulates during the incremental assignment.

            travel_time_list = [] ### A list holding all the travel times

            for incre_id in incre_id_list:

                t_incre_0 = time.time()
                ### Split OD
                OD_incre = OD[OD_msk == incre_id]
                ### Routing (map reduce)
                edge_volume, travel_time_list_incre = map_reduce_edge_flow(day, hour, incre_id)
                travel_time_list += travel_time_list_incre
                ### Updating
                network_attr_df = update_graph(edge_volume, network_attr_df, day, hour, incre_id)
                t_incre_1 = time.time()
                logger.info('DY{}_HR{} INCRE {}: {} sec, {} OD pairs \n'.format(day, hour, incre_id, t_incre_1-t_incre_0, OD_incre.shape[0]))

            t_hour_1 = time.time()
            logger.info('DY{}_HR{}: {} sec \n'.format(day, hour, t_hour_1-t_hour_0))

            network_attr_df[['start_mtx', 'end_mtx', 'cum_flow']].to_csv(absolute_path+'/output/edge_flow_DY{}_HR{}.csv'.format(day, hour), index=False)

            with open(absolute_path + '/output/travel_time_DY{}_HR{}.txt'.format(day, hour), 'w') as f:
                for travel_time_item in travel_time_list:
                    f.write("%s\n" % travel_time_item)

            g.writegraph(bytes(absolute_path+'/output/network_DY{}_HR{}.mtx'.format(day, hour), encoding='utf-8'))

    t_main_1 = time.time()
    logger.info('total run time: {} sec \n\n\n\n\n'.format(t_main_1 - t_main_0))

if __name__ == '__main__':
    main()

