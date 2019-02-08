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

pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, absolute_path+'/../')
sys.path.insert(0, '/Users/bz247/')
from sp import interface 

folder = 'sf_overpass'
scenario = 'original'

def map_edge_flow(row):
    ### Find shortest path for each unique origin --> one destination
    ### In the future change to multiple destinations
    
    origin_ID = int(OD_incre['start_sp'].iloc[row])
    destin_ID = int(OD_incre['end_sp'].iloc[row])
    traffic_flow = int(OD_incre['flow'].iloc[row]) ### number of travellers with this OD
    probe_veh = int(OD_incre['probe'].iloc[row]) ### 1 if the shortest path between this OD pair is traversed by a probe vehicle

    sp = g_0.dijkstra(origin_ID, destin_ID) ### g_0 is the network with imperfect information for route planning
    sp_dist = sp.distance(destin_ID) ### agent believed travel time with imperfect information
    if sp_dist > 10e7:
        return [], 0 ### empty path; not reach destination; travel time 0
    else:
        sp_route = sp.route(destin_ID) ### agent route planned with imperfect information
        results = [origin_ID, destin_ID, sp_dist, traffic_flow, probe_veh]+[(edge[0], edge[1]) for edge in sp_route]
        ### origin_ID: start node in sp
        ### destin_ID: end node in sp
        ### sp_dist: agent-believed travel time
        ### traffic_flow: num of agents between this OD pair
        ### probe_veh: num of probe vehicles (with location service on) between this OD pair
        ### [(edge[0], edge[1]) for edge in sp_route]: agent's choice of route
        return results, 1


def reduce_edge_flow_pd(agent_routes, day, hour, incre_id):
    ### Reduce (count the total traffic flow per edge) with pandas groupby

    logger = logging.getLogger('reduce')
    t0 = time.time()
    flat_L = [(e[0], e[1], r[3], r[4]) for r in agent_routes for e in r[5:]] ### r[0]-r[4] are origin_ID, destin_ID, sp_dist, traffic_flow and probe_flow
    df_L = pd.DataFrame(flat_L, columns=['start_sp', 'end_sp', 'flow', 'probe'])
    df_L_flow = df_L.groupby(['start_sp', 'end_sp']).agg({
        'flow': np.sum, 'probe': np.sum}).rename(columns={
        'flow': 'link_flow', 'probe': 'link_probe'}).reset_index() # link_flow counts the number of vehicles, link_probe counts the number of probe vehicles
    t1 = time.time()
    logger.debug('DY{}_HR{} INC {}: reduce find {} edges, {} sec w/ pd.groupby, max link flow {}, max link probe {}'.format(day, hour, incre_id, df_L_flow.shape[0], t1-t0, max(df_L_flow['link_flow']), max(df_L_flow['link_probe'])))
    
    return df_L_flow

def map_reduce_edge_flow(day, hour, incre_id):
    ### One time step of ABM simulation
    
    logger = logging.getLogger('map')

    ### Build a pool
    process_count = 4
    pool = Pool(processes=process_count)

    ### Find shortest pathes
    unique_origin = OD_incre.shape[0]
    t_odsp_0 = time.time()
    res = pool.imap_unordered(map_edge_flow, range(unique_origin))

    ### Close the pool
    pool.close()
    pool.join()
    t_odsp_1 = time.time()

    ### Organize results
    ### non-empty path; 1 reaches destination;
    agent_routes, destination_counts = zip(*res)

    logger.debug('DY{}_HR{} INC {}: {} O --> {} D found, dijkstra pool {} sec on {} processes'.format(day, hour, incre_id, unique_origin, sum(destination_counts), t_odsp_1 - t_odsp_0, process_count))

    #edge_volume = reduce_edge_flow(edge_flow_tuples, day, hour)
    edge_volume = reduce_edge_flow_pd(agent_routes, day, hour, incre_id)

    return edge_volume

def update_graph(edge_volume, edges_df, day, hour, incre_id, sigma, link_probe_set, link_probe_count):
    ### Update graph

    logger = logging.getLogger('update')
    t_update_0 = time.time()

    ### first update the cumulative flow in the current time step
    edges_df = pd.merge(edges_df, edge_volume, how='left', on=['start_sp', 'end_sp'])
    edges_df = edges_df.fillna(value={'link_flow': 0, 'link_probe': 0}) ### fill flow for unused edges as 0
    edges_df['hour_flow'] += edges_df['link_flow'] ### update the cumulative flow

    edge_probe_df = edges_df.loc[edges_df['link_probe']>0].copy().reset_index()
    #logger.info('links to be updated {}'.format(edge_probe_df.shape[0]))
    if edge_probe_df.shape[0]==0:
        pass
    else:
        edge_probe_df['t_avg'] = edge_probe_df['fft']*(1.3 + 1.3*0.6*(edge_probe_df['hour_flow']/edge_probe_df['capacity'])**4) ### ground truth average travel time for links that are being used
        edge_probe_df['v_avg'] = edge_probe_df['length']/edge_probe_df['t_avg']
        edge_probe_df['v_probe'] = edge_probe_df.apply(lambda r: max(0.1, np.random.normal(r['v_avg'], sigma/np.sqrt(r['link_probe']))), axis=1) ### Assuming individual speed ~ N(v_avg, v_avg*0.73151*exp(v_avg)). With probe vehicle size of N, the mean of all probe vehicles ~ N(v_avg, sigma/sqrt(N)). Chung and Recker, 2014. 2.23694 to convert m/s to mph.
        edge_probe_df['t_probe'] = edge_probe_df['length']/edge_probe_df['v_probe']
        #edge_probe_df['t_probe'] = edge_probe_df.apply(lambda r: max(0.1, r['t_avg']*np.random.normal(1, 1/np.sqrt(r['link_probe']))), axis=1)
        edge_probe_df['o-d'] = edge_probe_df.apply(lambda r: '{}-{}'.format(r['start_sp'], r['end_sp']), axis=1)
        link_probe_set = link_probe_set.union(set(edge_probe_df['o-d'].tolist()))
        link_probe_count += sum(edge_probe_df['link_probe'])

        for row in edge_probe_df.itertuples():
            g_0.update_edge(getattr(row,'start_sp'), getattr(row,'end_sp'), c_double(getattr(row,'t_probe')))

        #logger.info(edge_probe_df[['length', 't_avg', 't_probe', 'v_avg', 'v_probe']].describe())

    t_update_1 = time.time()
    #logger.info('DY{}_HR{} INC {}: {} edges updated in {} sec'.format(day, hour, incre_id, edge_probe_df.shape[0], t_update_1-t_update_0))

    edges_df = edges_df.drop(columns=['link_flow', 'link_probe'])
    #print(network_attr_df.loc[0])
    return edges_df, link_probe_set, link_probe_count

def read_OD(day, hour, probe_ratio):
    ### Read the OD table of this time step

    logger = logging.getLogger('read_OD')
    t_OD_0 = time.time()

    ### Change OD list from using osmid to sequential id. It is easier to find the shortest path based on sequential index.
    intracity_OD = pd.read_csv(absolute_path+'/../1_OD/output/{}/{}/DY{}/SF_OD_DY{}_HR{}.csv'.format(folder, scenario, day, day, hour))
    intercity_OD = pd.read_csv(absolute_path+'/../1_OD/output/{}/{}/intercity/intercity_HR{}.csv'.format(folder, scenario, hour))
    OD = pd.concat([intracity_OD, intercity_OD], ignore_index=True)
    nodes_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/nodes.csv'.format(folder, scenario))

    OD = pd.merge(OD, nodes_df[['node_id_igraph', 'node_osmid']], how='left', left_on='O', right_on='node_osmid')
    OD = pd.merge(OD, nodes_df[['node_id_igraph', 'node_osmid']], how='left', left_on='D', right_on='node_osmid', suffixes=['_O', '_D'])
    OD['start_sp'] = OD['node_id_igraph_O'] + 1 ### the node id in module sp is 1 higher than igraph id
    OD['end_sp'] = OD['node_id_igraph_D'] + 1
    OD = OD[['start_sp', 'end_sp', 'flow']]
    OD['probe'] = np.random.choice([0, 1], size=OD.shape[0], p=[1-probe_ratio, probe_ratio]) ### Randomly assigning 1% of vehicles to report speed
    OD = OD.sample(frac=1).reset_index(drop=True) ### randomly shuffle rows

    t_OD_1 = time.time()
    logger.debug('DY{}_HR{}: {} sec to read {} OD pairs, {} probes \n'.format(day, hour, t_OD_1-t_OD_0, OD.shape[0], sum(OD['probe'])))

    return OD, sum(OD['probe'])

def abm_static(random_seed, sigma, probe_ratio):

    logging.basicConfig(filename=absolute_path+'/sf_abm_mp_speed_info.log', level=logging.INFO)
    logger = logging.getLogger('main')
    logger.debug('{} \n'.format(datetime.datetime.now()))
    logger.debug('{} network'.format(folder))
    logger.debug('random seed {}'.format(random_seed))
    logger.debug('probe ratio: {}, sigma: {}'.format(probe_ratio, sigma))

    t_main_0 = time.time()

    ### Read in the initial network and make it a global variable
    #global g
    #g = interface.readgraph(bytes(absolute_path+'/../0_network/data/{}/{}/network_sparse.mtx'.format(folder, scenario), encoding='utf-8'))
    ### every hour a fft network for static assignment
    global g_0

    ### Read in the edge attribute for volume delay calculation later
    edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges.csv'.format(folder, scenario))
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft']] ### results will be returned for each (start_sp, end_sp)

    ### Prepare to split the hourly OD into increments
    global OD_incre
    num_of_incre = 20
    incre_p_list = [1/num_of_incre for i in range(num_of_incre)]
    incre_id_list = [i for i in range(num_of_incre)]
    logger.debug('{} increments'.format(num_of_incre))

    link_probe_set = set()
    link_probe_count = 0

    ### Loop through days and hours
    for day in [4]:
        for hour in range(7, 8):

            #logger.info('*************** DY{} HR{} ***************'.format(day, hour))
            t_hour_0 = time.time()

            ### static iterative assignment in each hour
            g_0 = interface.readgraph(bytes(absolute_path+'/../0_network/data/{}/{}/network_sparse.mtx'.format(folder, scenario), encoding='utf-8'))

            OD, probe_veh_counts = read_OD(day, hour, probe_ratio)
            OD_msk = np.random.choice(incre_id_list, size=OD.shape[0], p=incre_p_list)

            edges_df['hour_flow'] = 0 ### Reset the hourly cumulative traffic flow to zero at the beginning of each time step. This cumulates during the incremental assignment.

            for incre_id in incre_id_list:

                t_incre_0 = time.time()
                ### Split OD
                OD_incre = OD[OD_msk == incre_id]
                ### Routing (map reduce)
                edge_volume = map_reduce_edge_flow(day, hour, incre_id)
                ### Updating
                edges_df, link_probe_set, link_probe_count = update_graph(edge_volume, edges_df, day, hour, incre_id, sigma,link_probe_set, link_probe_count)
                t_incre_1 = time.time()
                logger.debug('DY{}_HR{} INCRE {}: {} sec, {} OD pairs \n'.format(day, hour, incre_id, t_incre_1-t_incre_0, OD_incre.shape[0]))

            t_hour_1 = time.time()
            logger.debug('DY{}_HR{}: {} sec \n'.format(day, hour, t_hour_1-t_hour_0))

            #edges_df[['edge_id_igraph', 'hour_flow']].to_csv(absolute_path+'/output/test/edge_flow_DY{}_HR{}_probe{}_vsdev{}.csv'.format(day, hour, probe_ratio, sigma), index=False)
            edges_df['t_avg'] = edges_df['fft']*(1.3 + 1.3*0.6*(edges_df['hour_flow']/edges_df['capacity'])**4)
            edges_df['v_avg'] = edges_df['length']/edges_df['t_avg']
            edges_df['vht'] = edges_df['t_avg']*edges_df['hour_flow']/3600
            edges_df['vkmt'] = edges_df['length']*edges_df['hour_flow']/1000
            n_largest = edges_df['hour_flow'].sort_values(ascending=False).tolist()[0:10]
            logger.debug('links probed {}, w/ repetition {}, VHT {}, VKMT {}, Max.10 {}'.format(len(link_probe_set), link_probe_count, sum(edges_df['vht']), sum(edges_df['vkmt']), np.average(n_largest)))
            #g.writegraph(bytes(absolute_path+'/output_incre/network_result_DY{}_HR{}.mtx'.format(day, hour), encoding='utf-8'))

    t_main_1 = time.time()
    logger.debug('total run time: {} sec \n\n\n\n\n'.format(t_main_1 - t_main_0))
    return [probe_veh_counts, len(link_probe_set), link_probe_count, sum(edges_df['vht']), sum(edges_df['vkmt']), np.average(n_largest)]

def main():
    random_seed = os.environ['SLURM_ARRAY_TASK_ID']
    #random_seed = 0

    results_collect = []
    for sigma in [0, 1, 2, 5, 10]:
    #for sigma in [0]:
        for probe_ratio in [0, 0.0001, 0.0002, 0.0005, 0.001, 0.01, 0.1, 1]:
        #for probe_ratio in [0]:
            results_abm = abm_static(random_seed, sigma, probe_ratio)
            results_abm = [sigma, probe_ratio] + results_abm
            results_collect.append(results_abm)

    results_collect_df = pd.DataFrame(results_collect, columns = ['sigma', 'probe_ratio', 'probe_veh_counts', 'links_probed_norepe', 'links_probed_repe', 'VHT', 'VKMT', 'max10'])
    results_collect_df.to_csv(absolute_path+'/output/speed_sensor/random_seed_{}.csv'.format(random_seed), index=False)

if __name__ == "__main__":
    main()
