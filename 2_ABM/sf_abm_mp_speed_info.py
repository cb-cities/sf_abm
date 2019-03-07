### Based on https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
import json
import sys
import numpy as np
import scipy.sparse as scipy_sparse
import scipy.io as sio
from multiprocessing import Pool 
import time 
import os
import logging
import datetime
import warnings
import pandas as pd 
from ctypes import *
import gc 

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
    
    agent_id = int(OD_ss['agent_id'].iloc[row])
    origin_ID = int(OD_ss['origin_sp'].iloc[row])
    destin_ID = int(OD_ss['destin_sp'].iloc[row])
    ss_id = int(OD_ss['ss_id'].iloc[row])
    agent_vol = int(OD_ss['flow'].iloc[row]) ### number of travellers with this OD
    probe_veh = int(OD_ss['probe'].iloc[row]) ### 1 if the shortest path between this OD pair is traversed by a probe vehicle

    sp = g.dijkstra(origin_ID, destin_ID) ### g_0 is the network with imperfect information for route planning
    sp_dist = sp.distance(destin_ID) ### agent believed travel time with imperfect information
    if sp_dist > 10e7:
        print('distance too long, agent id {}, ss id {}, o {}, d {}'.format(agent_id, ss_id, origin_ID, destin_ID))
        return [], 0 ### empty path; not reach destination; travel time 0
    else:
        sp_route = sp.route(destin_ID) ### agent route planned with imperfect information
        results = {'agent_id': agent_id, 'o_sp': origin_ID, 'd_sp': destin_ID, 'ss': ss_id, 'vol': agent_vol, 'probe': probe_veh, 'route': [edge[0] for edge in sp_route]+[destin_ID]}
        ### agent_ID: agent for each OD pair
        ### agent_vol: num of agents between this OD pair
        ### probe_veh: num of probe vehicles (with location service on) between this OD pair
        ### [(edge[0], edge[1]) for edge in sp_route]: agent's choice of route
        return results, 1


def reduce_edge_flow_pd(agent_info_routes, day, hour, ss_id):
    ### Reduce (count the total traffic flow per edge) with pandas groupby

    logger = logging.getLogger('reduce')
    t0 = time.time()

    flat_L = [(e[0], e[1], r['vol'], r['probe']) for r in agent_info_routes for e in zip(r['route'], r['route'][1:])]
    df_L = pd.DataFrame(flat_L, columns=['start_sp', 'end_sp', 'vol', 'probe'])
    df_L_flow = df_L.groupby(['start_sp', 'end_sp']).agg({
        'vol': np.sum, 'probe': np.sum}).rename(columns={
        'vol': 'ss_vol', 'probe': 'ss_probe'}).reset_index() # link_flow counts the number of vehicles, link_probe counts the number of probe vehicles
    t1 = time.time()
    logger.debug('DY{}_HR{} SS {}: reduce find {} edges, {} sec w/ pd.groupby, max substep volume {}, max substep probe {}'.format(day, hour, ss_id, df_L_flow.shape[0], t1-t0, max(df_L_flow['ss_vol']), max(df_L_flow['ss_probe'])))
    
    return df_L_flow

def map_reduce_edge_flow(day, hour, ss_id):
    ### One time step of ABM simulation
    
    logger = logging.getLogger('map-reduce')

    ### Build a pool
    process_count = 32
    pool = Pool(processes=process_count)

    ### Find shortest pathes
    unique_origin = OD_ss.shape[0]
    t_odsp_0 = time.time()
    res = pool.imap_unordered(map_edge_flow, range(unique_origin))

    ### Close the pool
    pool.close()
    pool.join()
    t_odsp_1 = time.time()

    ### Organize results
    ### non-empty path; 1 reaches destination;
    agent_info_routes, destination_counts = zip(*res)

    logger.debug('DY{}_HR{} SS {}: {} O --> {} D found, dijkstra pool {} sec on {} processes'.format(day, hour, ss_id, unique_origin, sum(destination_counts), t_odsp_1 - t_odsp_0, process_count))

    #edge_volume = reduce_edge_flow(edge_flow_tuples, day, hour)
    edge_volume = reduce_edge_flow_pd(agent_info_routes, day, hour, ss_id)

    return edge_volume, agent_info_routes

def update_graph(edge_volume, edges_df, day, hour, ss_id, hour_demand, assigned_demand, probed_link_list):
    ### Update graph

    logger = logging.getLogger('update')
    t_update_0 = time.time()

    ### first update the cumulative link volume in the current time step
    edges_df = pd.merge(edges_df, edge_volume, how='left', on=['start_sp', 'end_sp'])
    edges_df = edges_df.fillna(value={'ss_vol': 0, 'ss_probe': 0}) ### fill volume for unused edges as 0
    edges_df['true_vol'] += edges_df['ss_vol'] ### update the total volume (newly assigned + carry over)
 
    edges_df['perceived_vol'] = np.where(edges_df['ss_probe']==0, edges_df['perceived_vol'], edges_df['true_vol']) ### If there is a probe, then we can perceive the true volume (net + carry over); otherwise, we don't update the perceived volume.
    probed_link_list += edges_df[edges_df['ss_probe']>0]['edge_id_igraph'].tolist()

    ### True flux
    #edges_df['true_flow'] = edges_df['hour_vol']*hour_demand/assigned_demand

    ## Perceived flux
    edges_df['perceived_flow'] = edges_df['perceived_vol']*hour_demand/assigned_demand
    edges_df['perceived_t'] = edges_df['fft']*(1 + 0.6*(edges_df['perceived_flow']/edges_df['capacity'])**4)

    update_df = edges_df.loc[edges_df['perceived_t'] != edges_df['previous_t']].copy().reset_index()
    #logger.info('links to be updated {}'.format(edge_probe_df.shape[0]))
    for row in update_df.itertuples():
        g.update_edge(getattr(row,'start_sp'), getattr(row,'end_sp'), c_double(getattr(row,'perceived_t')))

    edges_df['previous_t'] = edges_df['perceived_t']
    edges_df = edges_df.drop(columns=['ss_vol', 'ss_probe', 'perceived_t'])

    t_update_1 = time.time()
    #logger.info('DY{}_HR{} INC {}: {} edges updated in {} sec'.format(day, hour, incre_id, edge_probe_df.shape[0], t_update_1-t_update_0))

    return edges_df, probed_link_list

def convert_to_graph(edges_df, identifier):
    ### Convert to mtx
    wgh = edges_df['fft']
    row = edges_df['start_sp'] - 1
    col = edges_df['end_sp'] - 1
    vcount = edges_df[['start_sp', 'end_sp']].values.max()
    g_coo = scipy_sparse.coo_matrix((wgh, (row, col)), shape=(vcount, vcount))
    sio.mmwrite(absolute_path+'/output/sensor/network_mtx/network_sparse_{}.mtx'.format(identifier), g_coo)

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
    OD['agent_id'] = range(OD.shape[0])
    OD['origin_sp'] = OD['node_id_igraph_O'] + 1 ### the node id in module sp is 1 higher than igraph id
    OD['destin_sp'] = OD['node_id_igraph_D'] + 1
    OD['probe'] = np.random.choice([0, 1], size=OD.shape[0], p=[1-probe_ratio, probe_ratio]) ### Randomly assigning 1% of vehicles to report speed
    OD = OD[['agent_id', 'origin_sp', 'destin_sp', 'flow', 'probe']]
    OD = OD.sample(frac=1).reset_index(drop=True) ### randomly shuffle rows

    t_OD_1 = time.time()
    logger.debug('DY{}_HR{}: {} sec to read {} OD pairs, {} probes \n'.format(day, hour, t_OD_1-t_OD_0, OD.shape[0], sum(OD['probe'])))

    return OD

def output_edges_df(edges_df, day, hour, random_seed, probe_ratio):

    ### Aggregate and calculate link-level variables after all increments
    
    edges_df['true_flow'] = edges_df['true_vol'] ### True hourly flow rate, used to calculate t_avg
    edges_df['t_avg'] = edges_df['fft']*(1 + 0.6*(edges_df['true_flow']/edges_df['capacity'])**4) ### True travel time
    #edges_df['v_avg'] = edges_df['length']/edges_df['t_avg']
    #edges_df['vht'] = edges_df['t_avg']*edges_df['hour_flow']/3600
    #edges_df['vkmt'] = edges_df['length']*edges_df['hour_flow']/1000

    ### Output
    edges_df[['edge_id_igraph', 'true_flow', 't_avg']].to_csv(absolute_path+'/output/sensor/edges_df/edges_df_DY{}_HR{}_r{}_p{}.csv'.format(day, hour, random_seed, probe_ratio), index=False)

def sta(random_seed=0, probe_ratio=1):

    logger = logging.getLogger('sta')
    logger.info('{} network, random_seed {}, probe_ratio {}'.format(folder, random_seed, probe_ratio))

    t_main_0 = time.time()
    ### Fix random seed
    np.random.seed(random_seed)
    ### Define global variables to be shared with subprocesses
    global g ### weighted graph
    global OD_ss ### substep demand

    ### Read in the edge attribute for volume delay calculation later
    edges_df0 = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges_elevation.csv'.format(folder, scenario))
    edges_df0 = edges_df0[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft']]
    #convert_to_graph(edges_df0, '0')

    ### Define substep parameters
    substep_counts = 20
    substep_ps = [1/substep_counts for i in range(substep_counts)] ### probability of being in each substep
    substep_ids = [i for i in range(substep_counts)]
    logger.debug('{} substeps'.format(substep_counts))

    sta_stats = []

    ### Loop through days and hours
    for day in [4]:

        ### Read in the initial network (free flow travel time)
        ### network_sparse_0.mtx is used for stand alone traffic simulation.
        ### Other mtx files are used when fft or capacity is modified through coupling with other models.
        g = interface.readgraph(bytes(absolute_path+'/output/sensor/network_mtx/network_sparse_{}.mtx'.format(0), encoding='utf-8'))
        ### Variables reset at each 3 AM
        edges_df = edges_df0 ### length, capacity and fft that should never change in one simulation
        edges_df['previous_t'] = edges_df['fft'] ### Used to find which edge to update. At the beginning of each day, previous_t is the free flow time.

        for hour in range(3, 5):

            #logger.info('*************** DY{} HR{} ***************'.format(day, hour))
            t_hour_0 = time.time()

            ### Read OD
            OD = read_OD(day, hour, probe_ratio)
            ### Total OD, assigned OD
            hour_demand = OD.shape[0]
            assigned_demand = 0
            ### Divide into substeps
            OD_msk = np.random.choice(substep_ids, size=OD.shape[0], p=substep_ps)
            OD['ss_id'] = OD_msk
            
            ### Initialize some parameters
            probed_link_list = [] ### probed links
            probe_veh_counts = np.sum(OD['probe'])

            ### Reset some variables at the beginning of each time step
            edges_df['true_vol'] = 0
            edges_df['perceived_vol'] = 0
            #agents_list = []

            for ss_id in substep_ids:

                t_substep_0 = time.time()

                ### Get the substep demand
                OD_ss = OD[OD['ss_id'] == ss_id]
                assigned_demand += OD_ss.shape[0]

                ### Routing for this substep (map reduce)
                edge_volume, agent_info_routes = map_reduce_edge_flow(day, hour, ss_id)
                ### Collecting agent routes in this increment
                #agents_list += agent_info_routes

                ### Updating
                edges_df, probed_link_list = update_graph(edge_volume, edges_df, day, hour, ss_id, hour_demand, assigned_demand, probed_link_list)

                t_substep_1 = time.time()
                logger.debug('DY{}_HR{} SS {}: {} sec, {} OD pairs'.format(day, hour, ss_id, t_substep_1-t_substep_0, OD_ss.shape[0], ))

            output_edges_df(edges_df, day, hour, random_seed, probe_ratio)

            ### Update carry over flow
            sta_stats.append([
                random_seed, probe_ratio,
                day, hour, hour_demand, probe_veh_counts, 
                len(set(probed_link_list)), len(probed_link_list)/len(set(probed_link_list)),
                np.sum(edges_df['t_avg']*edges_df['true_flow']),
                np.sum(edges_df['length']*edges_df['true_flow']),
                np.mean(edges_df.nlargest(10, 'true_flow')['true_flow'])
                ])

            t_hour_1 = time.time()
            ### log hour results before resetting the flow for the next time step
            logger.info('DY{}_HR{}: {} sec, OD {}'.format(day, hour, round(t_hour_1-t_hour_0, 3), hour_demand))
            gc.collect()
    
    t_main_1 = time.time()
    logger.info('total run time: {} sec \n\n\n\n\n'.format(t_main_1 - t_main_0))
    return sta_stats

def main():

    logging.basicConfig(filename=absolute_path+'/sf_abm_mp_speed_info.log', level=logging.INFO)
    logger = logging.getLogger('main')
    logger.info('no carry over volume')
    logger.info('{}'.format(datetime.datetime.now()))

    #random_seed = int(os.environ['RANDOM_SEED'])
    random_seed = 0

    results_collect = []
    #for probe_ratio in [0.001]:
    for probe_ratio in [1, 0.1, 0.01, 0.005, 0.001, 0]:
        sta_stats = sta(random_seed, probe_ratio)
        results_collect += sta_stats

    results_collect_df = pd.DataFrame(results_collect, columns = ['random_seed', 'probe_ratio', 'day', 'hour', 'hour_demand', 'probe_veh_counts', 'links_probed_norepe', 'links_probed_times', 'VHT', 'VKMT', 'max10'])
    results_collect_df.to_csv(absolute_path+'/output/sensor/summary_df/summary_r{}_p{}.csv'.format(random_seed, probe_ratio), index=False)

if __name__ == '__main__':
    main()
