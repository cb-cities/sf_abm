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
### CO2 - speed function constants (Barth and Boriboonsomsin, "Real-World Carbon Dioxide Impacts of Traffic Congestion")
b0 = 7.362867270508520
b1 = -0.149814315838651
b2 = 0.004214810510200
b3 = -0.000049253951464
b4 = 0.000000217166574

def map_edge_flow(row):
    ### Find shortest path for each unique origin --> one destination
    ### In the future change to multiple destinations
    
    agent_id = int(OD_ss['agent_id'].iloc[row])
    origin_ID = int(OD_ss['origin_sp'].iloc[row])
    destin_ID = int(OD_ss['destin_sp'].iloc[row])
    ss_id = int(OD_ss['ss_id'].iloc[row])
    agent_flow = int(OD_ss['flow'].iloc[row]) ### number of travellers with this OD
    probe_veh = int(OD_ss['probe'].iloc[row]) ### 1 if the shortest path between this OD pair is traversed by a probe vehicle
    eco_veh = int(OD_ss['eco'].iloc[row]) ### 1 if the vehicle is going on the least co2 route

    ### Graph
    if eco_veh == 0:
        sp = g_time.dijkstra(origin_ID, destin_ID)
    elif eco_veh == 1:
        sp = g_eco.dijkstra(origin_ID, destin_ID)
    else:
        return [], 0

    ### Route distance
    sp_dist = sp.distance(destin_ID) ### agent believed travel time with imperfect information
    if sp_dist > 10e7:
        return [], 0 ### empty path; not reach destination; travel time 0
    else:
        sp_route = sp.route(destin_ID) ### agent route planned with imperfect information
        results = {'agent_id': agent_id, 'o_sp': origin_ID, 'd_sp': destin_ID, 'ss': ss_id, 'flow': agent_flow, 'probe': probe_veh, 'eco': eco_veh, 'route': [edge[0] for edge in sp_route]+[destin_ID]}
        ### agent_ID: agent for each OD pair
        ### traffic_flow: num of agents between this OD pair
        ### probe_veh: num of probe vehicles (with location service on) between this OD pair
        ### [(edge[0], edge[1]) for edge in sp_route]: agent's choice of route
        return results, 1


def reduce_edge_flow_pd(agent_info_routes, day, hour, ss_id):
    ### Reduce (count the total traffic flow per edge) with pandas groupby

    logger = logging.getLogger('reduce')
    t0 = time.time()
    flat_L = [(e[0], e[1], r['flow'], r['probe']) for r in agent_info_routes for e in zip(r['route'], r['route'][1:])]
    df_L = pd.DataFrame(flat_L, columns=['start_sp', 'end_sp', 'flow', 'probe'])
    df_L_flow = df_L.groupby(['start_sp', 'end_sp']).agg({
        'flow': np.sum, 'probe': np.sum}).rename(columns={
        'flow': 'ss_flow', 'probe': 'ss_probe'}).reset_index() # link_flow counts the number of vehicles, link_probe counts the number of probe vehicles
    t1 = time.time()
    logger.debug('DY{}_HR{} SS {}: reduce find {} edges, {} sec w/ pd.groupby, max substep flow {}, max substep probe {}'.format(day, hour, ss_id, df_L_flow.shape[0], t1-t0, max(df_L_flow['ss_flow']), max(df_L_flow['ss_probe'])))
    
    return df_L_flow

def map_reduce_edge_flow(day, hour, ss_id):
    ### One time step of ABM simulation
    
    logger = logging.getLogger('map-reduce')

    ### Build a pool
    process_count = 4
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

def update_graph(edge_volume, edges_df, day, hour, ss_id, hour_demand, assigned_demand, probed_link_list, iri_impact):
    ### Update graph

    logger = logging.getLogger('update')
    t_update_0 = time.time()

    ### first update the cumulative flow in the current time step
    edges_df = pd.merge(edges_df, edge_volume, how='left', on=['start_sp', 'end_sp'])
    edges_df = edges_df.fillna(value={'ss_flow': 0, 'ss_probe': 0}) ### fill flow for unused edges as 0
    edges_df['hour_flow'] += edges_df['ss_flow'] ### update the total flow
 
    edges_df['perceived_hour_flow'] = np.where(edges_df['ss_probe']==0, edges_df['perceived_hour_flow'], edges_df['hour_flow']) ### If there is a probe, then we can perceive the true flow; otherwise, we don't update the perceived flow.
    probed_link_list += edges_df[edges_df['ss_probe']>0]['edge_id_igraph'].tolist()

    ### True flux
    #edges_df['hour_flux'] = edges_df['hour_flow']*hour_demand/assigned_demand

    ## Perceived flux
    edges_df['perceived_flux'] = edges_df['perceived_hour_flow']*hour_demand/assigned_demand
    edges_df['perceived_t'] = edges_df['fft']*(1 + 0.6*(edges_df['perceived_flux']/edges_df['capacity'])**4)
    ### Perceived emission
    edges_df['perceived_v'] = edges_df['length']/edges_df['perceived_t'] * 2.23694
    edges_df['perceived_co2'] = np.exp(
        b0 + 
        b1*edges_df['perceived_v'] + 
        b2*edges_df['perceived_v']**2 + 
        b3*edges_df['perceived_v']**3 + 
        b4*edges_df['perceived_v']**4) * (
        1+0.0714*iri_impact*(100-edges_df['pci_current'])) / 1609.34*edges_df['length']*edges_df['slope_factor']

    ### Update time weights
    time_update_df = edges_df.loc[edges_df['perceived_t'] != edges_df['previous_t']].copy().reset_index()
    #logger.info('links to be updated {}'.format(edge_probe_df.shape[0]))
    for row in time_update_df.itertuples():
        g_time.update_edge(getattr(row,'start_sp'), getattr(row,'end_sp'), c_double(getattr(row,'perceived_t')))

    ### Update co2 weights
    eco_update_df = edges_df.loc[edges_df['perceived_co2'] != edges_df['previous_co2']].copy().reset_index()
    #logger.info('links to be updated {}'.format(edge_probe_df.shape[0]))
    for row in eco_update_df.itertuples():
        g_eco.update_edge(getattr(row,'start_sp'), getattr(row,'end_sp'), c_double(getattr(row,'perceived_co2')))

    edges_df['previous_t'] = edges_df['perceived_t']
    edges_df['previous_co2'] = edges_df['perceived_co2']
    edges_df = edges_df.drop(columns=['ss_flow', 'ss_probe', 'perceived_flux', 'perceived_t', 'perceived_v', 'perceived_co2'])

    t_update_1 = time.time()
    #logger.info('DY{}_HR{} INC {}: {} edges updated in {} sec'.format(day, hour, incre_id, edge_probe_df.shape[0], t_update_1-t_update_0))

    return edges_df, probed_link_list

def read_OD(day, hour, probe_ratio, eco_route_ratio):
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
    OD['probe'] = np.random.choice([0, 1], size=OD.shape[0], p=[1-probe_ratio, probe_ratio]) ### Randomly assigning probe_ratio of vehicles to report speed
    OD['eco'] = np.random.choice([0, 1], size=OD.shape[0], p=[1-eco_route_ratio, eco_route_ratio]) ### Randomly assigning eco_route_ratio of vehicles to route by least emission route
    OD = OD[['agent_id', 'origin_sp', 'destin_sp', 'flow', 'probe', 'eco']]
    OD = OD.sample(frac=1).reset_index(drop=True) ### randomly shuffle rows

    t_OD_1 = time.time()
    logger.debug('DY{}_HR{}: {} sec to read {} OD pairs, {} probes, {} eco \n'.format(day, hour, t_OD_1-t_OD_0, OD.shape[0], sum(OD['probe']), sum(OD['eco'])))

    return OD

def output_edges_df(edges_df, year, day, hour, random_seed, probe_ratio, budget, eco_route_ratio, iri_impact, case):

    ### Aggregate and calculate link-level variables after all increments
    
    edges_df['true_flow'] = edges_df['hour_flow']
    edges_df['t_avg'] = edges_df['fft']*(1 + 0.6*(edges_df['true_flow']/edges_df['capacity'])**4)

    ### Output
    edges_df[['edge_id_igraph', 'true_flow', 't_avg']].to_csv(absolute_path+'/output_march/edges_df_abm/edges_df_b{}_e{}_i{}_c{}_y{}_HR{}.csv'.format(budget, eco_route_ratio, iri_impact, case, year, hour), index=False)

def sta(year, day=2, random_seed=0, probe_ratio=1, budget=100, eco_route_ratio=0.1, iri_impact=0.03, case='er'):

    logging.basicConfig(filename=absolute_path+'/logs/sta.log', level=logging.CRITICAL)
    logger = logging.getLogger('sta')
    logger.info('{}'.format(datetime.datetime.now()))
    logger.info('maintenance, {} network, year {}, random_seed {}, probe_ratio {}, eco_route_ratio {}, iri_impact {}'.format(folder, year, random_seed, probe_ratio, eco_route_ratio, iri_impact))

    t_main_0 = time.time()
    ### Fix random seed
    np.random.seed(random_seed)
    ### Define global variables to be shared with subprocesses
    global g_time ### graph weighted by travel time
    global g_eco ### graph weighted by co2 per link per vehicle
    global OD_ss ### substep demand

    ### Read in the initial network (free flow travel time)
    g_time = interface.readgraph(bytes(absolute_path+'/../0_network/data/{}/{}/network_sparse.mtx'.format(folder, scenario), encoding='utf-8'))
    g_eco = interface.readgraph(bytes(absolute_path+'/output_march/network/network_sparse_b{}_e{}_i{}_c{}_y{}.mtx'.format(budget, eco_route_ratio, iri_impact, case, year), encoding='utf-8'))

    ### Read in the edge attribute for volume delay calculation later
    edges_df = pd.read_csv(absolute_path+'/output_march/edge_df/edges_b{}_e{}_i{}_c{}_y{}.csv'.format(budget, eco_route_ratio, iri_impact, case, year))
    ### Set edge variables that will be updated at the end of each time step
    edges_df['previous_t'] = edges_df['fft']
    edges_df['previous_co2'] = edges_df['eco_wgh']

    ### Define substep parameters
    substep_counts = 20
    substep_ps = [1/substep_counts for i in range(substep_counts)] ### probability of being in each substep
    substep_ids = [i for i in range(substep_counts)]
    logger.debug('{} substeps'.format(substep_counts))

    ### Loop through days and hours
    for day in [day]:
        for hour in range(3, 5):

            #logger.info('*************** DY{} HR{} ***************'.format(day, hour))
            t_hour_0 = time.time()

            ### Read OD
            OD = read_OD(day, hour, probe_ratio, eco_route_ratio)
            ### Total OD, assigned OD
            hour_demand = OD.shape[0]
            assigned_demand = 0
            ### Divide into substeps
            OD_msk = np.random.choice(substep_ids, size=OD.shape[0], p=substep_ps)
            OD['ss_id'] = OD_msk
            
            ### Initialize some parameters
            probed_link_list = [] ### probed links

            ### Reset some variables at the beginning of each time step
            edges_df['hour_flow'] = 0
            edges_df['perceived_hour_flow'] = 0
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
                edges_df, probed_link_list = update_graph(edge_volume, edges_df, day, hour, ss_id, hour_demand, assigned_demand, probed_link_list, iri_impact)

                t_substep_1 = time.time()
                logger.debug('DY{}_HR{} SS {}: {} sec, {} OD pairs'.format(day, hour, ss_id, t_substep_1-t_substep_0, OD_ss.shape[0], ))

            output_edges_df(edges_df, year, day, hour, random_seed, probe_ratio, budget, eco_route_ratio, iri_impact, case)

            t_hour_1 = time.time()
            ### log hour results before resetting the flow for the next time step
            logger.info('DY{}_HR{}: {} sec, OD {}, VHT {}, l probed {}, unq l probed {}'.format(day, hour, round(t_hour_1-t_hour_0, 3), hour_demand, sum(edges_df['hour_flow']*edges_df['t_avg']/3600), len(probed_link_list), len(set(probed_link_list))))

    t_main_1 = time.time()
    logger.info('total run time: {} sec \n\n\n\n\n'.format(t_main_1 - t_main_0))
    #return probe_veh_counts, links_probed_norepe, links_probed_repe, VHT, VKMT, max10

