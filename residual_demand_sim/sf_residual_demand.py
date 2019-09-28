### Based on https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
import os
import gc 
import sys
import time 
import random
import logging
import datetime
import numpy as np
import pandas as pd 
from ctypes import *
import scipy.io as sio
from heapq import nlargest
import scipy.sparse as scipy_sparse
from multiprocessing import Pool 

pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, absolute_path+'/../')
from sp import interface 

folder = 'sf_overpass'

def base_co2(mph_array):
    ### CO2 - speed function constants (Barth and Boriboonsomsin, "Real-World Carbon Dioxide Impacts of Traffic Congestion")
    b0 = 7.362867270508520
    b1 = -0.149814315838651
    b2 = 0.004214810510200
    b3 = -0.000049253951464
    b4 = 0.000000217166574
    return np.exp(b0 + b1*mph_array + b2*mph_array**2 + b3*mph_array**3 + b4*mph_array**4)

def map_edge_flow_no_residual(row):
    ### Find shortest path for each unique origin --> one destination
    ### In the future change to multiple destinations
    
    agent_id = int(OD_ss['agent_id'].iloc[row])
    origin_ID = int(OD_ss['origin_sp'].iloc[row])
    destin_ID = int(OD_ss['destin_sp'].iloc[row])
    eco_veh = int(OD_ss['eco'].iloc[row]) ### 1 if the vehicle is going on the least co2 route

    if eco_veh == 0:
        sp = g_time.dijkstra(origin_ID, destin_ID)
    elif eco_veh == 1:
        sp = g_co2.dijkstra(origin_ID, destin_ID)
    else:
        return [], 'n_a'

    sp_dist = sp.distance(destin_ID) ### agent believed travel time with imperfect information
    if sp_dist > 10e7:
        return [], 'n_a' ### empty path; not reach destination, no travel time
    else:
        sp_route = sp.route(destin_ID) ### agent route planned with imperfect information

        # sp_route_df = pd.DataFrame([edge for edge in sp_route], columns=['start_sp', 'end_sp'])
        # #sp_route_df.insert(0, 'seq_id', range(sp_route_df.shape[0]))
        # sub_edges_df = edges_df[(edges_df['start_sp'].isin(sp_route_df['start_sp'])) & (edges_df['end_sp'].isin(sp_route_df['end_sp']))]

        # #sp_route_df = pd.merge(sp_route_df, sub_edges_df[['previous_t']], how='left')
        # sp_route_df = sp_route_df.merge(sub_edges_df[['start_sp', 'end_sp','previous_t']], on=['start_sp', 'end_sp'], how='left')
        # sp_route_df['timestamp'] = sp_route_df['previous_t'].cumsum()

        # trunc_sp_route_df = sp_route_df ### no truncation if no residual is considered
        # stop_node = trunc_sp_route_df.iloc[-1]['end_sp']
        # travel_time = trunc_sp_route_df.iloc[-1]['timestamp']
        # trunc_edges = trunc_sp_route_df[['start_sp', 'end_sp']]
        stop_node = destin_ID
        travel_time = sp_dist
        trunc_edges = pd.DataFrame([edge for edge in sp_route], columns=['start_sp', 'end_sp'])

        results = {'agent_id': agent_id, 'eco': eco_veh, 'o_sp': origin_ID, 'd_sp': destin_ID, 'h_sp': stop_node, 'travel_time': travel_time, 'route': trunc_edges}
        ### [(edge[0], edge[1]) for edge in sp_route]: agent's choice of route
        return results, 'a' ### 'a' means arrival

def map_edge_flow_residual(arg):
    ### Find shortest path for each unique origin --> one destination
    ### In the future change to multiple destinations
    row = arg[0]
    quarter_counts = arg[1]

    agent_id = int(OD_ss['agent_id'].iloc[row])
    origin_ID = int(OD_ss['origin_sp'].iloc[row])
    destin_ID = int(OD_ss['destin_sp'].iloc[row])
    eco_veh = int(OD_ss['eco'].iloc[row]) ### 1 if the vehicle is going on the least co2 route

    if eco_veh == 0:
        sp = g_time.dijkstra(origin_ID, destin_ID)
    elif eco_veh == 1:
        sp = g_co2.dijkstra(origin_ID, destin_ID)
    else:
        return [], 'n_a'

    ### Route distance, time or CO2 cost
    sp_dist = sp.distance(destin_ID) ### agent believed travel time with imperfect information
    if sp_dist > 10e7:
        return [], 'n_a' ### empty path; not reach destination, no travel time
    else:
        sp_route = sp.route(destin_ID) ### agent route planned with imperfect information

        sp_route_df = pd.DataFrame([edge for edge in sp_route], columns=['start_sp', 'end_sp'])
        #sp_route_df.insert(0, 'seq_id', range(sp_route_df.shape[0]))
        sub_edges_df = edges_df[(edges_df['start_sp'].isin(sp_route_df['start_sp'])) & (edges_df['end_sp'].isin(sp_route_df['end_sp']))]

        #sp_route_df = pd.merge(sp_route_df, sub_edges_df[['previous_t']], how='left')
        sp_route_df = sp_route_df.merge(sub_edges_df[['start_sp', 'end_sp','previous_t']], on=['start_sp', 'end_sp'], how='left')
        sp_route_df['timestamp'] = sp_route_df['previous_t'].cumsum()

        trunc_sp_route_df = sp_route_df[sp_route_df['timestamp']<(3600/quarter_counts)]
        try:
            stop_node = trunc_sp_route_df.iloc[-1]['end_sp']
            travel_time = trunc_sp_route_df.iloc[-1]['timestamp']
        except IndexError: ### caught in grid lock. Even the first link has travel time > assignment interval.
            stop_node = sp_route_df.iloc[0]['start_sp']
            travel_time = 0
        trunc_edges = trunc_sp_route_df[['start_sp', 'end_sp']]

        results = {'agent_id': agent_id, 'eco': eco_veh, 'o_sp': origin_ID, 'd_sp': destin_ID, 'h_sp': stop_node, 'travel_time': travel_time, 'route': trunc_edges}
        ### [(edge[0], edge[1]) for edge in sp_route]: agent's choice of route
        return results, 'a' ### 'a' means arrival, but could also stuck in the upstream node


def reduce_edge_flow_pd(agent_info_routes, day='', hour='', quarter='', ss_id=''):
    ### Reduce (count the total traffic flow per edge) with pandas groupby

    logger = logging.getLogger('reduce')
    t0 = time.time()
    # flat_L = [(e[0], e[1], r['vol'], r['probe']) for r in agent_info_routes for e in zip(r['route'], r['route'][1:])]
    # df_L = pd.DataFrame(flat_L, columns=['start_sp', 'end_sp', 'vol', 'probe'])
    df_L = pd.concat([r['route'] for r in agent_info_routes])
    df_L_flow = df_L.groupby(['start_sp', 'end_sp']).size().reset_index().rename(columns={0: 'ss_vol'}) # link_flow counts the number of vehicles, link_probe counts the number of probe vehicles
    t1 = time.time()
    logger.debug('DY{}_HR{}_QT{} SS {}: reduce find {} edges, {} sec w/ pd.groupby, max substep volume {}'.format(day, hour, quarter, ss_id, df_L_flow.shape[0], t1-t0, max(df_L_flow['ss_vol'])))
    
    return df_L_flow

def map_reduce_edge_flow(day='', hour='', quarter='', ss_id='', residual='', quarter_counts=''):
    ### One time step of ABM simulation
    
    logger = logging.getLogger('map-reduce')

    ### Build a pool
    process_count = 24
    pool = Pool(processes=process_count)

    ### Find shortest pathes
    unique_origin = OD_ss.shape[0]
    t_odsp_0 = time.time()

    if residual:
        if unique_origin == 0:
            return pd.DataFrame([], columns=['start_sp', 'end_sp', 'ss_vol']), [], [], 0
        elif hour <= 26:
            res = pool.imap_unordered(map_edge_flow_residual, [(i, quarter_counts) for i in range(unique_origin)])
        elif hour == 27: ### the final remaining demand
            res = pool.imap_unordered(map_edge_flow_no_residual, range(unique_origin))
        else:
            print('invalid hour')
    else:
        res = pool.imap_unordered(map_edge_flow_no_residual, range(unique_origin))

    ### Close the pool
    pool.close()
    pool.join()
    t_odsp_1 = time.time()

    ### Organize results
    ### non-empty path; 1 reaches destination;
    agent_info_routes, destination_counts = zip(*res)

    if np.sum([r['travel_time'] for r in agent_info_routes]) == 0: ### only grid locks, no movement
        edge_volume = pd.DataFrame([], columns=['start_sp', 'end_sp', 'ss_vol'])
    else:
        edge_volume = reduce_edge_flow_pd(agent_info_routes, day=day, hour=hour, quarter=quarter, ss_id=ss_id)
    
    ss_residual_OD_list = [(r['agent_id'], r['h_sp'], r['d_sp'], r['eco']) for r in agent_info_routes if r['h_sp']!=r['d_sp']]
    ss_travel_time_list = [(r['agent_id'], day, hour, quarter, ss_id, r['travel_time']) for r in agent_info_routes]
    ss_cannot_arrive = np.sum([1 for i in destination_counts if i=='n_a'])

    logger.debug('DY{}_HR{}_QT{} SS {}: {} O --> {} D found, dijkstra pool {} sec on {} processes'.format(day, hour, quarter, ss_id, unique_origin, unique_origin-ss_cannot_arrive, t_odsp_1 - t_odsp_0, process_count))

    return edge_volume, ss_residual_OD_list, ss_travel_time_list, ss_cannot_arrive

def update_graph(edge_volume, edges_df, traffic_only='', day='', hour='', quarter='', ss_id='', quarter_demand='', assigned_demand='', quarter_counts='', iri_impact=''):
    ### Update graph

    logger = logging.getLogger('update')
    t_update_0 = time.time()

    ### first update the cumulative link volume in the current time step
    edges_df = pd.merge(edges_df, edge_volume, how='left', on=['start_sp', 'end_sp'])
    edges_df = edges_df.fillna(value={'ss_vol': 0}) ### fill volume for unused edges as 0
    edges_df['quarter_vol'] += edges_df['ss_vol'] ### update the total volume (newly assigned + carry over)
    edges_df['tot_vol'] += edges_df['ss_vol'] ### tot_vol is not reset to 0 at each time step

    ### True flux
    edges_df['true_flow'] = (edges_df['quarter_vol']*quarter_demand/assigned_demand)*quarter_counts ### times quarter_counts to get the hourly flow. 

    edges_df['t_avg'] = edges_df['fft']*(1 + 0.6*(edges_df['true_flow']/edges_df['capacity'])**4)
    edges_df['v_avg_mph'] = edges_df['length']/edges_df['t_avg'] * 2.23694 ### time step link speed in mph
    if not traffic_only:
        edges_df['base_co2'] = base_co2(edges_df['v_avg_mph']) ### link-level co2 eimission in gram per mile per vehicle
        ### correction for slope
        edges_df['pci_co2'] = edges_df['base_co2'] * edges_df['slope_factor'] * edges_df['length'] /1609.34 * (1+iri_impact*(1+0.0714*(100-edges_df['pci_current']))) ### speed related CO2 x length x flow. Final results unit is gram.

    ### update time weights
    time_update_df = edges_df.loc[edges_df['t_avg'] != edges_df['previous_t']].copy().reset_index()
    #logger.info('links to be updated {}'.format(edge_probe_df.shape[0]))
    for row in time_update_df.itertuples():
        g_time.update_edge(getattr(row,'start_sp'), getattr(row,'end_sp'), c_double(getattr(row,'t_avg')))

    if not traffic_only:
        ### update CO2 weights
        eco_update_df = edges_df.loc[edges_df['pci_co2'] != edges_df['previous_co2']].copy().reset_index()
        #logger.info('links to be updated {}'.format(edge_probe_df.shape[0]))
        for row in eco_update_df.itertuples():
            g_co2.update_edge(getattr(row,'start_sp'), getattr(row,'end_sp'), c_double(getattr(row,'pci_co2')))

    edges_df['previous_t'] = edges_df['t_avg']
    if not traffic_only: edges_df['previous_co2'] = edges_df['pci_co2']
    edges_df = edges_df.drop(columns=['ss_vol'])

    t_update_1 = time.time()
    #logger.info('DY{}_HR{}_QT{} INC {}: {} edges updated in {} sec'.format(day, hour, quarter, incre_id, edge_probe_df.shape[0], t_update_1-t_update_0))

    return edges_df

def read_OD(year='', day='', hour='', eco_route_ratio='', case=''):
    ### Read the OD table of this time step

    logger = logging.getLogger('read_OD')
    t_OD_0 = time.time()

    ### Change OD list from using osmid to sequential id. It is easier to find the shortest path based on sequential index.
    if case != 'ps':
        intracity_OD = pd.read_csv(absolute_path+'/../1_OD/output/{}/intraSF_growth/SF_OD_YR{}_DY{}_HR{}.csv'.format(folder, year, day, hour))
        intercity_OD = pd.read_csv(absolute_path+'/../1_OD/output/{}/intercity_growth/intercity_YR{}_HR{}.csv'.format(folder, year, hour))
    else:
        intracity_OD = pd.read_csv(absolute_path+'/../1_OD/output/{}/peakspread/SF_OD_YR{}_DY{}_HR{}.csv'.format(folder, year, day, hour))
        intercity_OD = pd.read_csv(absolute_path+'/../1_OD/output/{}/peakspread/intercity_YR{}_HR{}.csv'.format(folder, year, hour))

    OD = pd.concat([intracity_OD, intercity_OD], ignore_index=True)
    nodes_df = pd.read_csv(absolute_path+'/../0_network/data/{}/nodes.csv'.format(folder))

    OD = pd.merge(OD, nodes_df[['node_id_igraph', 'node_osmid']], how='left', left_on='O', right_on='node_osmid')
    OD = pd.merge(OD, nodes_df[['node_id_igraph', 'node_osmid']], how='left', left_on='D', right_on='node_osmid', suffixes=['_O', '_D'])
    OD['agent_id'] = range(OD.shape[0])
    OD['origin_sp'] = OD['node_id_igraph_O'] + 1 ### the node id in module sp is 1 higher than igraph id
    OD['destin_sp'] = OD['node_id_igraph_D'] + 1
    OD['eco'] = np.random.choice([0, 1], size=OD.shape[0], p=[1-eco_route_ratio, eco_route_ratio]) ### Randomly assigning eco_route_ratio of vehicles to route by least emission route
    OD = OD[['agent_id', 'origin_sp', 'destin_sp', 'eco']]
    OD = OD.sample(frac=1).reset_index(drop=True) ### randomly shuffle rows

    # OD.to_csv(absolute_path+'/output/OD/OD_DY{}_HR{}_2.csv'.format(day, hour), index=False)

    t_OD_1 = time.time()
    logger.debug('DY{}_HR{}: {} sec to read {} OD pairs\n'.format(day, hour, t_OD_1-t_OD_0, OD.shape[0]))

    return OD

def quasi_sta(edges_df0, traffic_only='', outdir='', year='', day='', quarter_counts='', random_seed='', residual='', budget='', eco_route_ratio='', iri_impact='', case='', traffic_growth='', closure_list=[], closure_case=''):

    logger = logging.getLogger('quasi_sta')
    logger.info('{} network, random_seed {}'.format(folder, random_seed))

    t_main_0 = time.time()

    ### Fix random seed
    np.random.seed(random_seed)
    random.seed(random_seed)

    ### Define global variables to be shared with subprocesses
    global g_time ### weighted graph
    if not traffic_only: global g_co2
    global OD_ss ### substep demand
    global edges_df ### link weights

    ### Define quarter and substep parameters
    quarter_ps = [1/quarter_counts for i in range(quarter_counts)] ### probability of being in each division of hour
    quarter_ids = [i for i in range(quarter_counts)]
    
    substep_counts = 15
    substep_ps = [1/substep_counts for i in range(substep_counts)] ### probability of being in each substep
    substep_ids = [i for i in range(substep_counts)]
    logger.debug('{} quarters per hour, {} substeps'.format(quarter_counts, substep_counts))

    stats = []
    residual_OD_list = []
    travel_time_list = []

    ### Loop through days and hours
    for day in [day]:

        ### Read in the initial network (free flow travel time)
        g_time = interface.readgraph(bytes(absolute_path+'/../0_network/data/{}/network_sparse.mtx'.format(folder), encoding='utf-8'))
        if not traffic_only: g_co2 = interface.readgraph(bytes('{}/network/network_sparse_r{}_b{}_e{}_i{}_c{}_tg{}_y{}.mtx'.format(outdir, random_seed, budget, eco_route_ratio, iri_impact, case, traffic_growth, year), encoding='utf-8'))
        ### Variables reset at each 3 AM
        edges_df = edges_df0.copy() ### length, capacity and fft that should never change in one simulation
        edges_df['previous_t'] = edges_df['fft'] ### Used to find which edge to update. At the beginning of each day, previous_t is the free flow time.
        if not traffic_only: edges_df['previous_co2'] = edges_df['eco_wgh']
        edges_df['tot_vol'] = 0
        cannot_arrive = 0

        for hour in range(3, 28):

            #logger.info('*************** DY{} HR{} ***************'.format(day, hour))
            t_hour_0 = time.time()

            if hour <= 26:
                ### Read hourly OD
                OD = read_OD(year=year, day=day, hour=hour, eco_route_ratio=eco_route_ratio, case=case)
                ### Divide into quarters
                OD_quarter_msk = np.random.choice(quarter_ids, size=OD.shape[0], p=quarter_ps)
                OD['quarter'] = OD_quarter_msk
            elif hour==27: ### extra hour to handle remaining demand
                quarter_counts = 1
                OD = pd.DataFrame([], columns=['agent_id', 'origin_sp', 'end_sp', 'eco', 'quarter'])
            else:
                print('invalid hour')
                sys.exit(0)

            for quarter in range(quarter_counts):

                ### New OD in assignment period
                OD_quarter = OD[OD['quarter']==quarter]
                ### Add resudal OD
                OD_residual = pd.DataFrame(residual_OD_list, columns=['agent_id', 'origin_sp', 'destin_sp', 'eco'])
                OD_residual['quarter'] = quarter
                ### Total OD in each assignment period is the combined of new and residual OD
                OD_quarter = pd.concat([OD_quarter, OD_residual], ignore_index=True)
                ### Residual OD is no longer residual after it has been merged to the quarterly OD
                residual_OD_list = []
                OD_quarter = OD_quarter[OD_quarter['origin_sp'] != OD_quarter['destin_sp']]
                OD_quarter = OD_quarter.sort_values(by=['agent_id']) ### to make sure fixing the random seed works with subprocess results
                OD_quarter = OD_quarter.sample(frac=1).reset_index(drop=True)

                quarter_demand = OD_quarter.shape[0] ### total demand for this quarter, including total and residual demand
                residual_demand = OD_residual.shape[0] ### how many among the OD pairs to be assigned in this quarter are actually residual from previous quarters
                assigned_demand = 0
                OD_substep_msk = np.random.choice(substep_ids, size=quarter_demand, p=substep_ps)
                OD_quarter['ss_id'] = OD_substep_msk

                ### Reset some variables at the beginning of each time step
                edges_df['quarter_vol'] = 0

                for ss_id in substep_ids:

                    t_substep_0 = time.time()

                    ### Get the substep demand
                    OD_ss = OD_quarter[OD_quarter['ss_id'] == ss_id]
                    assigned_demand += OD_ss.shape[0]

                    ### Routing for this substep (map reduce)
                    edge_volume, ss_residual_OD_list, ss_travel_time_list, ss_cannot_arrive = map_reduce_edge_flow(day=day, hour=hour, quarter=quarter, ss_id=ss_id, residual=residual, quarter_counts=quarter_counts)
                    residual_OD_list += ss_residual_OD_list
                    travel_time_list += ss_travel_time_list
                    cannot_arrive += ss_cannot_arrive

                    ### Updating
                    edges_df = update_graph(edge_volume, edges_df, traffic_only=traffic_only, day=day, hour=hour, quarter=quarter, ss_id=ss_id, quarter_demand=quarter_demand, assigned_demand=assigned_demand, quarter_counts=quarter_counts, iri_impact=iri_impact)

                    t_substep_1 = time.time()
                    logger.debug('DY{}_HR{} SS {}: {} sec, {} OD pairs'.format(day, hour, ss_id, t_substep_1-t_substep_0, OD_ss.shape[0], ))

                #output_edges_df(edges_df, day, hour, quarter, residual, random_seed)
                edges_df[['edge_id_igraph', 'quarter_vol', 't_avg']].to_csv('{}/edges_df/edges_df_YR{}_DY{}_HR{}_qt{}_res{}_c{}_i{}_r{}.csv'.format(outdir, year, day, hour, quarter, residual, case, iri_impact, random_seed), index=False)

                ### Update carry over flow
                stats.append([
                    random_seed, year, day, hour, quarter, quarter_demand, residual_demand, len(residual_OD_list),
                    np.sum(edges_df['t_avg']*edges_df['quarter_vol']/(quarter_demand*60)),
                    np.sum(edges_df['length']*edges_df['quarter_vol']/(quarter_demand*1000)),
                    np.mean(edges_df.nlargest(10, 'quarter_vol')['quarter_vol'])
                    ])

                t_hour_1 = time.time()
                ### log hour results before resetting the flow for the next time step
                logger.info('DY{}_HR{}_QT{}: {} sec, OD {}, {} residual, {} cannot arrive'.format(day, hour, quarter, round(t_hour_1-t_hour_0, 3), quarter_demand, len(residual_OD_list), cannot_arrive))
                print('HR{} QT{}, {} sec, {} OD, producing {} residual, {} cannot arrive'.format(hour, quarter, round(t_hour_1 - t_hour_0, 1), quarter_demand, len(residual_OD_list), cannot_arrive))
                gc.collect()

    
    t_main_1 = time.time()
    logger.info('total run time: {} sec \n\n\n\n\n'.format(t_main_1 - t_main_0))
    print('total run time {} sec.'.format(t_main_1 - t_main_0))
    return stats, travel_time_list

def main():

    logging.basicConfig(filename=absolute_path+'/sf_residual_demand.log', level=logging.INFO)
    logger = logging.getLogger('main')
    logger.info('{}'.format(datetime.datetime.now()))

    ### Read in the edge attribute for volume delay calculation later
    edges_df0 = pd.read_csv(absolute_path+'/../0_network/data/{}/edges_elevation.csv'.format(folder))
    edges_df0 = edges_df0[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft']]
    #convert_to_graph(edges_df0, 'r{}_p{}'.format(random_seed, probe_ratio))

    residual = 1
    quarter_counts = 4
    random_seed = 0
    outdir = absolute_path+'/output'
    total_years = 1
    day = 2
    budget = 0
    eco_route_ratio=0
    iri_impact = 0
    case = 'nr'
    traffic_growth = 1
    closure_list = []
    closure_case = ''
    traffic_only = 1

    for year in range(total_years):
        x, y = quasi_sta(edges_df0, traffic_only=traffic_only, outdir=outdir, year=year, day=day, quarter_counts=quarter_counts, random_seed=random_seed, residual=residual, budget=budget, eco_route_ratio=eco_route_ratio, iri_impact=iri_impact, case=case, traffic_growth=traffic_growth, closure_list=closure_list, closure_case=closure_case)

if __name__ == '__main__':
    main()

