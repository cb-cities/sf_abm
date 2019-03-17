from mpi4py import MPI
import sys
import numpy as np
from scipy.stats import gamma
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
from sp import interface 

folder = 'sf_overpass'
scenario = 'original'

def map_edge_flow(row, g):
    ### Find shortest path for each unique origin --> one destination
    ### In the future change to multiple destinations
    
    agent_id = int(getattr(row, 'agent_id'))
    origin_ID = int(getattr(row, 'origin_sp'))
    destin_ID = int(getattr(row, 'destin_sp'))
    #ss_id = int(getattr(row, 'ss_id'))
    ss_id = 0
    agent_vol = int(getattr(row, 'flow')) ### number of travellers with this OD
    probe_veh = int(getattr(row, 'probe')) ### 1 if the shortest path between this OD pair is traversed by a probe vehicle

    sp = g.dijkstra(origin_ID, destin_ID) ### g_0 is the network with imperfect information for route planning
    sp_dist = sp.distance(destin_ID) ### agent believed travel time with imperfect information
    if sp_dist > 10e7:
        return [] ### empty path; not reach destination; travel time 0
    else:
        sp_route = sp.route(destin_ID) ### agent route planned with imperfect information
        results = {'agent_id': agent_id, 'o_sp': origin_ID, 'd_sp': destin_ID, 'ss': ss_id, 'vol': agent_vol, 'probe': probe_veh, 'route': [edge[0] for edge in sp_route]+[destin_ID]}
        ### agent_ID: agent for each OD pair
        ### agent_vol: num of agents between this OD pair
        ### probe_veh: num of probe vehicles (with location service on) between this OD pair
        ### [(edge[0], edge[1]) for edge in sp_route]: agent's choice of route
        return results

def local_func(g, edges_array, OD_item):
    for e in edges_array:
        g.update_edge(int(e[0]), int(e[1]), c_double(e[2]))
    res_list = []
    for row in OD_item.itertuples():
        res = map_edge_flow(row, g)
        if len(res)>0:
            res_list.append(res)
    return len(res_list)

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

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    hname = MPI.Get_processor_name()
    
    #print("this is rank = {} (total {}) running on {}".format(rank, size, hname))
    #comm.Barrier()
    #sys.exit(0)			    
    
    ### Common setup
    g = interface.readgraph(bytes(absolute_path+'/../2_ABM/output/sensor_cov/network_mtx/network_sparse_{}.mtx'.format(0), encoding='utf-8'))

    if rank == 0:
        edges_df0 = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges_elevation.csv'.format(folder, scenario))
        edges_df = edges_df0[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft']].copy()
        edges_df['flow'] = 0
        edges_df['t_avg'] = edges_df['fft']*(1 + 0.6*(edges_df['flow']/edges_df['capacity'])**4)
        edges_array = edges_df[['start_sp', 'end_sp', 't_avg']].values
        
        OD = read_OD(4, 18, 1)
<<<<<<< HEAD
        #OD = OD.iloc[0:200]
        OD['randNumCol'] = np.tile(range(size), reps=int(np.ceil(OD.shape[0]/size)))[0:OD.shape[0]]
=======
        OD = OD.iloc[0:200]
        OD['randNumCol'] = np.random.randint(0, size, OD.shape[0])
>>>>>>> e89c1e8d9d5f618747abccc542bfe9426754e376
        OD_list = [od for _, od in OD.groupby('randNumCol')]
        #OD_list = [OD.iloc[i*100: i*100+99] for i in range(size)]
        counts = [int(0)]*size
    else:
        edges_array = None
        OD_list = None
        counts = None

    ### Distribute workload
    count_item = comm.scatter(counts, root=0)
    edges_array = comm.bcast(edges_array, root=0)
    OD_item = comm.scatter(OD_list, root=0)
 
    # Map
<<<<<<< HEAD
    #print(rank, edges_array.shape)
=======
    print(rank, edges_array.shape)
>>>>>>> e89c1e8d9d5f618747abccc542bfe9426754e376
    count_item = local_func(g, edges_array,  OD_item)
    counts = comm.gather(count_item, root=0)
    #comm.Barrier()

    if rank == 0:
        sum_counts = sum(counts)
<<<<<<< HEAD
        print('rank size{}, total paths {}'.format(size, sum_counts))
        #print('end')


if __name__ == '__main__':
    #time_0 = time.time()
    main()
    #time_1 = time.time()
    #print('total time {}'.format(time_1-time_0))
=======
        print(sum_counts)
        print('end')


if __name__ == '__main__':
    main()
>>>>>>> e89c1e8d9d5f618747abccc542bfe9426754e376



