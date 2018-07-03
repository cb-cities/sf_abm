### Based on https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
import json
import sys
import igraph
import numpy as np
import scipy.sparse
import scipy.stats 
from multiprocessing import Pool 
from itertools import repeat 
import time 
import os
import logging
import datetime
import copy
import warnings

def map_edge_pop(origin):
    ### Find shortest path for each unique origin --> multiple destinations
    
    ### Origin's ID on graph
    origin_graphID = graphID_dict[origin]
    ### Destination list's IDs on graph
    destination_list = OD.rows[origin]
    destination_graphID_list = [graphID_dict[d] for d in destination_list]

    ### Population traversing the OD
    population_list = OD.data[origin]
    results = []
    if len(destination_list) > 0:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message="Couldn't reach some vertices at structural_properties") 
            path_collection = g.get_shortest_paths(origin_graphID, destination_graphID_list, weights='weight', output='epath')
        for di in range(len(path_collection)):
            path_result = [(edge, population_list[di]) for edge in path_collection[di]]
            results += path_result
    return results, len(destination_list)

def edge_tot_pop(L, day, hour):
    logger = logging.getLogger('main.one_step.edge_tot_pop')
    t0 = time.time()
    edge_volume = {}
    for sublist in L:
        for p in sublist:
            try:
                edge_volume[p[0]] += p[1]
            except KeyError:
                edge_volume[p[0]] = p[1]
    t1 = time.time()
    logger.info('DY{}_HR{}: # edges to be updated {}, taking {} seconds'.format(day, hour, len(edge_volume), t1-t0))
    
    # with open('edge_volume_2.json', 'w') as outfile:
    #     json.dump(edge_volume, outfile, indent=2)

    return edge_volume

def one_step(day, hour):
    ### One time step of ABM simulation
    
    logger = logging.getLogger('main.one_step')

    ### Read/Generate OD matrix for this time step
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    global OD
    OD = scipy.sparse.load_npz(absolute_path+'../TNC/OD_matrices/DY{}_HR{}_OD.npz'.format(day, hour)) ### An hourly OD matrix for SF based Uber/Lyft origins and destinations
    logger.debug('finish reading sparse OD matrix, shape is {}'.format(OD.shape))
    OD = OD.tolil()
    logger.debug('finish converting the matrix to lil')

    ### The following three steps needs to be re-written
    ### The idea is to query the OD based on the OD matrix row/col numbers, then find the corresponding igraph vertice IDs for shortest path computing
    ### Load the dictionary {OD_matrix_row/col_number: node_osmid}, as each row/col in OD matrix represent a node in the graph, and has a unique OSM node ID
    OD_nodesID_dict = json.load(open(absolute_path+'../TNC/OD_matrices/DY{}_HR{}_node_dict.json'.format(day, hour)))
    logger.debug('finish loading nodesID_dict')

    ### Create dictionary: {node_osmid: node_igraph_id}
    ### First, create a list of the OSM node id for all vertices of the graph, the order is based on the vertices ID in the graph
    g_vs_node_osmid = g.vs['node_osmid']
    logger.debug('finish g_vs_node_osmid')
    ### Then, create the dictionary {node_osmid: node_igraph_id}
    g_vs_node_osmid_dict = {g_vs_node_osmid[i]: i for i in range(g.vcount())}
    logger.debug('finish g_vs_node_osmid_dict')

    ### Create dictionary: {OD_matrix_row/col_number: node_igraph_id}
    global graphID_dict
    graphID_dict = {int(key): g_vs_node_osmid_dict[value] for key, value in OD_nodesID_dict.items()}
    logger.debug('finish converting OD matrix id to graph id')

    ### Define processes
    process_count = 32
    logger.debug('number of process is {}'.format(process_count))

    ### Build a pool
    pool = Pool(processes=process_count)
    logger.debug('pool initialized')

    ### Find shortest pathes
    non_empty_origin = [r for r in range(len(OD.rows)) if len(OD.rows[r])>0]
    unique_origin = 50897
    res = pool.imap_unordered(map_edge_pop, non_empty_origin[0:unique_origin])
    logger.info('DY{}_HR{}: # OD rows (unique origins) {}'.format(day, hour, unique_origin))

    ### Close the pool
    pool.close()
    pool.join()

    ### Collapse into edge total population dictionary
    edge_pop_tuples, destination_counts = zip(*res)
    logger.info('DY{}_HR{}: # destinations {}'.format(day, hour, sum(destination_counts)))
    edge_volume = edge_tot_pop(edge_pop_tuples, day, hour)
    #print(list(edge_volume.items())[0])

    return edge_volume

def main():
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    logging.basicConfig(filename=absolute_path+'sf_abm_mp.log', level=logging.INFO)
    logger = logging.getLogger('main')

    t_start = time.time()

    ### Read initial graph
    global g
    g = igraph.Graph.Read_GraphMLz(absolute_path+'../data_repo/SF.graphmlz')
    logger.info('{} \n\n'.format(datetime.datetime.now()))
    logger.info('graph summary {}'.format(g.summary()))
    g.es['fft'] = np.array(g.es['sec_length'], dtype=np.float)/np.array(g.es['speed_limit'], dtype=np.float)*2.23694
    logger.info('max/min FFT: {}/{}'.format(max(g.es['fft']), min(g.es['fft'])))

    g.es['weight'] = np.array(g.es['fft'], dtype=np.float)*1.2 ### According to (Colak, 2015), for SF, even vol=0, t=1.2*fft, maybe traffic light?

    for day in [1]:
        for hour in range(3, 4):

            logger.info('*************** DY{} HR{} ***************'.format(day, hour))

            t0 = time.time()
            edge_volume = one_step(day, hour)
            t1 = time.time()
            logger.info('DY{}_HR{}: running time {}'.format(day, hour, t1-t0))

            ### Update graph
            volume_array = np.zeros(g.ecount())
            volume_array[list(edge_volume.keys())] = np.array(list(edge_volume.values()))*400 ### 400 is the factor to scale Uber/Lyft trip # to total car trip # in SF.
            logger.info('DY{}_HR{}: max link volume {}'.format(day, hour, max(volume_array)))
            fft_array = np.array(g.es['fft'], dtype=np.float)
            capacity_array = np.array(g.es['capacity'], dtype=np.float)
            g.es['t_new'] = fft_array*(1.2+0.78*(volume_array/capacity_array)**4) ### BPR and (colak, 2015)
            g.es['weight'] = g.es['t_new']

    t_end = time.time()
    logger.info('total run time is {} seconds \n\n\n\n\n'.format(t_end-t_start))

if __name__ == '__main__':
    main()

