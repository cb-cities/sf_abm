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

    day=1
    hour=3
    
    ### Origin's ID on graph
    origin_graphID = graphID_dict[origin]
    ### Destination list's IDs on graph
    destination_list = OD.rows[origin]
    destination_counts = len(destination_list)
    destination_graphID_list = [graphID_dict[d] for d in destination_list]

    ### Population traversing the OD
    population_list = OD.data[origin]
    if len(destination_list) > 0:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message="Couldn't reach some vertices at structural_properties") 
            path_collection = g.get_shortest_paths(origin_graphID, destination_graphID_list, weights='weights', output='epath')
        for di in range(len(path_collection)):
            path_result = [(edge, population_list[di]) for edge in path_collection[di]]
            results = path_result
    #return results, destination_counts
    return destination_counts

def edge_tot_pop(L):
    ### NOT IN USE CURRENTLY
    #logger = logging.getLogger('main.one_step.edge_tot_pop')
    t0 = time.time()
    edge_volume = {}
    for sublist in L:
        for p in sublist:
            try:
                edge_volume[p[0]] += p[1]
            except KeyError:
                edge_volume[p[0]] = p[1]
    t1 = time.time()
    #logger.debug('numbers of edges to be updated {}, it took {} seconds'.format(len(edge_volume), t1-t0))
    return edge_volume

def one_step(day, hour):
    ### One time step of ABM simulation
    
    logger = logging.getLogger('main.one_step')

    ### Read/Generate OD matrix for this time step
    global OD
    OD = scipy.sparse.load_npz('TNC/OD_matrices/DY{}_HR{}_OD.npz'.format(day, hour)) ### An hourly OD matrix for SF based Uber/Lyft origins and destinations
    logger.debug('finish reading sparse OD matrix, shape is {}'.format(OD.shape))
    OD = OD.tolil()
    logger.debug('finish converting the matrix to lil')

    ### The following three steps needs to be re-written
    ### The idea is to query the OD based on the OD matrix row/col numbers, then find the corresponding igraph vertice IDs for shortest path computing
    ### Load the dictionary {OD_matrix_row/col_number: node_osmid}, as each row/col in OD matrix represent a node in the graph, and has a unique OSM node ID
    OD_nodesID_dict = json.load(open('TNC/OD_matrices/DY{}_HR{}_node_dict.json'.format(day, hour)))
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
    process_count = 4
    logger.debug('number of process is {}'.format(process_count))

    ### Build a pool
    pool = Pool(processes=process_count)
    logger.debug('pool initialized')

    ### Find shortest pathes
    unique_origin = 20000
    res = pool.imap_unordered(map_edge_pop, range(unique_origin))
    logger.debug('number of OD rows (unique origins) is {}'.format(unique_origin))
    #edge_pop_tuples, destination_counts = zip(*res)
    #destination_counts = zip(*res)

    ### Close the pool
    pool.close()
    pool.join()
    logger.debug('number of OD pairs is {}'.format(sum([i for i in res])))

    ### Collapse into edge total population dictionary
    #edge_volume = edge_tot_pop(edge_pop_tuples)
    #logger.debug('number of destinations {}'.format(sum(destination_counts)))
    #print(list(edge_volume.items())[0])

    #return edge_volume


def main():
    logging.basicConfig(filename='sf_abm_mp.log', level=logging.INFO)
    logger = logging.getLogger('main')

    t_start = time.time()

    ### Read initial graph
    global g
    g = igraph.load('data_repo/Imputed_data_False9_0509.graphmlz') ### This file contains the weekday 9am link level travel time for SF, imputed data collected from a month worth of Google Directions API
    logger.debug('graph summary {}'.format(g.summary()))
    g.es['weights'] = g.es['sec_length']
    logger.debug('graph weights attribute created')

    t0 = time.time()
    one_step(1,3)
    t1 = time.time()
    logger.debug('running time for one time step is {}'.format(t1-t0))
    ### Update graph
    #edge_weights = np.array(g.es['weights'])
    #edge_weights[list(edge_volume.keys())] = np.array(list(edge_volume.values()))
    #g.es['weights'] = edge_weights.tolist()
    t_end = time.time()
    logger.info('total run time is {} seconds'.format(t_end-t_start))

if __name__ == '__main__':
    main()

