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

    ### Population traversing the OD
    #population_list = OD.data[origin]
    #if len(destination_list) > 0:
    if True:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message="Couldn't reach some vertices at structural_properties") 
            #path_collection = g.get_shortest_paths(origin_graphID, destination_graphID_list, weights='weights', output='epath')
            path_collection = g.get_shortest_paths(origin, weights='weight', output='epath')
            path_counts = len(path_collection)
        #for di in range(len(path_collection)):
            #path_result = [(edge, population_list[di]) for edge in path_collection[di]]
            #results = path_result
    #return results, destination_counts
    #return destination_counts
    return path_counts

def one_step():
    ### One time step of ABM simulation
    
    logger = logging.getLogger('main.one_step')

    ### Define processes
    process_count = 25
    logger.debug('number of process is {}'.format(process_count))

    ### Build a pool
    pool = Pool(processes=process_count)
    logger.debug('pool initialized')

    ### Find shortest pathes
    unique_origin = 320
    res = pool.imap_unordered(map_edge_pop, range(unique_origin))
    logger.debug('number of OD rows (unique origins) is {}'.format(unique_origin))
    #edge_pop_tuples, destination_counts = zip(*res)
    #destination_counts = zip(*res)

    ### Close the pool
    pool.close()
    pool.join()
    total_destination_count = sum([i for i in res])
    print(total_destination_count)
    logger.debug('number of destinations is {}'.format(total_destination_count))

    ### Collapse into edge total population dictionary
    #edge_volume = edge_tot_pop(edge_pop_tuples)
    #logger.debug('number of destinations {}'.format(sum(destination_counts)))
    #print(list(edge_volume.items())[0])

    #return edge_volume


def main():
    logging.basicConfig(filename='London_abm_mp_sssp.log', level=logging.DEBUG)
    logger = logging.getLogger('main')

    t_start = time.time()

    ### Read initial graph
    global g
    g = igraph.load('data_repo/London_Directed/London_0621.graphmlz') ### This file contains the weekday 9am link level travel time for SF, imputed data collected from a month worth of Google Directions API
    logger.debug('graph summary {}'.format(g.summary()))
    g.es['weight'] = g.es['length']
    logger.debug('graph weights attribute created')

    t0 = time.time()
    one_step()
    t1 = time.time()
    logger.debug('running time for one time step is {}'.format(t1-t0))
    ### Update graph
    #edge_weights = np.array(g.es['weights'])
    #edge_weights[list(edge_volume.keys())] = np.array(list(edge_volume.values()))
    #g.es['weights'] = edge_weights.tolist()
    t_end = time.time()
    print(t_end-t_start)
    logger.info('total run time is {} seconds'.format(t_end-t_start))


if __name__ == '__main__':
    main()

