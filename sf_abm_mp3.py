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

def map_edge_pop(pid):
    #logger.info('process ID is {}'.format(os.getpid()))
    print('process ID is {}, pid is {}'.format(os.getpid(), pid))
    #logger = logging.getLogger('main.one_step.map_edge_pop')
    #t0_process = time.time()
    
    ### Read initial graph
    #g = igraph.load('data_repo/Imputed_data_False9_0509.graphmlz')
    #logger.debug('graph summary {}'.format(g.summary()))
    #g.es['weights'] = g.es['sec_length']
    #logger.info('graph weights attribute created')
    
    day=1
    hour=3
    ### Read/Generate OD matrix for this time step
    #OD_matrix = random_OD(g)
    #OD = scipy.sparse.load_npz('TNC/OD_matrices/DY{}_HR{}_OD.npz'.format(day, hour))
    #logger.debug('finish reading sparse OD matrix, shape is {}'.format(OD_matrix.shape))
    #OD = OD.tolil()
    #logger.info('finish converting the matrix to lil')
    ### Load the dictionary used to find the osm_node_id from matrix row/col id
    #OD_nodesID_dict = json.load(open('TNC/OD_matrices/DY{}_HR{}_node_dict.json'.format(day, hour)))
    #logger.info('finish loading nodesID_dict')
    #g_vs_node_osmid = g.vs['node_osmid']
    #logger.info('finish g_vs_node_osmid')
    #g_vs_node_osmid_dict = {g_vs_node_osmid[i]: i for i in range(g.vcount())}
    #logger.info('finish g_vs_node_osmid_dict')
    #graphID_dict = {int(key): g_vs_node_osmid_dict[value] for key, value in OD_nodesID_dict.items()}
    #logger.info('finish converting OD matrix id to graph id')
    
    vcount = 1000
    process_count = 500
    vL = range(pid*(0+int(vcount/process_count)), min(vcount, (pid+1)*(0+int(vcount/process_count))))
    print('process ID is {}, vL is {}'.format(os.getpid(), vL))
    
    results = []
    destination_counts = 0
    for origin in vL:
        ### Origin's ID on graph
        origin_graphID = graphID_dict[origin]
        ### Destination list's IDs on graph
        destination_list = OD.rows[origin]
        destination_counts += len(destination_list)
        destination_graphID_list = [graphID_dict[d] for d in destination_list]
        ### Population traversing the OD
        population_list = OD.data[origin]
        if len(destination_list) > 0:
            path_collection = g.get_shortest_paths(
                #origin, destination_list, 
                origin_graphID, destination_graphID_list,
                weights='weights', output='epath')
            # try:
            #     print('path_collection[0][0]', path_collection[0][0])
            # except IndexError:
            #     pass
            #print(path_collection)
            for di in range(len(path_collection)):
                path_result = [(edge, population_list[di]) for edge in path_collection[di]]
                results += path_result
        #print('length of results', len(results))
        #print('destination_counts', destination_counts)
    #t1_process = time.time()
    #t0_process_f = datetime.datetime.fromtimestamp(int(t0_process)).strftime('%Y-%m-%d %H:%M:%S')
    #t1_process_f = datetime.datetime.fromtimestamp(int(t1_process)).strftime('%Y-%m-%d %H:%M:%S')
    #logger.info('process ID is {}, has {} OD pairs, starts at {}, ends at {}, takes {} seconds, id of graph object is {}, id of vL object is {}.'.format(os.getpid(), destination_counts, t0_process_f, t1_process_f, t1_process-t0_process, id(g), id(vL)))
    #return results, destination_counts
    return destination_counts

def edge_tot_pop(L):
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
    
    #logger = logging.getLogger('main.one_step')
    ### Read/Generate OD matrix for this time step
    global OD
    OD = scipy.sparse.load_npz('TNC/OD_matrices/DY{}_HR{}_OD.npz'.format(day, hour))
    #logger.debug('finish reading sparse OD matrix, shape is {}'.format(OD_matrix.shape))
    OD = OD.tolil()
    #logger.info('finish converting the matrix to lil')
    ### Load the dictionary used to find the osm_node_id from matrix row/col id
    global OD_nodesID_dict
    OD_nodesID_dict = json.load(open('TNC/OD_matrices/DY{}_HR{}_node_dict.json'.format(day, hour)))
    #logger.info('finish loading nodesID_dict')
    g_vs_node_osmid = g.vs['node_osmid']
    #logger.info('finish g_vs_node_osmid')
    g_vs_node_osmid_dict = {g_vs_node_osmid[i]: i for i in range(g.vcount())}
    #logger.info('finish g_vs_node_osmid_dict')
    global graphID_dict
    graphID_dict = {int(key): g_vs_node_osmid_dict[value] for key, value in OD_nodesID_dict.items()}
    #logger.info('finish converting OD matrix id to graph id')

    ### Partition the nodes into 4 chuncks
    #vcount = 1000 #OD_matrix.shape[0]
    #logger.debug('number of origins {}'.format(vcount))
    process_count = 4
    #logger.debug('numbers of cores is {}'.format(process_count))
    #partitioned_v = list(chunks(vcount, int(vcount/process_count)))
    #logger.info('vertices partition finished')

    ### Build a pool
    pool = Pool(processes=process_count)
    #logger.info('pool initialized')

    ### Generate (edge, population) tuple
    #res = pool.starmap(map_edge_pop, zip(partitioned_v, repeat(g), repeat(OD_matrix), repeat(OD_graphID_dict)))
    #res = pool.starmap(map_edge_pop, zip(
    #    [range(i, min(vcount, i+int(vcount/process_count)+1)) for i in range(0, vcount, int(vcount/process_count)+1)],
    #    [g for i in range(0,process_count)], [OD_matrix for i in range(0,process_count)], [OD_graphID_dict for i in range(0, process_count)]))
    res = pool.imap_unordered(map_edge_pop, range(500))
    #res = map_edge_pop(0)
    #edge_pop_tuples, destination_counts = zip(*res)
    #destination_counts = zip(*res)
    ### Close the pool
    pool.close()
    pool.join()
    #print('pool joined')
    print([i for i in res])
    #print(res)

    ### Collapse into edge total population dictionary
    #edge_volume = edge_tot_pop(edge_pop_tuples)
    #logger.debug('number of destinations {}'.format(sum(destination_counts)))
    #print(list(edge_volume.items())[0])

    #return edge_volume


def main():
    #logging.basicConfig(filename='sf_abm_multiprocess_0614_pm.log', level=logging.DEBUG)
    #logger = logging.getLogger('main')
    #logger.debug('')
    #logger.debug('Current time {}'.format(time.strftime('%Y-%m-%d %H:%M')))
    t_start = time.time()

    ### Read initial graph
    global g
    g = igraph.load('data_repo/Imputed_data_False9_0509.graphmlz')
    #logger.debug('graph summary {}'.format(g.summary()))
    g.es['weights'] = g.es['sec_length']
    #logger.info('graph weights attribute created')

    #t0 = time.time()
    #edge_volume = one_step(1, 3)
    one_step(1,3)
    #t1 = time.time()
    #logger.debug('running time for one time step is {}'.format(t1-t0))
    ### Update graph
    #edge_weights = np.array(g.es['weights'])
    #edge_weights[list(edge_volume.keys())] = np.array(list(edge_volume.values()))
    #g.es['weights'] = edge_weights.tolist()
    t_end = time.time()
    print(t_end-t_start)
    #logger.debug('total run time is {} seconds'.format(t_end-t_start))

if __name__ == '__main__':
    main()

