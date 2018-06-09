### Based on https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
import json
import sys
import igraph
import numpy as np
import scipy.sparse
import scipy.stats 
#from multiprocessing import Pool 
from itertools import repeat 
import time
import logging

def chunks(vcount, n):
    for i in range(0, vcount, n+1):
        yield range(i, min(vcount, i+n+1))

def map_edge_pop(vL, g, OD, graphID_dict):
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
    return results, destination_counts

def edge_tot_pop(L):
    edge_volume = {}
    #for sublist in L:
    for p in L:
        try:
            edge_volume[p[0]] += p[1]
        except KeyError:
            edge_volume[p[0]] = p[1]
    logger.debug('numbers of edges to be updated {}'.format(len(edge_volume)))
    return edge_volume

def random_OD(g):
    ### Generate a random OD matrix
    ### Number of OD pairs = vcount**2 * density
    rvs = scipy.stats.poisson(25, loc=10).rvs
    OD_matrix = scipy.sparse.random(g.vcount(), g.vcount(), 
        density=2*10e-6, format='csr', data_rvs=rvs)
    OD_matrix = OD_matrix.tolil()
    return OD_matrix

def one_step(g, day, hour):
    ### One time step of ABM simulation

    ### Read/Generate OD matrix for this time step
    #OD_matrix = random_OD(g)
    OD_matrix = scipy.sparse.load_npz('TNC/OD_matrices/DY{}_HR{}_OD.npz'.format(day, hour))
    logger.debug('finish reading sparse OD matrix, shape is {}'.format(OD_matrix.shape))
    OD_matrix = OD_matrix.tolil()
    logger.info('finish converting the matrix to lil')
    ### Load the dictionary used to find the osm_node_id from matrix row/col id
    OD_nodesID_dict = json.load(open('TNC/OD_matrices/DY{}_HR{}_node_dict.json'.format(day, hour)))
    logger.info('finish loading nodesID_dict')
    g_vs_node_osmid = g.vs['node_osmid']
    logger.info('finish g_vs_node_osmid')
    g_vs_node_osmid_dict = {g_vs_node_osmid[i]: i for i in range(g.vcount())}
    logger.info('finish g_vs_node_osmid_dict')
    OD_graphID_dict = {int(key): g_vs_node_osmid_dict[value] for key, value in OD_nodesID_dict.items()}
    logger.info('finish converting OD matrix id to graph id')

    ### Partition the nodes into 4 chuncks
    vcount = 4 #OD_matrix.shape[0]
    logger.debug('number of origins {}'.format(vcount))
    # process_count = 1
    # partitioned_v = list(chunks(vcount, int(vcount/process_count)))
    # print('vertices partition finished')

    ### Build a pool
    # pool = Pool(processes=process_count)
    # print('pool initialized')

    ### Generate (edge, population) tuple
    #edge_pop_tuples = pool.starmap(map_edge_pop, zip(partitioned_v, repeat(g), repeat(OD_matrix), repeat(OD_graphID_dict)))
    edge_pop_tuples, destination_counts = map_edge_pop(range(vcount), g, OD_matrix, OD_graphID_dict)
    logger.debug('number of destinations {}'.format(destination_counts))    

    ### Close the pool
    # pool.close()
    # pool.join()

    ### Collapse into edge total population dictionary
    edge_volume = edge_tot_pop(edge_pop_tuples)
    #print(list(edge_volume.items())[0])

    return edge_volume


def main():
    ### Read initial graph
    g = igraph.load('data_repo/Imputed_data_False9_0509.graphmlz')
    logger.debug('graph summary {}'.format(g.summary()))
    g.es['weights'] = g.es['sec_length']
    logger.info('graph weights attribute created')

    t0 = time.time()
    edge_volume = one_step(g, 1, 3)
    t1 = time.time()
    logger.debug('running time for one time step is {}'.format(t1-t0))
    ### Update graph
    edge_weights = np.array(g.es['weights'])
    edge_weights[list(edge_volume.keys())] = np.array(list(edge_volume.values()))
    g.es['weights'] = edge_weights.tolist()


if __name__ == '__main__':
    logging.basicConfig(filename='sf_abm.log', level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.debug('Current time {}'.format(time.strftime('%Y-%m-%d %H:%M')))
    t_start = time.time()
    main()
    t_end = time.time()
    logger.debug('total run time is {} seconds'.format(t_end-t_start))

