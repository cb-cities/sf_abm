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
import boto3
import pandas as pd 

sys.path.insert(0, '/Users/bz247')
from sp import interface 

def map_edge_pop(row):
    ### Find shortest path for each unique origin --> multiple destinations
    
    origin_ID = OD['O'].iloc[row] ### origin's ID on graph nodes
    destin_ID = OD['D'].iloc[row] ### destination's ID on graph nodes
    traffic_flow = OD['flow'].iloc[row] ### number of travellers with this OD

    results = []
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', message="Couldn't reach some vertices at structural_properties") 
        path_collection = g.get_shortest_paths(origin_ID, destin_ID, weights='weight', output='epath')
    ### multiple destinations
    # for di in range(len(path_collection)):
    #     path_result = [(edge, population_list[di]) for edge in path_collection[di]]
    #     results += path_result
    ### one destinations
    if len(path_collection[0]) > 0:
        results = [(edge, traffic_flow) for edge in path_collection[0]]
        return results, 1
    else:
        return [], 0

def edge_tot_pop(L, day, hour):
    logger = logging.getLogger('main.one_step.edge_tot_pop')
    t0 = time.time()
    edge_volume = {}
    for sublist in L:
        for p in sublist: ### p[0] is edge ID on graph, p[1] is edge flow
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
    OD = pd.read_csv(absolute_path+'/../TNC/output/SF_graph_DY{}_HR{}_OD_50000.csv'.format(day, hour))
    # OD = scipy.sparse.load_npz(absolute_path+'/../TNC/output/DY{}_HR{}_OD.npz'.format(day, hour)) ### An hourly OD matrix for SF based Uber/Lyft origins and destinations
    # logger.debug('finish reading sparse OD matrix, shape is {}'.format(OD.shape))
    # OD = OD.tolil()
    # logger.debug('finish converting the matrix to lil')

    ### Define processes
    process_count = 4
    logger.debug('number of process is {}'.format(process_count))

    ### Build a pool
    pool = Pool(processes=process_count)
    logger.debug('pool initialized')

    ### Find shortest pathes
    unique_origin = 2000
    logger.info('DY{}_HR{}: # OD rows (unique origins) {}'.format(day, hour, unique_origin))

    t_odsp_0 = time.time()
    res = pool.imap_unordered(map_edge_pop, range(unique_origin))

    ### Close the pool
    pool.close()
    pool.join()
    t_odsp_1 = time.time()
    logger.debug('shortest_path time is {}'.format(t_odsp_1 - t_odsp_0))

    ### Collapse into edge total population dictionary
    edge_pop_tuples, destination_counts = zip(*res)
    logger.info('DY{}_HR{}: # destinations {}'.format(day, hour, sum(destination_counts)))
    edge_volume = edge_tot_pop(edge_pop_tuples, day, hour)
    #print(list(edge_volume.items())[0])

    return edge_volume

### Put geojson object to S3, which will be accessed by DeckGL
def geojson2s3(geojson_dict, out_bucket, out_key):
    s3client = boto3.client('s3')
    s3client.put_object(
        Body=json.dumps(geojson_dict, indent=2), 
        Bucket=out_bucket, 
        Key=out_key, 
        #ContentType='application/json',
        ACL='private')#'public-read'

def write_geojson(g, day, hour):
    feature_list = []

    for edge in g.es:
        feature = {'type': 'Feature', 
            'geometry': {'type': 'LineString', 
                'coordinates': [[
                    g.vs[edge.source]['n_x'], g.vs[edge.source]['n_y']],[
                    g.vs[edge.target]['n_x'], g.vs[edge.target]['n_y']]]}, 
            'properties': {'link_id': edge['edge_osmid'], 
                'query_weekend': day, 'query_hour': hour, 
                'sec_speed': edge['sec_length']/edge['t_new'], 
                'sec_volume': edge['volume']}}
        feature_list.append(feature)
    
    feature_geojson = {'type': 'FeatureCollection', 'features': feature_list}

    S3_BUCKET = 'sf-abm'
    S3_FOLDER = 'test_0707/'
    KEY = S3_FOLDER+'DY{}_HR{}.json'.format(day, hour)
    geojson2s3(feature_geojson, S3_BUCKET, KEY)

def main():
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    logging.basicConfig(filename=absolute_path+'/sf_abm_mp.log', level=logging.DEBUG)
    logger = logging.getLogger('main')
    logger.info('{} \n\n'.format(datetime.datetime.now()))

    t_start = time.time()

    ### Read initial graph
    global g
    g = igraph.Graph.Read_Pickle(absolute_path+'/../data_repo/data/sf/network_graph.pkl')
    logger.info('graph summary {}'.format(g.summary()))
    g.es['fft'] = np.array(g.es['sec_length'], dtype=np.float)/np.array(g.es['maxmph'], dtype=np.float)*2.23694
    fft_array = np.array(g.es['fft'], dtype=np.float)
    capacity_array = np.array(g.es['capacity'], dtype=np.float)
    ### 2.23694 is to convert mph to m/s;
    ### the free flow time should still be calibrated rather than equal to the time at speed limit, check coefficient 1.2 in defining ['weight']
    logger.info('max/min FFT in seconds: {}/{}'.format(max(g.es['fft']), min(g.es['fft'])))

    g.es['weight'] = fft_array * 1.2 ### According to (Colak, 2015), for SF, even vol=0, t=1.2*fft, maybe traffic light? 1.2 is f_p - k_bay

    for day in [1]:
        for hour in range(9, 10):

            logger.info('*************** DY{} HR{} ***************'.format(day, hour))

            t0 = time.time()
            edge_volume = one_step(day, hour)
            t1 = time.time()
            logger.info('DY{}_HR{}: running time {}'.format(day, hour, t1-t0))

            ### Update graph
            volume_array = np.zeros(g.ecount())
            volume_array[list(edge_volume.keys())] = np.array(list(edge_volume.values()))*400 ### 400 is the factor to scale Uber/Lyft trip # to total car trip # in SF.
            g.es['volume'] = volume_array
            logger.info('DY{}_HR{}: max link volume {}'.format(day, hour, max(volume_array)))
            g.es['t_new'] = fft_array*(1.2+0.78*(volume_array/capacity_array)**4) ### BPR and (colak, 2015)
            g.es['weight'] = g.es['t_new']

            #write_geojson(g, day, hour)

    t_end = time.time()
    logger.info('total run time is {} seconds \n\n\n\n\n'.format(t_end-t_start))

if __name__ == '__main__':
    main()

