import os
import sys
import json 
import numpy as np 
import pandas as pd 
import scipy.sparse as sp
import scipy.io as sio
from ctypes import *
import gc

pd.set_option('display.max_columns', 10)
absolute_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, absolute_path+'/../')
sys.path.insert(0, '/Users/bz247/')
from sp import interface 

folder = 'sf_overpass'
scenario = 'original'

def optimal_dist(g, origin_ID, destin_ID):
    ### Find shortest path for each unique origin --> one destination

    sp = g.dijkstra(origin_ID, destin_ID)
    sp_dist = sp.distance(destin_ID) ### agent believed travel time with imperfect information
    return sp_dist

def actual_dist(edges_df, agent_route):
    agent_route_df = pd.DataFrame(agent_route, columns=['start_sp', 'end_sp'])
    agent_route_df = pd.merge(agent_route_df, edges_df[['start_sp', 'end_sp', 't_avg']], how='left', on=['start_sp', 'end_sp'])
    return sum(agent_route_df['t_avg'])

def get_graph_and_edges_df(random_seed, sigma, probe_ratio, incre_id):

    g = interface.readgraph(bytes(absolute_path+'/../0_network/data/{}/{}/network_sparse.mtx'.format(folder, scenario), encoding='utf-8'))
    if incre_id > 0:
        incre_edges_df = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/incre/edge_flow_incre{}_random{}_probe{}_sigma{}.csv'.format(incre_id-1, random_seed, probe_ratio, sigma))
        for row in incre_edges_df.itertuples():
            g.update_edge(getattr(row,'start_sp'), getattr(row,'end_sp'), c_double(getattr(row,'t_avg')))
    else:
        incre_edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges.csv'.format(folder, scenario))
        incre_edges_df['t_avg'] = incre_edges_df['fft']
        incre_edges_df = incre_edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 't_avg']]

    return g, incre_edges_df

def calculate_regret():
    random_seed = 0
    sigma = 10
    probe_ratio = 0.01

    agent_routes = json.load(open(absolute_path+'/../2_ABM/output/speed_sensor/agent_routes_random{}_probe{}_sigma{}.json'.format(random_seed, probe_ratio, sigma)))

    num_of_incre = 20
    incre_id_list = [i for i in range(num_of_incre)]
    #for incre_id in incre_id_list:
    for incre_id in range(0, 8):

        print('incre_id', incre_id)
        agents_regret = []

        g, incre_edges_df = get_graph_and_edges_df(random_seed, sigma, probe_ratio, incre_id)
        incre_agent_routes = agent_routes[str(incre_id)]
        
        for agent in incre_agent_routes:
            agent_optimal_dist = optimal_dist(g, agent['origin_sp'], agent['destin_sp'])
            if incre_id == 0:
                agent_actual_dist = agent_optimal_dist
            else:
                agent_actual_dist = actual_dist(incre_edges_df, agent['agent_route'])
            agents_regret.append([incre_id, agent['agent_id'], agent['origin_sp'], agent['destin_sp'], agent['agent_flow'], agent_optimal_dist, agent_actual_dist])

        agents_regret_df = pd.DataFrame(agents_regret, columns=['incre_id', 'agent_id', 'origin_sp', 'destin_sp', 'agent_flow', 'optimal_dist', 'actual_dist'])
        agents_regret_df = agents_regret_df.round({'optimal_dist': 4, 'actual_dist': 4})
        agents_regret_df.to_csv(absolute_path+'/regret/agent_regret_random{}_probe{}_sigma{}_incre{}.csv'.format(random_seed, probe_ratio, sigma, incre_id), index=False)
        gc.collect()

def population_regret():
    random_seed = 0
    sigma = 10
    probe_ratio = 0.01

    all_agents = pd.DataFrame(columns=['incre_id', 'agent_id', 'origin_sp', 'destin_sp', 'agent_flow', 'optimal_dist', 'actual_dist'])
    for incre_id in range(0, 20):
        incre_agents = pd.read_csv(absolute_path+'/regret/agent_regret_random{}_probe{}_sigma{}_incre{}.csv'.format(random_seed, probe_ratio, sigma, incre_id))
        print(probe_ratio, incre_id, sum(incre_agents['actual_dist'])/sum(incre_agents['optimal_dist']))
        all_agents = pd.concat([all_agents, incre_agents], ignore_index=True)
    print(probe_ratio)
    print(all_agents.shape)
    print(all_agents[['optimal_dist', 'actual_dist']].describe())
    print(sum(all_agents['optimal_dist']), sum(all_agents['actual_dist']))


if __name__ == '__main__':
    population_regret()
