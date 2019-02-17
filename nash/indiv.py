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

def agents_route_length(case, case_dict, nodes_dict, edges_dict):
    agents_list = json.load(open(absolute_path+'/../2_ABM/output/speed_sensor/indiv_agent/agent_routes_random0_probe{}_sigma{}.json'.format(case_dict[case]['probe'], case_dict[case]['sigma'])))
    agents_new_list = []
    for a in agents_list:
        #a_route_geom = 'LINESTRING ({})'.format(','.join(['{} {}'.format(a_n['lon'], a_n['lat']) for a_n in a['route']]))
        a_route_length = sum([edges_dict['{}-{}'.format(u, v)] for (u, v) in zip(a['route'], a['route'][1:])])
        agents_new_list.append({'agent_id': a['agent_id'], 'incre': a['incre'], 'route_length_{}'.format(case): a_route_length})
    
    agents_route_length_df = pd.DataFrame(agents_new_list)
    return agents_route_length_df

def length_by_case():
    edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges.csv'.format(folder, scenario))
    edges_dict = {}
    for e in edges_df.itertuples():
        edges_dict['{}-{}'.format(getattr(e, 'start_sp'), getattr(e, 'end_sp'))] = getattr(e, 'length')

    nodes_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/nodes.csv'.format(folder, scenario))
    nodes_dict = {}
    for n in nodes_df.itertuples():
        nodes_dict[getattr(n, 'node_id_igraph')+1] = (getattr(n, 'lon'), getattr(n, 'lat'))

    case_dict = {'perfect_info': {'sigma': 0, 'probe': 1},
                'no_info': {'sigma': 10, 'probe': 0},
                '10pct': {'sigma': 10, 'probe': 0.1},
                '1pct': {'sigma': 10, 'probe': 0.01}}
    agents_route_length_df = agents_route_length('perfect_info', case_dict, nodes_dict, edges_dict)
    for case in ['no_info', '10pct', '1pct']:
        case_df = agents_route_length(case, case_dict, nodes_dict, edges_dict)
        agents_route_length_df = pd.merge(agents_route_length_df, case_df, how='left', on=['agent_id', 'incre'])
    agents_route_length_df.to_csv(absolute_path+'/agents_route_length_by_case.csv', index=False)


def analyze():
    agents_route_length_df = pd.read_csv(absolute_path+'/agents_route_length_by_case.csv')
    #print(agents_route_length_df.describe())
    agents_route_length_df['std'] = agents_route_length_df[['route_length_perfect_info', 'route_length_no_info', 'route_length_10pct', 'route_length_1pct']].std(axis=1)
    agents_route_length_df = agents_route_length_df.sort_values(by='std', ascending=False).reset_index(drop=True)
    print(agents_route_length_df.head())

#        agent_id  incre  route_length_perfect_info  route_length_no_info  \
# 0     14644     11                8277.039983           8277.039983   
# 1     26044      4                4175.621525           4297.705371   
# 2     34217     16                7554.509303           9330.027320   
# 3     13901     16                6198.159535           8852.911758   
# 4     28482      7                5180.323377           5179.951343   

#    route_length_10pct  route_length_1pct           std  
# 0        13275.075462       45456.718191  17912.462583  
# 1        38285.880102        4088.586893  17049.503499  
# 2        41258.223118       33256.101990  16969.495510  
# 3        37425.835082       35375.975964  16727.340906  
# 4        38655.748326        5622.009295  16665.461978 

def agent_route_geom(case, case_dict, nodes_dict, vis_agent_id_list):
    agents_list = json.load(open(absolute_path+'/../2_ABM/output/speed_sensor/indiv_agent/agent_routes_random0_probe{}_sigma{}.json'.format(case_dict[case]['probe'], case_dict[case]['sigma'])))
    
    agent_route_geom_list = []
    for a in agents_list:
        if a['agent_id'] in vis_agent_id_list:
            a_route_geom = 'LINESTRING ({})'.format(','.join(['{} {}'.format(nodes_dict[a_n][0], nodes_dict[a_n][1]) for a_n in a['route']]))
            agent_route_geom_list.append({'agent_id': a['agent_id'], 'incre': a['incre'], 'case': case, 'geom': a_route_geom})
        else:
            continue

    agent_route_geom_df = pd.DataFrame(agent_route_geom_list)

    return agent_route_geom_df 

def geom_by_case():

    nodes_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/nodes.csv'.format(folder, scenario))
    nodes_dict = {}
    for n in nodes_df.itertuples():
        nodes_dict[getattr(n, 'node_id_igraph')+1] = (getattr(n, 'lon'), getattr(n, 'lat'))

    case_dict = {'perfect_info': {'sigma': 0, 'probe': 1},
                'no_info': {'sigma': 10, 'probe': 0},
                '10pct': {'sigma': 10, 'probe': 0.1},
                '1pct': {'sigma': 10, 'probe': 0.01}}

    vis_agent_id_list = [14644, 26044, 34217, 13901, 28482]
    agent_route_geom_df = agent_route_geom('perfect_info', case_dict, nodes_dict, vis_agent_id_list)
    for case in ['no_info', '10pct', '1pct']:
        case_df = agent_route_geom(case, case_dict, nodes_dict, vis_agent_id_list)
        agent_route_geom_df = pd.concat([agent_route_geom_df, case_df], ignore_index=True)
    #print(agent_route_geom_df.head())
    #sys.exit(0)

    for vis_agent in vis_agent_id_list:
        agent_route_geom_df[agent_route_geom_df['agent_id']==vis_agent].to_csv(absolute_path+'/vis_agent/vis_agent_{}.csv'.format(vis_agent), index=False)

if __name__ == '__main__':
    #length_by_case()
    #analyze()
    geom_by_case()
