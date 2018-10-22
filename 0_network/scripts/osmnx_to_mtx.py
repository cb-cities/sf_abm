import igraph 
import sys 
import scipy.sparse as sp
import scipy.io as sio
import numpy as np 
import json
import time
import os 
import pandas as pd 
import ast 
import re 

absolute_path = os.path.dirname(os.path.abspath(__file__))
folder = 'sf_osmnx'
scenario = 'original'

### Read in the nodes and edges
nodes_data = pd.read_csv(absolute_path + '/../data/{}/sf_simplified_nodes.csv'.format(folder))
edges_data = pd.read_csv(absolute_path + '/../data/{}/sf_simplified_edges.csv'.format(folder))
print(nodes_data.shape, edges_data.shape)
# print(nodes_data.iloc[0])
# print(edges_data.iloc[0])

### Associate traffic signals to edges
print(nodes_data.groupby('highway').count())
signals_data = nodes_data[nodes_data['highway'].isin(['traffic_signals', 'crossing', 'stop', 'psv:traffic_signals'])]
edges_data['signals'] = 0
edges_data.loc[(edges_data['u'].isin(signals_data['osmid']) | edges_data['v'].isin(signals_data['osmid'])), 'signals'] = 1
print(sum(edges_data['signals']))

### Impute missing data
def str2num(s, outtype):
    s = s.partition(',')[0]
    s = s.partition('-')[0]
    s = s.partition(' ')[0]
    n = re.sub(r'\D', '', s)
    if outtype=='int': return int(n)
    if outtype=='float': return float(n)
#print(edges_data['lanes'])
edges_data['lanes_num'] = edges_data['lanes'].fillna(value=0)
edges_data['lanes_num'] = edges_data['lanes_num'].apply(lambda x: str2num(x, 'int') if isinstance(x, str) else x)
edges_data['maxspeed_num'] = edges_data['maxspeed'].fillna(value=0)
edges_data['maxspeed_num'] = edges_data['maxspeed_num'].apply(lambda x: str2num(x, 'float') if isinstance(x, str) else x)

### US: lanes
### motorway/motorway_link/trunk/trunk_link: 2 or more
### others: 1
edges_data.loc[(edges_data['highway'].isin(['motorway', 'motorway_link', 'trunk', 'trunk_link'])) & (edges_data['lanes']==0), 'lanes_num'] = 2
edges_data.loc[edges_data['lanes_num']==0, 'lanes_num'] = 1
### US: speed
### motorway/motorway_link: 65 mph
### trunk/trunk_link/primary/primary_link: 55 mph
### others: 25 mph
edges_data.loc[(edges_data['highway'].isin(['motorway', 'motorway_link'])) & (edges_data['maxspeed_num']==0), 'maxspeed_num'] = 65
edges_data.loc[(edges_data['highway'].isin(['trunk', 'truck_link', 'primary', 'primary_link'])) & (edges_data['maxspeed_num']==0), 'maxspeed_num'] = 55
edges_data.loc[edges_data['maxspeed_num']==0, 'maxspeed_num'] = 25

# add capacity
### Capacity formula from the supplement notes in Colak, Lima and Gonzalez (2016)
def add_capacity(maxspeed_array, lanes_array):
    capacity_array = np.where(maxspeed_array<40, 950*lanes_array, (1500+30*maxspeed_array)*lanes_array)
    capacity_array = np.where(maxspeed_array>=60, (1700+10*maxspeed_array)*lanes_array, capacity_array)
    return capacity_array

edges_data['capacity'] = add_capacity(edges_data['maxspeed_num'], edges_data['lanes_num'])

### Create initial weight
edges_data['fft'] = edges_data['length']/edges_data['maxspeed_num']*2.23694 + edges_data['signals']*15
### 2.23694 is to convert mph to m/s;
edges_data['weight'] = edges_data['fft'] * 1.3 ### (Colak, 2015)


### Convert to mtx
nodes_data['id'] = range(nodes_data.shape[0])
edges_data['osmid_e'] = edges_data['osmid']
edges_data = edges_data.drop(columns=['osmid'])
edges_data = pd.merge(edges_data, nodes_data[['id', 'x', 'y', 'osmid']], how='left', left_on='u', right_on='osmid')
edges_data = pd.merge(edges_data, nodes_data[['id', 'x', 'y', 'osmid']], how='left', left_on='v', right_on='osmid', suffixes=['_u', '_v'])
edges_data = edges_data[['uniqueid', 'u', 'v', 'length', 'osmid_e', 'signals', 'lanes_num', 'maxspeed_num', 'capacity', 'fft', 'weight', 'x_u', 'y_u', 'x_v', 'y_v', 'id_u', 'id_v']]
print(edges_data.iloc[0])

row = edges_data['id_u'].tolist()
col = edges_data['id_v'].tolist()
wgh = edges_data['weight'].tolist()
g_coo = sp.coo_matrix((wgh, (row, col)), shape=(nodes_data.shape[0], nodes_data.shape[0]))
print(g_coo.shape, len(g_coo.data))
sio.mmwrite(absolute_path+'/../data/{}/{}/network_sparse.mtx'.format(folder, scenario), g_coo)
# g_coo = sio.mmread(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder))


### Additional Attributes from the graph
edges_data['sp_id_u'] = edges_data['id_u'] + 1
edges_data['sp_id_v'] = edges_data['id_v'] + 1
edges_data.to_csv(absolute_path+'/../data/{}/{}/network_attributes.csv'.format(folder, scenario))
