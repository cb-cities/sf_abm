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
folder = 'bayarea_osmnx'
scenario = 'undamaged'

### Read the igraph object in
sf_graph_file = absolute_path+'/../data/{}/bayarea_simplified.graphml'.format(folder)
g = igraph.Graph.Read_GraphML(sf_graph_file)
print('Summary of the graph: \n', g.summary())

'''
IGRAPH D--- 224223 549008 -- unnamed
+ attr: crs (g), name (g), simplified (g), streets_per_node (g), highway (v), id (v), osmid (v), ref (v), x (v), y (v), access (e), area (e), bridge (e), est_width (e), geometry (e), highway (e), id (e), junction (e), lanes (e), length (e), maxspeed (e), name (e), oneway (e), osmid (e), ref (e), service (e), tunnel (e), uniqueid (e), width (e)
'''

### Imputing missing data

def str2num(l, outtype):
    l = [l_i.partition(',')[0] for l_i in l]
    l = [l_i.partition('-')[0] for l_i in l]
    l = [l_i.partition(' ')[0] for l_i in l]
    l = [re.sub(r'\D', '', l_i) for l_i in l]
    l_array = np.array(l)
    l_array[l_array==''] = '0'
    return l_array.astype(outtype)

attribute_df = pd.DataFrame({
    'uniqueid': np.array(g.es['uniqueid']).astype(np.int), 
    'osmid': np.array(g.es['osmid']),
    'length': np.array(g.es['length']).astype(np.float),
    'highway': g.es['highway'],
    'lanes': str2num(g.es['lanes'], np.int),
    'maxspeed': str2num(g.es['maxspeed'], np.float)})

### US: lanes
### motorway/motorway_link/trunk/trunk_link: 2 or more
### others: 1
attribute_df.loc[(attribute_df['highway'].isin(['motorway', 'motorway_link', 'trunk', 'trunk_link'])) & (attribute_df['lanes']==0), 'lanes'] = 2
attribute_df.loc[attribute_df['lanes']==0, 'lanes'] = 1
### US: speed
### motorway/motorway_link: 65 mph
### trunk/trunk_link/primary/primary_link: 55 mph
### others: 25 mph
attribute_df.loc[(attribute_df['highway'].isin(['motorway', 'motorway_link'])) & (attribute_df['maxspeed']==0), 'maxspeed'] = 65
attribute_df.loc[(attribute_df['highway'].isin(['trunk', 'truck_link', 'primary', 'primary_link'])) & (attribute_df['maxspeed']==0), 'maxspeed'] = 55
attribute_df.loc[attribute_df['maxspeed']==0, 'maxspeed'] = 25

# add capacity
### Capacity formula from the supplement notes in Colak, Lima and Gonzalez (2016)
def add_capacity(maxspeed_array, lanes_array):
    capacity_array = np.where(maxspeed_array<40, 950*lanes_array, (1500+30*maxspeed_array)*lanes_array)
    capacity_array = np.where(maxspeed_array>=60, (1700+10*maxspeed_array)*lanes_array, capacity_array)
    return capacity_array

attribute_df['capacity'] = add_capacity(attribute_df['maxspeed'], attribute_df['lanes'])

### Create initial weight
attribute_df['fft'] = attribute_df['length']/attribute_df['maxspeed']*2.23694
### 2.23694 is to convert mph to m/s;
### the free flow time should still be calibrated rather than equal to the time at speed limit, check coefficient 1.2 in defining ['weight']
attribute_df['weight'] = attribute_df['fft'] * 1.2 ### According to (Colak, 2015), for SF, even vol=0, t=1.2*fft, maybe traffic light? 1.2 is f_p - k_bay


### Convert to mtx
edgelist = g.get_edgelist()
print(edgelist[0:10])
row, row_x, row_y = [], [], []
col, col_x, col_y = [], [], []
for e in edgelist:
    row.append(e[0])
    row_x.append(g.vs[e[0]]['x'])
    row_y.append(g.vs[e[0]]['y'])
    col.append(e[1])
    col_x.append(g.vs[e[1]]['x'])
    col_y.append(g.vs[e[1]]['y'])
wgh = attribute_df['weight'].tolist()
print(len(row), len(col), len(wgh)) ### 549008 549008 549008
print(min(row), max(row), min(col), max(col), min(wgh), max(wgh)) # 0 224222 0 224222 0.01428062496 3346.709430817758

### Find the information for the longest link
print(np.argmax(wgh)) ### 321231
print(np.argmin(wgh)) ### 152598
#print(attribute_df.iloc[321231])

g_coo = sp.coo_matrix((wgh, (row, col)), shape=(g.vcount(), g.vcount()))
print(g_coo.shape, len(g_coo.data))
#g_csr = sp.csr_matrix(g_coo)
#sp.save_npz('../data/network_sparse.npz', g_csr)
#sio.mmwrite(absolute_path+'/../data/{}/{}/network_sparse.mtx'.format(folder, scenario), g_coo)
# g_coo = sio.mmread(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder))


### Additional Attributes from the graph
attribute_df['start'] = row
attribute_df['end'] = col
attribute_df['start_mtx'] = attribute_df['start'] + 1
attribute_df['end_mtx'] = attribute_df['end'] + 1
attribute_df['start_x'] = row_x
attribute_df['start_y'] = row_y
attribute_df['end_x'] = col_x
attribute_df['end_y'] = col_y
attribute_df.to_csv(absolute_path+'/../data/{}/{}/network_attributes.csv'.format(folder, scenario))
