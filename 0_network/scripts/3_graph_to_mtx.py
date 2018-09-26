import igraph 
import sys 
import scipy.sparse as sp
import scipy.io as sio
import numpy as np 
import json
import time
import os 
import pandas as pd 

absolute_path = os.path.dirname(os.path.abspath(__file__))
folder = 'sf'

### Read the igraph object in
sf_graph_file = absolute_path+'/../data/{}/network_graph.pkl'.format(folder)
g = igraph.Graph.Read_Pickle(sf_graph_file)
print('Summary of the graph: \n', g.summary())

# t0 = time.time()
# path_collection = g.get_shortest_paths(1, weights='sec_length', output='epath')
# t1 = time.time()
# print(t1-t0)
# sys.exit(0)

# IGRAPH D--- 83335 149318 -- 
# + attr: n_x (v), n_y (v), node_index (v), node_osmid (v), edge_index (e), edge_osmid (e), end_node (e), sec_length (e), speed_limit (e), start_node (e)

# node_osmid_list = g.vs['node_osmid']
# node_osmid2graphid = {node_osmid_list[i]: i for i in range(len(node_osmid_list))}
# with open('../data/node_osmid2graphid.json', 'w') as outfile:
#     json.dump(node_osmid2graphid, outfile, indent=2)
# sys.exit(0)

### Create initial weight
g.es['fft'] = np.array(g.es['sec_length'], dtype=np.float)/np.array(g.es['maxmph'], dtype=np.float)*2.23694
fft_array = np.array(g.es['fft'], dtype=np.float)
capacity_array = np.array(g.es['capacity'], dtype=np.float)
### 2.23694 is to convert mph to m/s;
### the free flow time should still be calibrated rather than equal to the time at speed limit, check coefficient 1.2 in defining ['weight']
g.es['weight'] = fft_array * 1.2 ### According to (Colak, 2015), for SF, even vol=0, t=1.2*fft, maybe traffic light? 1.2 is f_p - k_bay


### Convert to mtx
edgelist = g.get_edgelist()
print(edgelist[0:10])
row = [e[0] for e in edgelist]
col = [e[1] for e in edgelist]
wgh = g.es['weight']
print(len(row), len(col), len(wgh)) ### 149318 149318 149318
print(min(row), max(row), min(col), max(col), min(wgh), max(wgh)) # 0 83334 0 83334 0.1 3118.0905577608523

### Find the information for the longest link
# print(np.argmax(wgh)) ### Return is 4570
# print(g.es[4570])   ### sec_length is 3118.09, start osm = 645557712, end osm = 1895821104
# print(g.vs.select(node_osmid_eq = '645557712').indices) ### indices is 18063
# print(g.vs[18063])
# print(g.vs.select(node_osmid_eq = '1895821104').indices) ### indices is 72851
# print(g.vs[72851])
### It's the east side of the bay bridge
# sys.exit(0)

g_coo = sp.coo_matrix((wgh, (row, col)), shape=(g.vcount(), g.vcount()))
print(g_coo.shape, len(g_coo.data))
#g_csr = sp.csr_matrix(g_coo)
#sp.save_npz('../data/network_sparse.npz', g_csr)
sio.mmwrite(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder), g_coo)
# g_coo = sio.mmread(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder))


### Additional Attributes from the graph
network_attributes_df = pd.DataFrame({'start': row, 'end': col, 'sec_length': g.es['sec_length'], 'maxmph': g.es['maxmph'], 'capacity': g.es['capacity']})
network_attributes_df['start_mtx'] = network_attributes_df['start'] + 1
network_attributes_df['end_mtx'] = network_attributes_df['end'] + 1
network_attributes_df.to_csv(absolute_path+'/../data/{}/network_attributes.csv'.format(folder))