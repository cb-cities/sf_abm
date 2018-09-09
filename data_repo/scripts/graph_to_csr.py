import igraph 
import sys 
import scipy.sparse as sp
import numpy as np 
import json
import time

sf_graph_file = '../data/network_graph.pkl'
g = igraph.Graph.Read_Pickle(sf_graph_file)
print(g.summary())

t0 = time.time()
path_collection = g.get_shortest_paths(1, weights='sec_length', output='epath')
t1 = time.time()
print(t1-t0)
sys.exit(0)

# IGRAPH D--- 83335 149318 -- 
# + attr: n_x (v), n_y (v), node_index (v), node_osmid (v), edge_index (e), edge_osmid (e), end_node (e), sec_length (e), speed_limit (e), start_node (e)

# node_osmid_list = g.vs['node_osmid']
# node_osmid2graphid = {node_osmid_list[i]: i for i in range(len(node_osmid_list))}
# with open('../data/node_osmid2graphid.json', 'w') as outfile:
#     json.dump(node_osmid2graphid, outfile, indent=2)
# sys.exit(0)

edgelist = g.get_edgelist()
print(edgelist[0:10])
row = [e[0] for e in edgelist]
col = [e[1] for e in edgelist]
wgh = g.es['sec_length']
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
print(g_coo.shape)
g_csr = sp.csr_matrix(g_coo)
sp.save_npz('../data/network_sparse.npz', g_csr)
