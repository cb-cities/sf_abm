import igraph 
import sys 
import scipy.sparse as sp
import numpy as np 

sf_graph_file = '../data/network_graph.pkl'
g = igraph.Graph.Read_Pickle(sf_graph_file)
print(g.summary())

edgelist = g.get_edgelist()
print(edgelist[0:10])
row = [e[0] for e in edgelist]
col = [e[1] for e in edgelist]
wgh = g.es['sec_length']
print(len(row), len(col), len(wgh))
print(min(row), max(row), min(col), max(col), min(wgh), max(wgh))
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
