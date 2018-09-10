import igraph 
import sys 
import scipy.sparse as sp
import numpy as np 
import time

g_csr = sp.load_npz('../data/network_sparse.npz')
g_coo = sp.coo_matrix(g_csr)
source, target = g_coo.nonzero()
edgelist = list(zip(source.tolist(), target.tolist()))
g = igraph.Graph(max(g_coo.shape), edgelist, edge_attrs={'weight': g_coo.data.tolist()}, directed=True)
print(g.summary())

t0 = time.time()
path_collection = g.get_shortest_paths(0, weights='weight', output='epath')
t1 = time.time()
print(t1-t0)



