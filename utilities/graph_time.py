import igraph 
import sys 
import scipy.sparse
import scipy.io as sio
import numpy as np 
import time
import os

sys.path.insert(0, '/Users/bz247')
from sp import interface 

absolute_path = os.path.dirname(os.path.abspath(__file__))
graph_file = absolute_path+'/../data_repo/data/sf/network_sparse.mtx'
g = interface.readgraph(bytes(graph_file, encoding='utf-8'))
ta0 = time.time()
sp = g.dijkstra(1020)
ta1 = time.time()
route = sp.route(20)
ta2 = time.time()
#print( " -> ".join("%s"%vertex[1] for vertex in route) )
vpath = [vertex[1] for vertex in route]
ta3 = time.time()
print(ta1-ta0, ta2-ta1, ta3-ta2)

g_csr = sio.mmread(graph_file)
g_coo = scipy.sparse.coo_matrix(g_csr)
source, target = g_coo.nonzero()
edgelist = list(zip(source.tolist(), target.tolist()))
g = igraph.Graph(max(g_coo.shape), edgelist, edge_attrs={'weight': g_coo.data.tolist()}, directed=True)
#print(g.summary())

tb0 = time.time()
path_collection = g.get_shortest_paths(1019, weights='weight', output='epath')
tb1 = time.time()
print(tb1-tb0)



