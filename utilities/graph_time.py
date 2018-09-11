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
graph_file = absolute_path+'/../0_network/data/sf/network_sparse.mtx'

### Time the PQ
print('########### Priority Queue SP #############')
g_pq = interface.readgraph(bytes(graph_file, encoding='utf-8'))
ta0 = time.time()
try:
	sp_pq = g_pq.dijkstra(1, 20)
except:
	print('route not found')
ta1 = time.time()
route_pq = sp_pq.route(20)
ta2 = time.time()
path_pq = [vertex[1] for vertex in route_pq]
ta3 = time.time()
print('PQ: distance 1020-->20: ', sp_pq.distance(20))
print('PQ: total time {}, dijkstra() {}, route() {}, vertex list {}, \n'.format(ta3-ta0, ta1-ta0, ta2-ta1, ta3-ta2))

# Time igraph
print('############## igraph ################')
g_csr = sio.mmread(graph_file)
g_coo = scipy.sparse.coo_matrix(g_csr)
source, target = g_coo.nonzero()
edgelist = list(zip(source.tolist(), target.tolist()))
g_igraph = igraph.Graph(max(g_coo.shape), edgelist, edge_attrs={'weight': g_coo.data.tolist()}, directed=True)
print(g_igraph.summary())

distance_igraph = g_igraph.shortest_paths_dijkstra(1019, 19, weights='weight')
tb0 = time.time()
path_igraph = g_igraph.get_shortest_paths(1019, 19, weights='weight', output='vpath')
tb1 = time.time()
print('igraph ODSP: 1019-->19 distance {}, running time {}: \n'.format(distance_igraph[0][0], tb1-tb0))

tb2 = time.time()
path_igraph_sssp = g_igraph.get_shortest_paths(1019, weights='weight', output='epath')
tb3 = time.time()
print('igraph SSSP at origin 1019: ', tb3-tb2)



