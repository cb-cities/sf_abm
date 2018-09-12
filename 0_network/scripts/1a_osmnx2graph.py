import osmnx as nx
import pprint
import igraph
import os 

absolute_path = os.path.dirname(os.path.abspath(__file__))

### Bounding box
south, west, north, east = 37.6040,-122.5287,37.8171,-122.3549 # e.g., SF
G = ox.graph_from_bbox(north, south, east, west, network_type='drive')
G = ox.project_graph(G)
pprint.pprint(ox.stats.basic_stats(G))
print('\n')
### {'n': 14059, 'm': 38312, 'k_avg': 5.450174265594993, 'intersection_count': 12586, 'streets_per_node_avg': 3.192261185006046, 'streets_per_node_counts': {0: 0, 1: 1473, 2: 117, 3: 6950, 4: 5294, 5: 206, 6: 16, 7: 3}, 'streets_per_node_proportion': {0: 0.0, 1: 0.10477274343836689, 2: 0.008322071271071912, 3: 0.49434525926452805, 4: 0.3765559428124333, 5: 0.014652535742229177, 6: 0.0011380610285226546, 7: 0.00021338644284799773}, 'edge_length_total': 4884781.274000006, 'edge_length_avg': 127.50003325328893, 'street_length_total': 2920113.3299999963, 'street_length_avg': 129.86939426284172, 'street_segments_count': 22485, 'node_density_km': None, 'intersection_density_km': None, 'edge_density_km': None, 'street_density_km': None, 'circuity_avg': 1.5574379636728192e-05, 'self_loop_proportion': 0.0040979327625809145, 'clean_intersection_count': None, 'clean_intersection_density_km': None}

### Save output
#ox.save_graph_shapefile(G1, filename='osmnx-sf')
ox.save_graphml(G1, filename=absolute_path+'/../data/osmnx-out1.graphml')

### Read back into igraph object
g = igraph.Graph.Read_GraphML(absolute_path+'/../data/osmnx-out1.graphml')
print(g.summary(), '\n')

g.vs['n_x'] = [float(x) for x in g.vs['lon']] ### to float
g.vs['n_y'] = [float(x) for x in g.vs['lat']] ### to float
g.vs['node_index'] = [range(g.vcount())] ### to int
g.vs['node_osmid'] = g.vs['osmid'] ### to str

g.es['edge_index'] = [range(g.ecount())] # to int
g.es['edge_osmid'] = g.es['osmid'] # to int
g.es['start_node'] = [g.vs[g.es[i].source]['node_osmid'] for i in range(g.ecount())] # to str
g.es['end_node'] = [g.vs[g.es[i].target]['node_osmid'] for i in range(g.ecount())] # to str
g.es['type'] = g.es['highway'] # to str
g.es['lane'] = g.es['lanes'] # to float
g.es['maxmph'] = g.es['maxspeed'] # to float
# g.es['capacity'] 
g.es['sec_length'] = [float(x) for x in g.es['length']] # to float

### 'IGRAPH D--- 42838 97601 -- \n+ attr: n_x (v), n_y (v), node_index (v), node_osmid (v), capacity (e), edge_index (e), edge_osmid (e), end_node (e), lane (e), maxmph (e), sec_length (e), start_node (e), type (e)'