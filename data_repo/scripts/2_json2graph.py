### Convert the imputed weekday/weekend & hourly specific link-level travel time file to graph objects.
import json
import sys
import haversine
import igraph
import os

absolute_path = os.path.dirname(os.path.abspath(__file__))
folder = 'sf'

### Construct the graph nodes from nodes.json
nodes_json = json.load(open(absolute_path+'/../data/{}/nodes.json'.format(folder)))
print('number of nodes: ', len(nodes_json))
node_index = 0
node_data = []
for n, cor in nodes_json.items():
    node_element = {
    'node_osmid': n,
    'node_index': node_index,
    'n_x': cor[1],
    'n_y': cor[0]}
    node_data.append(node_element)
    node_index += 1

### Construct the graph edges
ways_json = json.load(open(absolute_path+'/../data/{}/ways.json'.format(folder)))
print('number of edges: ', len(ways_json))
edge_index = 0
edge_data = []
edge_nodes_set = set()
for way in ways_json:
    for section_ct in range(len(way['nodes'])-1):
        edge_element = {
        'edge_osmid': way['osmid'],
        'edge_index': edge_index,
        'start_node': str(way['nodes'][section_ct]),
        'end_node': str(way['nodes'][section_ct+1]),
        'type': way['type'],
        'lane': way['lanes'],
        'maxmph': way['maxmph'],
        'capacity': way['capacity'],
        'sec_length': way['length'][section_ct]
        }
        edge_nodes_set.add(edge_element['start_node'])
        edge_nodes_set.add(edge_element['end_node'])
        edge_data.append(edge_element)
        edge_index += 1

### Check if all nodes in the edge dataset are contained in the provided nodes dataset
print('Are all nodes in edges in nodes.json: ', edge_nodes_set.issubset(set([*nodes_json])))

### Construct the graph object
g = igraph.Graph.DictList(
    vertices=node_data,
    edges=edge_data, 
    vertex_name_attr='node_osmid',
    edge_foreign_keys=('start_node','end_node'),
    directed=True)
print(g.summary())
g.simplify(multiple=True, loops=True, 
    combine_edges=dict(
        edge_osmid="first", edge_index="first", start_node="first", end_node="first",
        type="first", lane="sum", maxmph="mean", capacity="sum", sec_length="max"))
print(g.summary())

g.write_pickle(absolute_path+'/../data/{}/network_graph.pkl'.format(folder)) ### Save as pkl for preserving coordinate precision

node_osmid2graphid_dict = dict(zip(g.vs['node_osmid'], range(g.vcount())))
with open(absolute_path+'/../data/{}/node_osmid2graphid.json'.format(folder), 'w') as outfile:
    json.dump(node_osmid2graphid_dict, outfile, indent=2)

