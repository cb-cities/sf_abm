### Convert the imputed weekday/weekend & hourly specific link-level travel time file to graph objects.
import json
import sys
import haversine
import igraph
import os

absolute_path = os.path.dirname(os.path.abspath(__file__))

### Construct the graph nodes from nodes.json
node_json = json.load(open(absolute_path+'/../data/nodes.json'))
print(len(node_json))
node_index = 0
node_data = []
for n, cor in node_json.items():
    node_element = {
    'node_osmid': n,
    'node_index': node_index,
    'n_x': round(cor[1],6),
    'n_y': round(cor[0],6)}
    node_data.append(node_element)
    node_index += 1

### Construct the graph edges
link_json = json.load(open(absolute_path+'/../data/links.json'))
print(len(link_json))
edge_index = 0
edge_data = []
nodes_in_edge_set = set()
for link_id, link_values in link_json.items():
    if not isinstance(link_values['speed_limit'], int):
        print(link_values)
        sys.exit(0)
    for section_ct in range(len(link_values['sections'])):
        edge_element = {
        'edge_osmid': link_id,
        'edge_index': edge_index,
        'start_node': link_values['sections'][section_ct][0],
        'end_node': link_values['sections'][section_ct][1],
        'speed_limit': link_values['speed_limit'],
        'sec_length': link_values['length'][section_ct]
        }
        nodes_in_edge_set.add(edge_element['start_node'])
        nodes_in_edge_set.add(edge_element['end_node'])
        edge_data.append(edge_element)
        edge_index += 1

### Check if all nodes in the edge dataset are contained in the provided nodes dataset
print(nodes_in_edge_set.issubset(set([*node_json])))

### Construct the graph object
g = igraph.Graph.DictList(
    vertices=node_data,
    edges=edge_data, 
    vertex_name_attr='node_osmid',
    edge_foreign_keys=('start_node','end_node'),
    directed=True)
print(igraph.summary(g))
print(g.vs[0])
#sys.exit(0)
# print(g.es.find(edge_osmid='101554764'))
# route_a = g.get_shortest_paths(
#     g.vs.find(node_osmid='1172644728'),
#     g.vs.find(node_osmid='1172712808'),output="epath")
# print(route_a)
#g.write_graphmlz(absolute_path+'/../data/network_graph.graphmlz')
g.write_pickle(absolute_path+'/../data/network_graph.pkl') ### Save as pkl for preserving coordinate precision
# g = igraph.load('Collected_data_False14.graphmlz')


