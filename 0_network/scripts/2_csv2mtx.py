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

def simplify_graph(folder):
    ### Construct the graph nodes from nodes.json
    nodes_csv = pd.read_csv(absolute_path+'/../data/{}/nodes.csv'.format(folder))
    node_data = nodes_csv.to_dict('records')
    print(node_data[0])

    ### Construct the graph edges
    edges_csv = pd.read_csv(absolute_path+'/../data/{}/edges.csv'.format(folder))
    edge_data = edges_csv.to_dict('records')
    print(edge_data[0])

    ### Construct the graph object
    g = igraph.Graph.DictList(
        vertices=node_data,
        edges=edge_data, 
        vertex_name_attr='osmid',
        edge_foreign_keys=('start_node','end_node'),
        directed=True)
    print(g.summary())
    ### IGRAPH D--- 9781 27166 -- 
    ### + attr: lat (v), lon (v), osmid (v), signal (v), capacity (e), end_node (e), geometry (e), lanes (e), length (e), oneway (e), osmid (e), speed_limit (e), start_node (e), type (e)
    
    g = g.clusters(mode='STRONG').giant() ### STRONGLY connected components. All nodes are connected as a directed graph. Weak components mean components are connected as undirected graph.
    print('Giant component: ', g.summary()) ### IGRAPH D--- 9643 26973 -- 
    g.simplify(multiple=True, loops=True, 
        combine_edges=dict(
            osmid="first", start_node="first", end_node="first", geometry="first",
            oneway="first", type="first", lane="sum", speed_limit="mean", capacity="sum", length="max"))
    print('Simplify loops and multiple edges: ', g.summary()) ### IGRAPH D--- 9643 26893 -- 
    g.vs.select(_degree=0).delete()
    print('Remove degree 0 vertices: ', g.summary()) ### IGRAPH D--- 9643 26893 -- 
    g.es['edge_index'] = list(range(g.ecount()))

    return g

def convert_to_mtx(g):
    ### Create initial weight
    g.es['fft'] = np.array(g.es['length'], dtype=np.float)/np.array(g.es['speed_limit'], dtype=np.float)*2.23694 * 1.3
    ### 2.23694 is to convert mph to m/s;
    g.es['weight'] = np.array(g.es['fft'], dtype=np.float) * 1.5

    ### Convert to mtx
    edgelist = g.get_edgelist()
    print(edgelist[0:10])
    row = [e[0] for e in edgelist]
    col = [e[1] for e in edgelist]
    wgh = g.es['weight']
    print(len(row), len(col), len(wgh)) ### 149318 149318 149318
    print(min(row), max(row), min(col), max(col), min(wgh), max(wgh)) # 0 83334 0 83334 0.1 3118.0905577608523

    g_coo = sp.coo_matrix((wgh, (row, col)), shape=(g.vcount(), g.vcount()))
    print(g_coo.shape, len(g_coo.data))
    sio.mmwrite(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder), g_coo)
    # g_coo = sio.mmread(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder))

    ### Additional Attributes from the graph
    network_attributes_df = pd.DataFrame({
        'start_igraph': row, 
        'end_igraph': col, 
        'start_osm': g.es['start_node'],
        'end_osm': g.es['end_node'],
        'edge_id_igraph': g.es['edge_index'],
        'edge_osmid': g.es['osmid'],
        'length': g.es['length'], 
        'maxmph': g.es['speed_limit'], 
        'oneway': g.es['oneway'],
        'type': g.es['type'],
        'capacity': g.es['capacity'],
        'fft': g.es['fft'],
        'geometry': g.es['geometry']})
    network_attributes_df['start_sp'] = network_attributes_df['start_igraph'] + 1
    network_attributes_df['end_sp'] = network_attributes_df['end_igraph'] + 1
    network_attributes_df.to_csv(absolute_path+'/../data/{}/network_attributes.csv'.format(folder))

if __name__ == '__main__':
    folder = 'sf_overpass'
    g = simplify_graph(folder=folder)
    convert_to_mtx(g)
