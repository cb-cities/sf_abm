#!python3
import pprint
import json
import os
import numpy as np
import scipy.sparse as sp
import scipy.io as sio
import re
import sys
import random
import pandas as pd 
import igraph

# user defined module
import haversine

'''
Code structure:
 * main:
    ** osm_to_json: clean data from .osm by connecting end nodes on the same street (sometimes the same street is split into multiple ways in OSM), removing curve nodes (nodes that only represent link geometry but not meaningful graph nodes), output the converted data into nodes.csv and edges.csv.
        *** populate_attributes: converting OSM json to a uniform list for each edge
        *** merge_by_street: find two edges that are on the same street and connect its nodes
        *** split_at_midpoint: split an edge into two edges if it contains an intersection point

Input:
 * target.osm as downloaded from OSM overpass.

Output:
 * nodes.csv
 * edges.csv

Next step:
 * create graph as a sparse matrix .mtx for ABM simulation
'''

absolute_path = os.path.dirname(os.path.abspath(__file__))
folder='sf_overpass'

def populate_attributes(w):

    w_type = w['tags']['highway']
    # In OSM, the following types of roads are one-way by default:
    drivable_oneway_default = ['motorway', 'motorway_link', 'motorway_junction', 'trunk', 'trunk_link']
    # These roads are two-way by default:
    drivable_twoway_default = ['primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'unclassified', 'unsurfaced', 'residential', 'living_street']
    if w_type not in drivable_oneway_default+drivable_twoway_default:
        return
    if w['nodes'][0]==w['nodes'][-1]:
        return

    ### Keep oneway string, which could be ['yes', 'true', '1'], ['reverse', -1]
    try: 
        w_oneway_str = w['tags']['oneway']
    except KeyError: 
        w_oneway_str = ''
    w_nodes = w['nodes']
    if w_oneway_str in ['reverse', '-1']:
        w_oneway = 'yes'
        w_nodes = w_nodes[::-1]
    elif w_oneway_str in ['yes', 'true', '1']: 
        w_oneway = 'yes'
    else: 
        w_oneway = 'no'

    # set defalt # lanes and speed_limit
    ### Default lanes per direction OSM https://wiki.openstreetmap.org/wiki/Key:lanes
    ### Assumptions section
    ### motorway/motorway_link/trunk/trunk_link: 2 or more (should always be tagged)
    ### others: 1

    ### Default speed limit OSM https://wiki.openstreetmap.org/wiki/OSM_tags_for_routing/Maxspeed#United_States_of_America
    ### motorway/motorway_link: 65 mph
    ### trunk/trunk_link/primary/primary_link: 55 mph
    ### others: 25 mph
    ### free flow speed is roughtly speed_limit/1.3 (Colak, Lima and Gonzalez (2016), https://paper.dropbox.com/doc/Bay-Area-UE-Solver-resources-W9SLplNM8J3ljws9VFZaF)
    if w_type in ('motorway', 'motorway_link'): 
        w_lanes = 2
        w_speed_limit = 65
    elif w_type in ('trunk', 'trunk_link', 'primary', 'primary_link'): 
        w_lanes = 1
        w_speed_limit = 55
    else: 
        w_lanes = 1
        w_speed_limit = 25

    # update lanes when OSM information is available
    if 'lanes' in w['tags'].keys(): w_lanes = int(w['tags']['lanes'])
    # for two-way roads, there might be information for forward lanes and backward lanes
    w_lanesforward = 0
    w_lanesbackward = 0
    if 'lanes:forward' in w['tags'].keys(): w_lanesforward = int(w['tags']['lanes:forward'])
    if 'lanes:backward' in w['tags'].keys(): w_lanesforward = int(w['tags']['lanes:backward'])

    # update speed limit when information is available
    ### "maxspeed": "35 mph;30 mph" --> use the first number in string
    ### all units in mph
    try:
        if 'maxspeed' in w['tags'].keys(): w_speed_limit = int(re.search(r'\d+', w['tags']['maxspeed']).group())
    except AttributeError:
        pass

    try:
        w_name = w['tags']['name']
    except KeyError:
        w_name = ''

    return [w['id'], w_nodes, w_name, w_type, w_oneway, w_lanes, w_lanesforward, w_lanesbackward, w_speed_limit]

def merge_by_street(ways):
    print('Start connecting roads at end points')

    ### create dataframe, each row is a road in OSM
    col_names = ['osmid', 'nodes', 'name', 'type', 'oneway', 'lanes', 'lanesforward', 'lanesbackward', 'speed_limit']
    ways_df = pd.DataFrame(ways, columns = col_names)
    ways_df['osmid'] = ways_df['osmid'].astype(str)
    process_ways_df = ways_df.copy()
    print('Before connecting, edges dataframe shape is {}.'.format(process_ways_df.shape))

    ### Connect two roads if (1) they have the same street name; (2) they have a common node; (3) no other street has this common node.
    ### Not dealing with breaking edges at midnode-midnote or midnode-endnote intersections.
    for clean_round in range(3):
        process_ways_df['s_node'] = process_ways_df['nodes'].apply(lambda x: x[0])
        process_ways_df['e_node'] = process_ways_df['nodes'].apply(lambda x: x[-1])
        ### A list whose row # is twice the # of edges. Each row stores the osmid and the start (or end) node of the edge.
        melt_df = process_ways_df.melt(id_vars = ['osmid', 'name', 'oneway'], value_vars=['s_node', 'e_node'], value_name='end_node')
        ### Groupby end node and count
        end_node_count_df = melt_df.groupby(['end_node']).agg({
            'osmid': lambda x: tuple(x), ### tuple the osmids that share the same node
            'name': lambda x: tuple(x), ### tuple the street names that share the same node
            'oneway': lambda x: tuple(x), ### tuple the oneway sign that share the same node
            'end_node': 'count'}).rename(columns={'end_node': 'count'}).reset_index()
        #print(end_node_count_df.groupby('count').size()) ### node occurance table
        ### filter those nodes that (1) occur twice, (2) shared by the same street.
        osmid_pairs = end_node_count_df.loc[(
            end_node_count_df['count']==2) & (
            end_node_count_df['name'].str[0]==end_node_count_df['name'].str[1]) &
            (end_node_count_df['oneway'].str[0]==end_node_count_df['oneway'].str[1])]['osmid'].values

        processed_osmid = [] ### A list of all the osmids that are merged in this round. An osmid edge should only be merged once per round.
        newly_created = [] ### A list of lists, each sublist contains information of a newly merged way.
        ### For each osmid pair that is identified above 
        for pair in osmid_pairs:
            ### If any of the node in this pair has already been processed in this round, then it's nodes/geometry will change, and should not be processed again.
            if (pair[0] in processed_osmid) or (pair[1] in processed_osmid):
                continue

            ### Retrieve the information for the newly merged road, assuming it inherit the attributes of the first edge in the pair.
            old_way = process_ways_df[process_ways_df['osmid']==pair[0]][col_names].values.tolist()[0]
            ### Get the nodes in both osmid edges and connect them.
            nodes1 = old_way[1]
            nodes2 = process_ways_df[process_ways_df['osmid']==pair[1]]['nodes'].item()
            if old_way[4] == 'yes': ### oneway, need to preserve node order
                if nodes1[0]==nodes2[-1]:
                    new_nodes = nodes2+nodes1[1:]
                elif nodes1[-1]==nodes2[0]:
                    new_nodes = nodes1+nodes2[1:]
                else:
                    continue ### Don't merge, if two one-way edges share the same start or end

            elif old_way[4] == 'no': ### not oneway
                if nodes1[0]==nodes2[0]:
                    new_nodes = nodes1[::-1]+nodes2[1:]
                elif nodes1[-1]==nodes2[-1]:
                    new_nodes = nodes1+nodes2[::-1][1:]
                elif nodes1[0]==nodes2[-1]:
                    new_nodes = nodes2+nodes1[1:]
                elif nodes1[-1]==nodes2[0]:
                    new_nodes = nodes1+nodes2[1:]
                else:
                    print('there is some problem') ### Not sharing end nodes. Should not happen.

            else:
                print('no oneway information')
                continue

            ### Add both osmids in the pair to the processed osmid list
            processed_osmid += list(pair)
            ### Create a combined osmid
            ### Update the nodes with a combined list of nodes.
            new_way = ['{}-{}'.format(pair[0], pair[1]), new_nodes] + old_way[2:]
            newly_created.append(new_way)

        ### At the end of each round, concatenate the newly created edges with edges that have not been processed.
        process_ways_df = pd.concat([process_ways_df.loc[~process_ways_df['osmid'].isin(processed_osmid)][col_names],
            pd.DataFrame(newly_created, columns=col_names)], ignore_index=True)

        print('{} new edges created by merging at end points. Edges dataframe shape is {}'.format(len(newly_created), process_ways_df.shape))

    return process_ways_df

def split_at_midpoint(ways_df):
    # critieria for filtering out curve nodes, but preserve intersections:
    # 1. all end nodes are preserved
    # 2. nodes are preserved if it repeats as mid nodes.
    # *. mid-end intersections are preserved as end nodes.
    # The final set of nodes is the the union of results from the above two criteria
    # Find duplicates: https://stackoverflow.com/a/9835819
    ways_df['lanes'] = np.where(ways_df['lanesforward']>0, ways_df['lanesforward'], ways_df['lanes'])
    all_nodes = ways_df['nodes'].values.tolist()
    mid_nodes_l = [n for sublist in all_nodes for n in sublist[1:-1]] # all nodes with duplicates
    end_nodes_l = [sublist[0] for sublist in all_nodes] + [sublist[-1] for sublist in all_nodes]

    seen = set()
    dupes = set()
    for n in mid_nodes_l:
        if n not in seen:
            seen.add(n)
        else:
            dupes.add(n)
    intersection_nodes = set(end_nodes_l).union(dupes)
    print(len(all_nodes), len(set(end_nodes_l)), len(seen), len(dupes), len(intersection_nodes))

    # Remove curve nodes by only keeping intersection nodes
    split_ways = []
    for way in ways_df.itertuples():
        w_osmid = getattr(way, 'osmid')
        w_nodes = getattr(way, 'nodes')
        w_oneway = getattr(way, 'oneway')
        w_lanes = getattr(way, 'lanes')
        nid_in_way = [i for i in range(len(w_nodes)) if w_nodes[i] in intersection_nodes]
        for start, end in zip(nid_in_way, nid_in_way[1:]):
            split_ways.append([w_osmid, w_nodes[start:end+1], w_lanes])
            if w_oneway=='no':
                if getattr(way, 'lanesbackward')>0: w_lanes = getattr(way, 'lanesbackward')
                split_ways.append([w_osmid, w_nodes[start:end+1][::-1], w_lanes])
    split_ways_df = pd.DataFrame(split_ways, columns = ['osmid', 'split_nodes', 'split_lanes'])
    split_ways_df = pd.merge(split_ways_df, ways_df, on='osmid', how='left')
    split_ways_df = split_ways_df[['osmid', 'split_nodes', 'split_lanes', 'name', 'type', 'oneway', 'lanesforward', 'lanesbackward', 'speed_limit']]
    print('Shape before split: {}, after split: {}'.format(ways_df.shape, split_ways_df.shape))
    return intersection_nodes, split_ways_df


def osm_to_json(output_csv=False, folder = 'sf'):
    ### Clean the OSM data by removing curve nodes, separate into nodes and ways, output .json (for further processing) and .geosjon (for visualisation).

    # Load OSM data as downloaded from overpass
    osm_data = json.load(open(absolute_path+'/../data/{}/target.osm'.format(folder)))
    osm_data = osm_data['elements']
    print('length of the OSM data: ', len(osm_data))

    # Based on the OSM data, make a dictionary of node element: all_nodes = {osm_node_id:(lat,lon), ...}
    # also include any highway tags, such as traffic signals
    all_nodes = {}
    for n in osm_data:
        if n['type']=='node':
            try:
                all_nodes[n['id']] = (n['lat'], n['lon'], n['tags']['highway'])
            except KeyError:
                all_nodes[n['id']] = (n['lat'], n['lon'], '')
    print('it includes {} nodes'.format(len(all_nodes)))
    #random_key = random.choice(list(all_nodes))
    #print('example, {}: {}'.format(random_key, all_nodes[random_key]))

    ### Get drivable roads
    all_ways = [w_e for w_e in osm_data if (w_e['type']=='way')]
    drivable_ways = [populate_attributes(w_e) for w_e in all_ways]
    drivable_ways = [w_e for w_e in drivable_ways if w_e is not None]
    print('it includes {} drivable ways'.format(len(drivable_ways)))
    drivable_ways = merge_by_street(drivable_ways) ### From list of lists to DataFrame
    intersection_nodes, split_ways = split_at_midpoint(drivable_ways)

    ### Add geometry and length
    split_ways['geometry'] = split_ways.apply(lambda way: 'LINESTRING ({})'.format(','.join(['{} {}'.format(all_nodes[n][1], all_nodes[n][0]) for n in way['split_nodes']])), axis=1)
    split_ways['length'] = split_ways.apply(lambda way: 
        max(0.1, sum([haversine.haversine(
            all_nodes[x][0], all_nodes[x][1], all_nodes[y][0], all_nodes[y][1]) for (x,y) in zip(way['split_nodes'], way['split_nodes'][1:])])), 
        axis=1)
    #split_ways['edge_id'] = list(range(split_ways.shape[0]))
    split_ways['start_node'] = split_ways.apply(lambda way: way['split_nodes'][0], axis=1)
    split_ways['end_node'] = split_ways.apply(lambda way: way['split_nodes'][-1], axis=1)
    
    # add capacity
    split_ways['capacity'] = np.where(
        split_ways['speed_limit']<40, 950*split_ways['split_lanes'], ### speed limit < 40mph
        np.where(split_ways['speed_limit']<60, 
            (1500+30*split_ways['speed_limit'])*split_ways['split_lanes'], ### speed limit btw 40-60mph
            (1700+10*split_ways['speed_limit'])*split_ways['split_lanes'])) ### speed limit > 60mph

    ### Organize
    intersection_nodes_df = pd.DataFrame([
        (n, all_nodes[n][1], all_nodes[n][0], all_nodes[n][2]) for n in intersection_nodes], columns=['osmid', 'lon', 'lat', 'signal'])
    split_ways = split_ways.rename(columns={'split_lanes': 'lanes'})
    split_ways[['osmid', 'start_node', 'end_node', 'type', 'length', 'lanes', 'oneway', 'speed_limit', 'capacity', 'geometry']]

    if output_csv:
        intersection_nodes_df.to_csv(absolute_path+'/../data/{}/nodes.csv'.format(folder), index=False)
        split_ways.to_csv(absolute_path+'/../data/{}/edges.csv'.format(folder), index=False)

    return intersection_nodes_df, split_ways

def graph_simplify(nodes_df, edges_df):

    ### Construct the graph nodes from nodes.json
    node_data = nodes_df.to_dict('records')
    print(node_data[0])

    ### Construct the graph edges
    edge_data = edges_df.to_dict('records')
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
            oneway="first", type="first", lanes="sum", speed_limit="mean", capacity="sum", length="max"))
    print('Simplify loops and multiple edges: ', g.summary()) ### IGRAPH D--- 9643 26893 -- 
    g.vs.select(_degree=0).delete()
    print('Remove degree 0 vertices: ', g.summary()) ### IGRAPH D--- 9643 26893 -- 
    
    g.vs['node_id_igraph'] = list(range(g.vcount()))
    g.es['edge_id_igraph'] = list(range(g.ecount()))

    return g

def convert_to_mtx(g, folder, scenario):

    ### Create initial weight
    g.es['fft'] = np.array(g.es['length'], dtype=np.float)/np.array(g.es['speed_limit'], dtype=np.float)*2.23694 * 1.3
    ### 2.23694 is to convert mph to m/s;
    g.es['weight'] = np.array(g.es['fft'], dtype=np.float)# * 1.5

    ### Convert to mtx
    edgelist = g.get_edgelist()
    print(edgelist[0:10])
    row = [e[0] for e in edgelist]
    col = [e[1] for e in edgelist]
    wgh = g.es['weight']
    print(len(row), len(col), len(wgh))
    print(min(row), max(row), min(col), max(col), min(wgh), max(wgh))

    g_coo = sp.coo_matrix((wgh, (row, col)), shape=(g.vcount(), g.vcount()))
    print(g_coo.shape, len(g_coo.data))
    sio.mmwrite(absolute_path+'/../data/{}/{}/network_sparse.mtx'.format(folder, scenario), g_coo)
    # g_coo = sio.mmread(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder))

    ### Node attributes
    node_attributes_df = pd.DataFrame({
        'node_id_igraph': g.vs['node_id_igraph'],
        'node_osmid': g.vs['osmid'],
        'lon': g.vs['lon'],
        'lat': g.vs['lat'],
        'signal': g.vs['signal']
        })
    node_attributes_df.to_csv(absolute_path+'/../data/{}/{}/nodes.csv'.format(folder, scenario), index=False)


    ### Edge attributes
    # ['osmid', 'start_node', 'end_node', 'type', 'length', 'lanes', 'oneway', 'speed_limit', 'capacity', 'geometry']
    edge_attributes_df = pd.DataFrame({
        'edge_id_igraph': g.es['edge_id_igraph'],
        'start_igraph': row, 
        'end_igraph': col, 
        'edge_osmid': g.es['osmid'],
        'start_osm': g.es['start_node'],
        'end_osm': g.es['end_node'],
        'length': g.es['length'], 
        'lanes': g.es['lanes'],
        'maxmph': g.es['speed_limit'], 
        'oneway': g.es['oneway'],
        'type': g.es['type'],
        'capacity': g.es['capacity'],
        'fft': g.es['fft'],
        'geometry': g.es['geometry']})
    edge_attributes_df['start_sp'] = edge_attributes_df['start_igraph'] + 1
    edge_attributes_df['end_sp'] = edge_attributes_df['end_igraph'] + 1
    edge_attributes_df.to_csv(absolute_path+'/../data/{}/{}/edges.csv'.format(folder, scenario), index=False)


if __name__ == '__main__':
    nodes_df, edges_df = osm_to_json(output_csv=False, folder = folder)
    g = graph_simplify(nodes_df, edges_df)
    convert_to_mtx(g, folder=folder, scenario = 'original')


