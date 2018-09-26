#!python3
import pprint
import json
import os
import numpy as np
import re
import sys
import random

# user defined module
import haversine

'''
Code structure:
 * main:
    ** osm_to_geojson: convert data from .osm to .geojson for visualization.
    ** osm_to_json: clean data from .osm by removing curve nodes (nodes that only represent link geometry but not meaningful graph nodes), output the converted data into nodes.json and ways.json, with the option to output the .geojson format as well.
        *** create_way: (called by osm_to_json) handle one OSM way element by removing curve nodes, add default or OSM-provided information of number of lanes, maximum speed (mph) and capacity.

Input:
 * target.osm as downloaded from OSM overpass.

Output:
 * osm_nodes.geojson: OSM node elements in .geojson format.
 * osm_ways.geojson: OSM way elements in .geojson format.
 * nodes.json: nodes that we will keep to build the road network graph.
 * ways.json: ways that we will keep to build the road network graph.
 * converted_nodes.geojson: same as nodes.json but in .geojson format for easy visualisation.
 * converted_ways.geojson: same as ways.json but in .geojson format for easy visualisation.

Next step:
 * gather the outputs and run `2_json2graph.py` to create python-igraph object.
'''

def osm_to_geojson(folder='sf'):
    ### converts OSM data to geojson format
    ### this is mainly for visualization as geojson format can be easily imported to QGIS

    # Load OSM data as downloaded from overpass
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    osm_data = json.load(open(absolute_path+'/../data/{}/target.osm'.format(folder)))
    osm_data = osm_data['elements']
    print('length of the OSM data: ', len(osm_data))

    # Based on the OSM data, make a dictionary of node elements: all_nodes = {osm_node_id:(lat,lon), ...}
    all_nodes = {n['id']: (n['lat'], n['lon']) for n in osm_data if n['type']=='node'}
    print('it includes {} nodes'.format(len(all_nodes)))

    # Make a list of way elements
    all_ways = [w_e for w_e in osm_data if w_e['type']=='way']
    print('it includes {} nodes'.format(len(all_ways)))

    nodes_feature_list = [] ### OSM node element to "Point" feature in geojson format
    for k, v in all_nodes.items():
        node_feature = {
            'type': 'Feature', 
            'geometry': {'type': 'Point', 'coordinates': [v[1], v[0]]},
            'properties': {'osmid': k}
            }
        nodes_feature_list.append(node_feature)
    nodes_geojson = {'type': 'FeatureCollection', 'features': nodes_feature_list}

    links_feature_list = [] ### OSM way element to "LineString" feature in geojson format
    for w in all_ways:
        link_feature = {
            'type': 'Feature', 
            'geometry': {
                'type': 'LineString', 
                'coordinates': [[all_nodes[n][1], all_nodes[n][0]] for n in w['nodes']]
                },
            'properties': {
                'osmid': w['id'],
                'type': w['tags']['highway']
                }
            }
        links_feature_list.append(link_feature)
    links_geojson = {'type': 'FeatureCollection', 'features': links_feature_list}

    # Save output
    with open(absolute_path+'/../data/{}/osm_ways.geojson'.format(folder), 'w') as links_outfile:
        json.dump(links_geojson, links_outfile, indent=2)

    with open(absolute_path+'/../data/{}/osm_nodes.geojson'.format(folder), 'w') as nodes_outfile:
        json.dump(nodes_geojson, nodes_outfile, indent=2)


def create_way(w, intersection_nodes, oneway_str, reverse):
    ### Process one "way" element in OSM
    ### Remove curve nodes, add lanes, speed limit and capacity

    # Remove curve nodes by only keeping intersection nodes
    nodes_in_way = [n for n in w['nodes'] if n in intersection_nodes]
    nid_in_way = [i for i in range(len(w['nodes'])) if w['nodes'][i] in nodes_in_way]
    length_in_way = [round(sum(w['length'][x:y]),2) for (x,y) in zip(nid_in_way, nid_in_way[1:])]
    if reverse:
        nodes_in_way = nodes_in_way[::-1]
        length_in_way = length_in_way[::-1]

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
    w_type = w['tags']['highway']
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
    if ('lanes:forward' in w['tags'].keys()) and (oneway_str == 'nf'): w_lane = int(w['tags']['lanes:forward'])
    if ('lanes:backward' in w['tags'].keys()) and (oneway_str == 'nb'): w_lane = int(w['tags']['lanes:backward'])

    # update speed limit when information is available
    ### "maxspeed": "35 mph;30 mph" --> use the first number in string
    ### all units in mph
    try:
        if 'maxspeed' in w['tags'].keys(): w_speed_limit = int(re.search(r'\d+', w['tags']['maxspeed']).group())
    except AttributeError:
        pass
    # add capacity
    ### Capacity formula from the supplement notes in Colak, Lima and Gonzalez (2016)
    if w_speed_limit < 40:
        w_capacity = 950 * w_lanes
    elif (w_speed_limit < 60) and (w_speed_limit >= 40):
        w_capacity = (1500+30*w_speed_limit) * w_lanes
    else:
        w_capacity = (1700+10*w_speed_limit) * w_lanes

    # return a cleaned way element
    way = {
        'osmid': w['id'], 
        'oneway': oneway_str, 
        'type': w['tags']['highway'], 
        'lanes': w_lanes,
        'maxmph': w_speed_limit,
        'capacity': w_capacity,
        'nodes': nodes_in_way, 
        'length': length_in_way}

    return way, nodes_in_way


def osm_to_json(output_geojson=False, folder = 'sf'):
    ### Clean the OSM data by removing curve nodes, separate into nodes and ways, output .json (for further processing) and .geosjon (for visualisation).

    # Load OSM data as downloaded from overpass
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    osm_data = json.load(open(absolute_path+'/../data/{}/target.osm'.format(folder)))
    osm_data = osm_data['elements']
    print('length of the OSM data: ', len(osm_data))

    # Based on the OSM data, make a dictionary of node element: all_nodes = {osm_node_id:(lat,lon), ...}
    all_nodes = {n['id']: (n['lat'], n['lon']) for n in osm_data if n['type']=='node'}
    print('it includes {} nodes'.format(len(all_nodes)))
    random_key = random.choice(list(all_nodes))
    print('example, {}: {}'.format(random_key, all_nodes[random_key]))

    # Make a list of way elements
    all_ways = [w_e for w_e in osm_data if w_e['type']=='way']
    print('it includes {} nodes'.format(len(all_ways)))
    end_nodes_l = [] # list holding all end nodes of the way elements. All will be preserved.
    mid_nodes_l = [] # list holding all mid nodes of the way elements. Some will be discarded as curve nodes.
    ###################### lecture break ############################
    for way in all_ways:
        way_nodes = way['nodes']
        end_nodes_l.append(way_nodes[0])
        end_nodes_l.append(way_nodes[-1])
        mid_nodes_l += way_nodes[1:-1]
        # Use harversine formula to calculate the length between two nodes. Set length as 0.1 if calculated distance is smaller
        # some nodes will be cleaned as they define curves rather than intersections. However, the length between two nodes will contribute to the final total length
        way['length'] = [max(0.1, haversine.haversine(all_nodes[x][0], all_nodes[x][1], all_nodes[y][0], all_nodes[y][1])) for (x,y) in zip(way_nodes, way_nodes[1:])]
        #pprint.pprint(way)
        #sys.exit(0)

    # critieria for filtering out curve nodes, but preserve intersections:
    # 1. all end nodes are preserved
    # 2. nodes are preserved if it appears twice or more in mid_nodes_l + end_nodes_l
    # The final set of nodes is the the union of results from the above two criteria
    # Find duplicates: https://stackoverflow.com/a/9835819
    all_nodes_l = end_nodes_l + mid_nodes_l # all nodes with duplicates
    seen = set()
    dupes = set()
    for n in all_nodes_l:
        if n not in seen:
            seen.add(n)
        else:
            dupes.add(n)
    intersection_nodes = set(end_nodes_l).union(dupes)
    print(len(all_nodes_l), len(set(end_nodes_l)), len(seen), len(dupes), len(intersection_nodes))

    ### Get drivable roads
    # In OSM, the following types of roads are one-way by default:
    drivable_oneway_default = ['motorway', 'motorway_link', 'motorway_junction', 'trunk', 'trunk_link']
    # These roads are two-way by default:
    drivable_twoway_default = ['primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'unclassified', 'unsurfaced', 'track', 'residential', 'living_street', 'service']

    ways_list = []
    nodes_in_ways_list = []
    for w in all_ways:
        if w['tags']['highway'] in drivable_oneway_default:
            way, nodes_in_way = create_way(w, intersection_nodes, 'y', False)
            ways_list.append(way)
            nodes_in_ways_list += nodes_in_way
        if w['tags']['highway'] in drivable_twoway_default:
            if ('oneway' in w['tags']) and (w['tags']['oneway'] in ['yes', 'true', '1']):
                way, nodes_in_way = create_way(w, intersection_nodes, 'y', False)
                ways_list.append(way)
                nodes_in_ways_list += nodes_in_way
            elif ('oneway' in w['tags']) and w['tags']['oneway'] in ['reverse', '-1']:
                way, nodes_in_way = create_way(w, intersection_nodes, 'y', True)
                ways_list.append(way)
                nodes_in_ways_list += nodes_in_way
            else:
                way, nodes_in_way = create_way(w, intersection_nodes, 'nf', False) ### twoway and the forward lane direction
                ways_list.append(way)
                nodes_in_ways_list += nodes_in_way
                way, nodes_in_way = create_way(w, intersection_nodes, 'nb', True) ### twoway and the reverse/backward lane direction
                ways_list.append(way)
                nodes_in_ways_list += nodes_in_way

    with open(absolute_path+'/../data/{}/ways.json'.format(folder), 'w') as links_outfile:
        json.dump(ways_list, links_outfile, indent=2)

    nodes_in_ways_set = set(nodes_in_ways_list)
    nodes_in_links_dict = {n: all_nodes[n] for n in nodes_in_ways_set}
    with open(absolute_path+'/../data/{}/nodes.json'.format(folder), 'w') as nodes_outfile:
        json.dump(nodes_in_links_dict, nodes_outfile, indent=2)

    if output_geojson:
        nodes_feature_list = []
        for k, v in nodes_in_links_dict.items():
            node_feature = {
                'type': 'Feature', 
                'geometry': {'type': 'Point', 'coordinates': [v[1], v[0]]},
                'properties': {'osmid': k}
                }
            nodes_feature_list.append(node_feature)
        nodes_geojson = {'type': 'FeatureCollection', 'features': nodes_feature_list}

        links_feature_list = []
        for w in ways_list:
            link_feature = {
                'type': 'Feature', 
                'geometry': {
                    'type': 'LineString', 
                    'coordinates': [[nodes_in_links_dict[n][1], nodes_in_links_dict[n][0]] for n in w['nodes']]
                    },
                'properties': {
                    'osmid': w['osmid'],
                    'type': w['type']
                    }
                }
            links_feature_list.append(link_feature)
        links_geojson = {'type': 'FeatureCollection', 'features': links_feature_list}

        with open(absolute_path+'/../data/{}/converted_ways.geojson'.format(folder), 'w') as links_outfile:
            json.dump(links_geojson, links_outfile, indent=2)

        with open(absolute_path+'/../data/{}/convertd_nodes.geojson'.format(folder), 'w') as nodes_outfile:
            json.dump(nodes_geojson, nodes_outfile, indent=2)


if __name__ == '__main__':
    #osm_to_geojson(folder = 'sf')
    osm_to_json(output_geojson=False, folder = 'sf')


