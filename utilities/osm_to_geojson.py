#!python3
import pprint
import json
import os
import numpy as np
import haversine
import re
import sys
import random

def osm_to_geojson(folder='sf'):
    # Load OSM data as downloaded from overpass
    osm_data = json.load(open('{}/target.osm'.format(folder)))
    osm_data = osm_data['elements']
    print('length of the OSM data: ', len(osm_data))

    # Based on the OSM data, make a dictionary: all_nodes = {osm_node_id:(lat,lon), ...}
    all_nodes = {n['id']: (n['lat'], n['lon']) for n in osm_data if n['type']=='node'}
    print('it includes {} nodes'.format(len(all_nodes)))

    # Make a list of way elements
    all_ways = [w_e for w_e in osm_data if w_e['type']=='way']
    print('it includes {} nodes'.format(len(all_ways)))

    nodes_feature_list = []
    for k, v in all_nodes.items():
        node_feature = {
            'type': 'Feature', 
            'geometry': {'type': 'Point', 'coordinates': v[1], v[0]},
            'properties': {'osmid': k}
            }
        nodes_feature_list.append(node_feature)
    nodes_geojson = {'type': 'FeatureCollection', 'features': nodes_feature_list}

    links_feature_list = []
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

    with open('{}/osm_links.geojson'.format(folder), 'w') as links_outfile:
        json.dump(links_geojson, links_outfile, indent=2)

    with open('{}/osm_nodes.geojson'.format(folder), 'w') as nodes_outfile:
        json.dump(nodes_geojson, nodes_outfile, indent=2)

if __name__ == '__main__':
    osm_to_geojson(folder = 'sf')


