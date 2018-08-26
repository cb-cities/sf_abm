import json
import sys
from scipy import spatial
import igraph 
import pandas as pd 
import numpy as np 
import re 
import os 

def main():
    ### Default Speed OSM https://wiki.openstreetmap.org/wiki/OSM_tags_for_routing/Maxspeed#United_States_of_America
    ### motorway/motorway_link: 65 mph
    ### trunk/trunk_link/primary/primary_link: 55 mph
    ### others: 25 mph
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    links_file = absolute_path + '/../data/links_0.json'
    links_data = json.load(open(links_file))
    #link_types = [l['tag_type'] for k, l in links_data.items()]
    #print(set(link_types))
    for link_osmid, link in links_data.items():
        if link['tag_type'] in ('motorway', 'motorway_link'): link['speed_limit'] = 65
        elif link['tag_type'] in ('trunk', 'trunk_link', 'primary', 'primary_link'): link['speed_limit'] = 55
        else: link['speed_limit'] = 25

    links_osmid = set([re.sub("[^0-9]", "", key) for key, value in links_data.items()])

    osm_file = absolute_path + '/../data/target.osm'
    osm_data = json.load(open(osm_file))
    osm_data = osm_data['elements']
    osm_link_data = [element for element in osm_data if (element['type']=='way') and (str(element['id']) in links_osmid)]
    for osm_link in osm_link_data:
        try:
            ### "maxspeed": "35 mph;30 mph"
            speed_limit = re.search(r"\d+", osm_link['tags']['maxspeed']).group()
        except KeyError:
            continue

        links_data[str(osm_link['id'])]['speed_limit'] = int(speed_limit)
        try:
            links_data[str(osm_link['id'])+'r']['speed_limit'] = int(speed_limit)
        except KeyError:
            continue

    with open(absolute_path + '/../data/links.json', 'w') as outfile:
        json.dump(links_data, outfile, indent=2)


if __name__ == '__main__':
    main()

