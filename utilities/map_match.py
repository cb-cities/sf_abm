import json
import sys
from scipy import spatial
import igraph 
import pandas as pd 
import numpy as np 
import re 

def new_graph():
    g = igraph.Graph.Read_GraphMLz('data_repo/Imputed_data_False9_0509.graphmlz')
    print(g.summary())
    g.es['speed_limit'] = 25

    speed_file = 'data_repo/tagged_alloneway_speedlimit_links.json'
    speed_data = json.load(open(speed_file))

    for link_osmid, link in speed_data.items():
        if int(link['speed_limit']) > 25:
            g.es.select(edge_osmid=link_osmid)['speed_limit'] = int(link['speed_limit'])
    
    g.es['capacity'] = [(1700+s*10)*(s/15) for s in g.es['speed_limit']]

    del g.vs['id']
    del g.es['sec_duration']
    del g.es['sec_speed']
    print(g.summary())

    g.write_graphmlz('data_repo/SF.graphmlz')
    print('end')

def main():
    ### Default Speed OSM https://wiki.openstreetmap.org/wiki/OSM_tags_for_routing/Maxspeed#United_States_of_America
    ### motorway/motorway_link: 65 mph
    ### trunk/trunk_link/primary/primary_link: 55 mph
    ### others: 25 mph
    links_file = 'data_repo/tagged_alloneway_links.json'
    links_data = json.load(open(links_file))
    #link_types = [l['tag_type'] for k, l in links_data.items()]
    #print(set(link_types))
    for link_osmid, link in links_data.items():
        if link['tag_type'] in ('motorway', 'motorway_link'): link['speed_limit'] = 65
        elif link['tag_type'] in ('trunk', 'trunk_link', 'primary', 'primary_link'): link['speed_limit'] = 55
        else: link['speed_limit'] = 25

    links_osmid = set([re.sub("[^0-9]", "", key) for key, value in links_data.items()])

    osm_file = 'data_repo/target.osm'
    osm_data = json.load(open(osm_file))
    osm_data = osm_data['elements']
    osm_link_data = [element for element in osm_data if (element['type']=='way') and (str(element['id']) in links_osmid)]
    for osm_link in osm_link_data:
        try:
            speed_limit = re.sub("[^0-9]", "", osm_link['tags']['maxspeed'])
        except KeyError:
            continue

        links_data[str(osm_link['id'])]['speed_limit'] = speed_limit
        try:
            links_data[str(osm_link['id'])+'r']['speed_limit'] = speed_limit
        except KeyError:
            continue

    with open('data_repo/tagged_alloneway_speedlimit_links.json', 'w') as outfile:
        json.dump(links_data, outfile, indent=2)


if __name__ == '__main__':
    #main()
    new_graph()

