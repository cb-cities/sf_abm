#!python3
import pprint
import json
import os
import numpy as np
import haversine
import re
import sys

# Load OSM data as downloaded from overpass
#osm_dir = '~/osm-project/overpass'
#osm_data = json.load(open(os.path.expanduser(osm_dir+'/target.osm')))
absolute_path = os.path.dirname(os.path.abspath(__file__))
osm_data = json.load(open(absolute_path+'/../data/target.osm'))
osm_data = osm_data['elements']
print('length of the OSM data: ', len(osm_data))

# Based on the OSM data, make nodes_dict = {'osm_node_id':(lat,lon), ...}
nodes_dict = {str(n['id']): (n['lat'], n['lon']) for n in osm_data if n['type']=='node'}
print('it includes {} nodes'.format(len(nodes_dict)))
print('example node: ', nodes_dict['26117861'])

# Make links_dict = {'osm_way_id':[list_of_points_on_the_link], ...}
way_elements = [w_e for w_e in osm_data if w_e['type']=='way']
# One-way default:
pattern_drivable_oneway_default = "(motorway|motorway_link|motorway_junction|trunk|trunk_link)"
way_elements_oneway_default = [w_e for w_e in way_elements if re.search(pattern_drivable_oneway_default, w_e['tags']['highway'])]
links_dict = {str(l['id']): list(map(str, l['nodes'])) for l in way_elements_oneway_default}
link_type_dict = {str(l['id']): l['tags']['highway'] for l in way_elements_oneway_default}
# Links whose default is two-way:
pattern_drivable_twoway_default = "(primary_link|primary|secondary|tertiary|unclassified|unsurfaced|track|residential|living_street|dservice)"
way_elements_twoway_default = [w_e for w_e in way_elements if re.search(pattern_drivable_twoway_default, w_e['tags']['highway'])]
for l in way_elements_twoway_default:
    if ('oneway' in l['tags']) and (l['tags']['oneway'] in ['yes', 'true', '1']):
        links_dict[str(l['id'])] = list(map(str, l['nodes']))
        link_type_dict[str(l['id'])] = l['tags']['highway']
    elif ('oneway' in l['tags']) and l['tags']['oneway'] in ['reverse', '-1']:
        links_dict[str(l['id'])] = list(map(str, l['nodes'][::-1]))
        link_type_dict[str(l['id'])] = l['tags']['highway']
    else:
        links_dict[str(l['id'])] = list(map(str, l['nodes']))
        links_dict[str(l['id'])+'r'] = list(map(str, l['nodes'][::-1]))
        link_type_dict[str(l['id'])] = l['tags']['highway']
        link_type_dict[str(l['id'])+'r'] = l['tags']['highway']

#links_dict = {str(l['id']): list(map(str, l['nodes'])) for l in osm_data if (l['type']=='way') and (re.search(pattern_drivable, l['tags']['highway']))}
print('and {} links'.format(len(links_dict)))
#print('example link: ', links_dict['12437582'])
#print('example link: ', links_dict['12437582r'])
# Default one-ways: motorway, motorway_link, trunk, trunk_link, junction

# Reformat links_dict into a convenient format for assigning travel time
#{'osm_way_id': 
#   {'section': [(node1, node2), (node2, node3), ...]
#    'length': [harversine_length_of_each_section],
#    'start': osm_id_of_first_node,
#    'end': osm_id_of_last_node,
#    'total_length': total_length_of_link,
#    'cum_frac_length': cumulative_distance_from_start_as_fraction,
#    'tag_type': type of road as in OSM 'highway' tag}
nodes_in_links = set()
for l_key, l_value in links_dict.items():
    l_n = l_value
    nodes_in_links.update(l_n)
    links_dict[l_key] = {'sections': [(x,y) for x,y in zip(l_n, l_n[1:])]}
    links_dict[l_key]['length'] = [max(0.1, haversine.haversine(nodes_dict[x][0], nodes_dict[x][1], nodes_dict[y][0], nodes_dict[y][1])) for (x,y) in zip(l_n, l_n[1:])]
    links_dict[l_key]['start'] = l_n[0]
    links_dict[l_key]['end'] = l_n[-1]
    total_length = sum(links_dict[l_key]['length'])
    links_dict[l_key]['total_length'] = total_length
    links_dict[l_key]['cum_frac_length'] = [section_l/total_length for section_l in np.cumsum(links_dict[l_key]['length'])]
    links_dict[l_key]['tag_type'] = link_type_dict[l_key]

#print('\n An example of reformatted link:')
#pprint.pprint(links_dict['8915500'])
print(len(nodes_dict), len(nodes_in_links))
#print(type(next(iter(nodes_in_links))))

with open(absolute_path+'/../data/links_0.json', 'w') as links_outfile:
    json.dump(links_dict, links_outfile, indent=2)

nodes_in_links_dict = {n: nodes_dict[n] for n in nodes_in_links}
with open(absolute_path+'/../data/nodes.json', 'w') as nodes_in_links_outfile:
    json.dump(nodes_in_links_dict, nodes_in_links_outfile, indent=2)



