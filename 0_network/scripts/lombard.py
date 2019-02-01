import json
import os
import pandas as pd 

absolute_path = os.path.dirname(os.path.abspath(__file__))
folder='sf_overpass'

def main():
    print('Lombard')
    lombard_osmid = 402111597

    # Load OSM data as downloaded from overpass
    osm_data = json.load(open(absolute_path+'/../data/{}/target.osm'.format(folder)))
    osm_data = osm_data['elements']
    print('length of the OSM data: ', len(osm_data))

    # Based on the OSM data, make a dictionary of node element: all_nodes = {osm_node_id:(lat,lon), ...}
    # also include any highway tags, such as traffic signals
    all_nodes = {}
    for n in osm_data:
        if n['type']=='node':
            all_nodes[n['id']] = (n['lat'], n['lon'])

    lombard_way = [w for w in osm_data if w['id']==lombard_osmid]
    lombard_nodes = lombard_way[0]['nodes']
    lombard_wkt = 'LINESTRING ({})'.format(','.join(['{} {}'.format(all_nodes[n][1], all_nodes[n][0]) for n in lombard_nodes]))
    lombard_line_pd = pd.DataFrame({'id': [0], 'geom': [lombard_wkt]})
    
    lombard_nodes_pd = pd.DataFrame({'osmid': lombard_nodes})
    lombard_nodes_pd['geom'] = lombard_nodes_pd.apply(lambda row: 'POINT ({} {})'.format(all_nodes[row['osmid']][1], all_nodes[row['osmid']][0]), axis=1)
    
    lombard_line_pd.to_csv('lombard_lines.csv', index=False)
    lombard_nodes_pd.to_csv('lombard_nodes.csv', index=False)

if __name__ == '__main__':
    main()