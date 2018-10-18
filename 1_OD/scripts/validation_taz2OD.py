### Scale the TNC demand by supervisorial shares

import pandas as pd 
import geopandas as gpd 
import json 
import sys 
import matplotlib.path as mpltPath
import numpy as np 
from collections import Counter
import random 
import itertools 
import os 
import datetime

absolute_path = os.path.dirname(os.path.abspath(__file__))

################################################################
### Estabilish relationship between OSM/graph nodes and TAZs ###
################################################################

def find_in_nodes(row, points, nodes_df):
    ### return the indices of points in nodes_df that are contained in row['geometry']
    ### this function is called by TAZ_nodes()
    if row['geometry'].type == 'MultiPolygon':
        return []
    else:
        path = mpltPath.Path(list(zip(*row['geometry'].exterior.coords.xy)))
        in_index = path.contains_points(points)
        return nodes_df['index'].loc[in_index].tolist()

def uber_taz_nodes():
    ### Find corresponding nodes for each TAZ
    ### Input 1: TAZ polyline
    taz_gdf = gpd.read_file(absolute_path+'/../../4_validation/uber_movements/san_francisco_taz.json')
    taz_gdf = taz_gdf.to_crs({'init': 'epsg:4326'})

    ### Input 2: OSM nodes coordinate
    nodes_dict = json.load(open(absolute_path+'/../../0_network/data/sf/nodes.json'))
    nodes_df = pd.DataFrame.from_dict(nodes_dict, orient='index', columns=['lat', 'lon']).reset_index()
    points = nodes_df[['lon', 'lat']].values
    taz_gdf['in_nodes'] = taz_gdf.apply(lambda row: find_in_nodes(row, points, nodes_df), axis=1)
    taz_nodes_dict = {getattr(row, 'TAZ'): getattr(row, 'in_nodes') for row in taz_gdf.itertuples() if len(getattr(row, 'in_nodes'))>0}
    
    ### [{'taz': 1, 'in_nodes': '[...]''}, ...]
    with open(absolute_path+'/../../4_validation/uber_movements/uber_taz_nodes.json', 'w') as outfile:
        json.dump(taz_nodes_dict, outfile, indent=2)

if __name__ == '__main__':

    uber_taz_nodes()