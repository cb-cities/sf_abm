### Scale the TNC demand by supervisorial shares

import pandas as pd 
import geopandas as gpd 
import json 
import sys 
import numpy as np 
import os 

absolute_path = os.path.dirname(os.path.abspath(__file__))

def main():
    ### Read TAZ polyline
    taz_gdf = gpd.read_file(absolute_path+'/../input/TAZ981/TAZ981.shp')
    taz_gdf = taz_gdf.to_crs({'init': 'epsg:4326'})
    #print(taz_gdf.head())

    ### Uber/Lyft data
    taz_travel_df = pd.read_csv(absolute_path+'/../input/TNC_pickups_dropoffs.csv') ### TNC ODs from each TAZ
    day = 0
    hour = 9
    hour_taz_travel_df = taz_travel_df[(taz_travel_df['day_of_week']==day) & (taz_travel_df['hour']==hour)].reset_index()
    #print(hour_taz_travel_df.head())
    merged_taz_gdf = pd.merge(taz_gdf, hour_taz_travel_df, how='left', left_on='TAZ', right_on='taz')
    merged_taz_gdf = merged_taz_gdf[['TAZ', 'pickups', 'dropoffs', 'geometry']]

    ### Sampled data
    taz_nodes_dict = json.load(open(absolute_path+'/../output/sf_overpass/original/taz_nodes.json'))
    taz_nodes_df = pd.DataFrame([(k, vi) for k, v in taz_nodes_dict.items() for vi in v], columns=['TAZ', 'node'])
    nodes_od_df = pd.read_csv(absolute_path+'/../output/sf_overpass/original/DY{}/SF_OD_DY{}_HR{}.csv'.format(day, day, hour))
    nodes_od_df = pd.merge(nodes_od_df, taz_nodes_df, how='left', left_on='O', right_on='node')
    nodes_od_df = pd.merge(nodes_od_df, taz_nodes_df, how='left', left_on='D', right_on='node', suffixes=['_O', '_D'])
    O_taz_df = nodes_od_df.groupby('TAZ_O').agg({'flow': np.sum}).reset_index()
    O_taz_df['TAZ_O'] = O_taz_df['TAZ_O'].astype(int)
    D_taz_df = nodes_od_df.groupby('TAZ_D').agg({'flow': np.sum}).reset_index()
    D_taz_df['TAZ_D'] = D_taz_df['TAZ_D'].astype(int)

    merged_taz_gdf = pd.merge(merged_taz_gdf, O_taz_df, how='left', left_on='TAZ', right_on='TAZ_O')
    merged_taz_gdf = pd.merge(merged_taz_gdf, D_taz_df, how='left', left_on='TAZ', right_on='TAZ_D', suffixes = ['_O', '_D'])
    merged_taz_gdf = merged_taz_gdf[['TAZ', 'pickups', 'dropoffs', 'flow_O', 'flow_D', 'geometry']]
    merged_taz_gdf = merged_taz_gdf.fillna(value={'pickups': 0, 'dropoffs': 0, 'flow_O': 0, 'flow_D': 0})
    merged_taz_gdf.to_csv(absolute_path+'/../output/sf_overpass/original/DY{}_HR{}_geometry.csv'.format(day, hour), index=False)


if __name__ == '__main__':
    main()
