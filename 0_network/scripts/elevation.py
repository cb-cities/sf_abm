import numpy as np 
import pandas as pd 
from PIL import Image
import rasterio
import matplotlib.pyplot as plt 
import sys

def main():

    ### DEM map 1/3 second
    dem_path = '../data/sf_dem_usgs.tif'
    dem = rasterio.open(dem_path)
    demb = dem.read(1)
    row, col = dem.index(-122.4073357, 37.7353182)
    # print(dem.bounds.left, dem.bounds.top)
    # print(row, col, demb[row, col])

    ### Road end elevation
    nodes_df = pd.read_csv('../data/sf_overpass/original/nodes.csv')
    nodes_df['elevation'] = nodes_df.apply(lambda x: demb[dem.index(x['lon'], x['lat'])], axis=1)
    #nodes_df.to_csv('../data/sf_overpass/original/nodes_elevation.csv')
    
    ### Road links
    edges_df = pd.read_csv('../data/sf_overpass/original/edges.csv')  
    ### Start elevation
    edges_df = pd.merge(edges_df, nodes_df[['node_id_igraph', 'elevation']], left_on='start_igraph', right_on='node_id_igraph', how='left')
    edges_df = edges_df.rename(columns = {'elevation': 'start_elevation'})
    edges_df = edges_df.drop(columns='node_id_igraph')
    ### End elevation
    edges_df = pd.merge(edges_df, nodes_df[['node_id_igraph', 'elevation']], left_on='end_igraph', right_on='node_id_igraph', how='left')
    edges_df = edges_df.rename(columns = {'elevation': 'end_elevation'})
    edges_df = edges_df.drop(columns='node_id_igraph')
    ### Height difference, grade and adjusted length
    edges_df['delta_z'] = edges_df['end_elevation'] - edges_df['start_elevation']
    edges_df['slope'] = edges_df['delta_z']/edges_df['length']
    edges_df['slope'] = np.where(edges_df['slope']<-0.06, -0.06, np.where(edges_df['slope']>0.1, 0.1, edges_df['slope'])) ### According to Highway Capacity Manual Exhibit 16-7
    edges_df['length'] = np.sqrt(edges_df['length']**2 + edges_df['delta_z']**2)

    edges_df['fft'] = edges_df['length']/edges_df['maxmph'] 


if __name__ == '__main__':
    main()