import numpy as np 
import pandas as pd 
from PIL import Image
import rasterio
import matplotlib.pyplot as plt 
import sys

def change_capacity(edges_df):
    # edges_df['capacity_old'] = edges_df['capacity']
    edges_df['capacity'] = 1900 * 0.45 * edges_df['lanes'] ### Urban street
    edges_df['capacity'] = np.where(edges_df['type'].isin(['motorway', 'motorway_link']), 
        (1700+10*np.minimum(70, edges_df['maxmph']))/1.05*edges_df['lanes'],
        edges_df['capacity'])
    edges_df['capacity'] = np.where((edges_df['type'].isin(['primary', 'primary_link'])&(edges_df['lanes']>=2)), 
        (1000+20*np.minimum(60, edges_df['maxmph']))/1.05*edges_df['lanes'],
        edges_df['capacity'])
    edges_df['capacity'] = np.where((edges_df['type'].isin(['primary', 'primary_link'])&(edges_df['lanes']==1)), 
        1490, 
        edges_df['capacity'])

    # fig, ax = plt.subplots()
    # ax.hist(edges_df['capacity_old'], bins=100, fc='None', edgecolor='blue')
    # ax.hist(edges_df['capacity'], bins=100, fc='None', edgecolor='red')
    # plt.yscale('log')
    # plt.show()
    # sys.exit(0)

    return edges_df

def main():

    ### DEM map 1/9 second
    dem1 = rasterio.open('../data/sf_dem1.tif')
    demb1 = dem1.read(1)

    dem2 = rasterio.open('../data/sf_dem2.tif')
    demb2 = dem2.read(1)

    dem3 = rasterio.open('../data/sf_dem3.tif')
    demb3 = dem3.read(1)

    dem4 = rasterio.open('../data/sf_dem4.tif')
    demb4 = dem4.read(1)
    row, col = dem4.index(-122.4073357, 37.7353182)
    # print(dem4.bounds.left, dem4.bounds.top)
    # print(row, col, demb4[row, col])
    # sys.exit(0)

    ### Road end elevation
    nodes_df = pd.read_csv('../data/sf_overpass/original/nodes.csv')
    nodes_df['elevation'] = nodes_df.apply(lambda x: max(
        demb1[dem1.index(x['lon'], x['lat'])],
        demb2[dem2.index(x['lon'], x['lat'])],
        demb3[dem3.index(x['lon'], x['lat'])],
        demb4[dem4.index(x['lon'], x['lat'])]), axis=1)
    nodes_df.to_csv('../data/sf_overpass/original/nodes_elevation.csv', index=False)
    
    ### Road links
    edges_df = pd.read_csv('../data/sf_overpass/original/edges.csv')
    edges_df = change_capacity(edges_df) ### Capacity based on NCHRP report 825

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
    edges_df['slope'] = np.where(edges_df['slope']<-0.3, -0.3, np.where(edges_df['slope']>0.3, 0.3, edges_df['slope']))
    ### slope range according to Highway Capacity Manual Exhibit 16-7

    ### Update length
    edges_df['length'] = np.sqrt(edges_df['length']**2 + edges_df['delta_z']**2)
    ### Update fft
    edges_df['fft'] = edges_df.apply(lambda row: row['length']/row['maxmph']*2.23694 * 1.2 + row['traffic_signals_delay'] + row['crossings_stops_delay'], axis=1)
    ### Update capacity according to Highway Capacity Manual Exhibit 16-7
    edges_df['capacity'] = edges_df['capacity']*(1-edges_df['slope']/2)

    edges_df = edges_df.drop(columns=['delta_z'])
    edges_df.to_csv('../data/sf_overpass/original/edges_elevation.csv', index=False)


if __name__ == '__main__':
    main()