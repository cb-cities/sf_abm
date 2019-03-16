### Based on https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
import json
import sys
import numpy as np
import os
import pandas as pd 
from ctypes import *
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point

pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, absolute_path+'/../')
sys.path.insert(0, '/Users/bz247/')
from sp import interface 

folder = 'berkeley'
scenario = 'original'

def route_wkt(g, origin, destin, nodes_dict):
    sp = g.dijkstra(origin, destin) ### g_0 is the network with imperfect information for route planning
    sp_dist = sp.distance(destin) ### agent believed travel time with imperfect information
    sp_route = sp.route(destin) ### agent route planned with imperfect information
    node_results = [edge[0] for edge in sp_route]+[destin]
    wkt_results = 'LINESTRING ({})'.format(','.join(['{} {}'.format(nodes_dict[n][0], nodes_dict[n][1]) for n in node_results]))
    print(wkt_results)

def print_buffer(gdf):

    buffer_gdf = gdf
    buffer_gdf['geom_buffer'] = buffer_gdf.buffer(0.0015)
    buffer_gdf.to_csv('buffer.csv')

def main():
    nodes_df = pd.read_csv(open(absolute_path+'/{}/nodes.csv'.format(scenario)))
    nodes_dict = {getattr(row, 'node_id_igraph')+1:(getattr(row, 'lon'), getattr(row, 'lat')) for row in nodes_df.itertuples()}

    g = interface.readgraph(bytes(absolute_path+'/{}/network_sparse.mtx'.format(scenario), encoding='utf-8'))
    origin = 164
    destin = 541

    #route_wkt(g, origin, destin, nodes_dict)

    crime_locations_df = pd.read_csv(open(absolute_path+'/crime_locations.csv'))
    geometry = [Point(xy) for xy in zip(crime_locations_df.lon, crime_locations_df.lat)]
    crs = {'init': 'epsg:4326'}
    crime_locations_gdf = gpd.GeoDataFrame(crime_locations_df, crs=crs, geometry=geometry)

    # print_buffer(crime_locations_gdf)
    # sys.exit(0)

    ### Read OSM road network
    edges_df = pd.read_csv(open(absolute_path + '/{}/edges.csv'.format(scenario))) ### undirected
    edges_df['geometry'] = edges_df['geometry'].apply(wkt.loads)
    edges_gdf = gpd.GeoDataFrame(edges_df, crs = {'init': 'epsg:4326'}, geometry='geometry')

    edges_gdf_sindex = edges_gdf.sindex

    blocked_edges = []
    for crime_event in crime_locations_gdf.itertuples():
        crime_loc = getattr(crime_event, 'geometry')
        crime_buffer = crime_loc.buffer(0.001)
        possible_matches_index = list(edges_gdf_sindex.intersection(crime_buffer.bounds))
        possible_matches = edges_gdf.iloc[possible_matches_index].reset_index()
        precise_matches = possible_matches[possible_matches.intersects(crime_buffer)]
        blocked_edges += precise_matches['edge_id_igraph'].tolist()
    blocked_edges_set = set(blocked_edges)

    g_blocked = g
    blocked_edges_gdf = edges_gdf.loc[edges_gdf['edge_id_igraph'].isin(blocked_edges_set)]
    for b_e in blocked_edges_gdf.itertuples():
        g_blocked.update_edge(getattr(b_e,'start_sp'), getattr(b_e,'end_sp'), c_double(10000))

    route_wkt(g_blocked, origin, destin, nodes_dict)


if __name__ == '__main__':
    main()