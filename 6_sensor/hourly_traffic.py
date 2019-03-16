import scipy.io as sio
import json
import sys
import igraph 
import numpy as np 
import pandas as pd 
import geopandas as gpd
import os 

absolute_path = os.path.dirname(os.path.abspath(__file__))

folder = 'sf_overpass/original'

def main(day, hour, random_seed, probe_ratio):

    ### Get hour flow of a particular snapshot of the day
    edge_flow_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor/edges_df/edges_df_DY{}_HR{}_r{}_p{}.csv'.format(day, hour, random_seed, probe_ratio))

    ### Get attributes and geometry of each edge
    network_attr_df = pd.read_csv(absolute_path+'/../0_network/data/{}/edges.csv'.format(folder), index_col=0)
    edge_flow_df = pd.merge(edge_flow_df, network_attr_df, on = ['edge_id_igraph'])
    edge_flow_df['edge_id_igraph_str'] = edge_flow_df['edge_id_igraph'].astype(str)
    edge_flow_df['voc'] = edge_flow_df['true_flow']/edge_flow_df['capacity']

    ### Merge results from two directions
    edge_flow_df['undir_uv_igraph'] = pd.DataFrame(np.sort(edge_flow_df[['start_igraph', 'end_igraph']].values, axis=1), columns=['small_igraph', 'large_igraph']).apply(lambda x:'%s_%s' % (x['small_igraph'],x['large_igraph']),axis=1)
    edge_flow_df_grp = edge_flow_df.groupby('undir_uv_igraph').agg({
            'true_flow': np.sum, 
            'voc': np.max,
            'edge_id_igraph_str': lambda x: '-'.join(x),
            'geometry': 'first'}).reset_index()
    edge_flow_df_grp = edge_flow_df_grp.rename(columns={'true_flow': 'undirected_true_flow', 'voc': 'larger_voc'})
    
    # print(edge_flow_df_grp.iloc[0])
    # sys.exit(0)

    edge_flow_df_grp.to_csv(absolute_path+'/hourly_traffic/hourly_traffic_DY{}_HR{}_r{}_p{}.csv'.format(day, hour, random_seed, probe_ratio), index=False)

if __name__ == '__main__':
    main(day=4, hour=18, random_seed=0, probe_ratio=0.0)



