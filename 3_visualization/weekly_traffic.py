import json
import sys
import numpy as np 
import pandas as pd 
import os 

absolute_path = os.path.dirname(os.path.abspath(__file__))

folder = 'sf_overpass/original'

def main():

    ### Aggregate hourly flow to weekly flow
    network_attr_df = pd.read_csv(absolute_path+'/../0_network/data/{}/edges_elevation.csv'.format(folder))
    network_attr_df = network_attr_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'geometry']]
    network_attr_df['fiveday_traffic'] = 0

    random_seed = 0
    probe_ratio = 1
    for day in [0, 1, 2, 3, 4]:
        for hour in range(3, 27):
            hour_edge_flow_df = pd.read_csv(absolute_path+'/../2_ABM/output/edges_df/edges_df_DY{}_HR{}_r{}_p{}.csv'.format(day, hour, random_seed, probe_ratio))
            network_attr_df = pd.merge(network_attr_df, hour_edge_flow_df[['edge_id_igraph', 'net_true_flow']], on = ['edge_id_igraph'])
            network_attr_df['fiveday_traffic'] += network_attr_df['net_true_flow']
            network_attr_df = network_attr_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'fiveday_traffic', 'geometry']]

    ### Directed flow
    network_attr_df.to_csv('directed_weekly_traffic_20190227.csv', index=False)

    ### Merge results from two directions
    network_attr_df['edge_id_igraph_str'] = network_attr_df['edge_id_igraph'].astype(str)
    network_attr_df['undir_sp_uv'] = pd.DataFrame(np.sort(network_attr_df[['start_sp', 'end_sp']].values, axis=1), columns=['small', 'large']).apply(lambda x:'%s_%s' % (x['small'],x['large']),axis=1)
    network_attr_df_grp = network_attr_df.groupby('undir_sp_uv').agg({
            'fiveday_traffic': np.sum, 
            'edge_id_igraph_str': lambda x: '-'.join(x),
            'geometry': 'first'}).reset_index()
    network_attr_df_grp = network_attr_df_grp.rename(columns={'fiveday_traffic': 'undirected_fiveday_traffic'})

    network_attr_df_grp.to_csv('undirected_weekly_traffic_20190227.csv', index=False)

if __name__ == '__main__':
    main()



