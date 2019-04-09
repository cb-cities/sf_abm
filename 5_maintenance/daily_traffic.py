import json
import sys
import numpy as np 
import pandas as pd 
import os 
import matplotlib.pyplot as plt 
import matplotlib.cm as cm 
import gc

absolute_path = os.path.dirname(os.path.abspath(__file__))

folder = 'sf_overpass/original'
outdir = 'output_march19'

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

def hourly_traffic(case):

    ### ABM parameters
    random_seed = 0
    probe_ratio = 1

    ### sustainability simulation parameters
    budget = 700
    eco_route_ratio = 1.0
    iri_impact = 0.03
    case = 'ee'
    year = 10

    ### Aggregate hourly flow to weekly flow
    network_attr_df = pd.read_csv(absolute_path+'/../0_network/data/{}/edges_elevation.csv'.format(folder))
    network_attr_df = network_attr_df[['edge_id_igraph', 'start_sp', 'end_sp', 'type', 'lanes', 'length', 'geometry']]
    network_attr_df[case] = 0
    
    for day in [2]:
        for hour in range(3, 27):

            hour_edge_flow_df = pd.read_csv(absolute_path+'/{}/edges_df_abm/edges_df_b{}_e{}_i{}_c{}_y{}_HR{}.csv'.format(outdir, budget, eco_route_ratio, iri_impact, case, year, hour))
            network_attr_df = pd.merge(network_attr_df, hour_edge_flow_df[['edge_id_igraph', 'true_flow']], on = ['edge_id_igraph'])
            network_attr_df[case] += network_attr_df['true_flow']
            network_attr_df = network_attr_df[['edge_id_igraph', 'start_sp', 'end_sp', 'type', 'lanes', 'length', case, 'geometry']]
            gc.collect()

    ### Directed flow
    #network_attr_df.to_csv('directed_{}.csv'.format(case), index=False)

    ### Merge results from two directions
    network_attr_df['edge_id_igraph_str'] = network_attr_df['edge_id_igraph'].astype(str)
    network_attr_df['undir_uv_sp'] = pd.DataFrame(np.sort(network_attr_df[['start_sp', 'end_sp']].values, axis=1), columns=['small_sp', 'large_sp']).apply(lambda x:'%s_%s' % (x['small_sp'],x['large_sp']),axis=1)
    network_attr_df_grp = network_attr_df.groupby('undir_uv_sp').agg({
            case: np.sum, 
            'lanes': np.sum,
            'edge_id_igraph_str': lambda x: '-'.join(x),
            'type': 'first',
            'geometry': 'first'}).reset_index()
    network_attr_df_grp = network_attr_df_grp.rename(columns={case: 'undirected_{}'.format(case), 'lanes': 'undirected_lanes'})

    network_attr_df_grp.to_csv(absolute_path + '/{}/daily_undirected_b{}_e{}_i{}_c{}_y{}.csv'.format(outdir, budget, eco_route_ratio, iri_impact, case, year), index=False)


if __name__ == '__main__':
    hourly_traffic('friday_traffic')



