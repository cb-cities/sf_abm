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
    eco_route_ratio = 0.1
    iri_impact = 0.03
    case = 'er'
    year = 10

    ### Aggregate hourly flow to weekly flow
    network_attr_df = pd.read_csv(absolute_path+'/../0_network/data/{}/edges_elevation.csv'.format(folder))
    network_attr_df = network_attr_df[['edge_id_igraph', 'start_sp', 'end_sp', 'type', 'lanes', 'length', 'geometry']]
    network_attr_df[case] = 0
    
    for day in [2]:
        for hour in range(3, 27):

            hour_edge_flow_df = pd.read_csv(absolute_path+'/{}/edges_df_singleyear/edges_df_DY{}_HR{}_r{}_p{}.csv'.format(outdir, day, hour, random_seed, probe_ratio))
            #hour_edge_flow_df = pd.read_csv(absolute_path+'/{}/edges_df_abm/edges_df_b{}_e{}_i{}_c{}_y{}_HR{}.csv'.format(outdir, budget, eco_route_ratio, iri_impact, case, year, hour))
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

    network_attr_df_grp.to_csv(absolute_path + '/{}/daily_undirected_DY{}_r{}_p{}.csv'.format(outdir, day, random_seed, probe_ratio), index=False)
    #network_attr_df_grp.to_csv(absolute_path + '/{}/daily_undirected_b{}_e{}_i{}_c{}_y{}.csv'.format(outdir, budget, eco_route_ratio, iri_impact, case, year), index=False)

def difference_plot():
    base_df = pd.read_csv(absolute_path+'/{}/daily_undirected_DY2_r0_p1.csv'.format(outdir))
    e50_df = pd.read_csv(absolute_path + '/{}/daily_undirected_b700_e0.5_i0.03_cee_y10.csv'.format(outdir))
    e100_df = pd.read_csv(absolute_path + '/{}/daily_undirected_b700_e1.0_i0.03_cee_y10.csv'.format(outdir))

    base_df = pd.merge(base_df, e50_df, on='edge_id_igraph_str', how='left')
    base_df = base_df.rename(columns={'undirected_ee': 'undirected_ee50'})
    base_df['diff_e50'] = base_df['undirected_ee50'] - base_df['undirected_normal']

    base_df = pd.merge(base_df, e100_df, on='edge_id_igraph_str', how='left')
    base_df = base_df.rename(columns={'undirected_ee': 'undirected_ee100'})
    base_df['diff_e100'] = base_df['undirected_ee100'] - base_df['undirected_normal']

    base_df['log_undirected_normal'] = np.log(base_df['undirected_normal']+1)/np.log(10)
    base_df['type2'] = base_df['type'].apply(lambda x: x.split('_')[0])

    #fig, ax = plt.subplots()
    #fig.set_size_inches(9, 5)
    color = iter(cm.viridis(np.linspace(0, 1, 13)))
    base_grp = base_df.groupby('type2')
    for name, grp in base_grp:
        c = next(color)
        plt.scatter('log_undirected_normal', 'diff_e100', data=grp, s=1, c=c, label=name)
        plt.legend()
        plt.show()
    #plt.xscale('log')
    # plt.legend()
    # plt.show()

if __name__ == '__main__':
    hourly_traffic('friday_traffic')
    #difference_plot()



