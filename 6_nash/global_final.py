import os
import sys
import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
import scipy.sparse as sp
import scipy.io as sio 

pd.set_option('display.max_columns', 10)
absolute_path = os.path.dirname(os.path.abspath(__file__))

folder = 'sf_overpass'
scenario = 'original'

### Route length
def route_length(edges_df, loaded_1, loaded_2, agent_1, agent_2):

    agent_1 = pd.merge(agent_1, edges_df[['start_sp', 'end_sp', 'length']], how='left', on=['start_sp', 'end_sp'])
    agent_route_length_1 = agent_1.groupby(['od_id']).agg({'length': np.sum}).rename(columns={'length': 'route_length_1'}).reset_index()
    agent_route_length_1['route_length_1_km'] = agent_route_length_1['route_length_1']/1000
    print('Case 1 # of agents: {}'.format(agent_route_length_1.shape[0]))

    agent_2 = pd.merge(agent_2, edges_df[['start_sp', 'end_sp', 'length']], how='left', on=['start_sp', 'end_sp'])
    agent_route_length_2 = agent_2.groupby(['od_id']).agg({'length': np.sum}).rename(columns={'length': 'route_length_2'}).reset_index()
    agent_route_length_2['route_length_2_km'] = agent_route_length_2['route_length_2']/1000
    print('Case 2 # of agents: {}'.format(agent_route_length_2.shape[0]))

    agent_compare = pd.merge(agent_route_length_1, agent_route_length_2, how='inner', on=['od_id'])
    print('Case 1 & 2 # of compares: {}'.format(agent_compare.shape[0]))

    plt.scatter('route_length_1_km', 'route_length_2_km', data=agent_compare, alpha=0.5, s=1)
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel('journey length (km) with probe ratio 10%')
    plt.ylabel('journey length (km) withprobe ratio 1%')
    plt.title('Journey distance change per agent (random seed 0, sigma 10)')
    plt.show()

    ### Agents travelling longer in 10% case
    agent_compare_opposite = agent_compare[(agent_compare['route_length_1_km'] - agent_compare['route_length_2_km'])>50]

def loaded_network(edges_df, loaded_df, case):

    loaded_df = pd.merge(loaded_df, edges_df[['edge_id_igraph', 'geometry', 'capacity']], how='left', on='edge_id_igraph')
    loaded_df['voc'] = loaded_df['hour_flow']/loaded_df['capacity']
    loaded_df['undir_uv_sp'] = pd.DataFrame(np.sort(loaded_df[['start_sp', 'end_sp']].values, axis=1), columns=['small_sp', 'large_sp']).apply(lambda x:'%s_%s' % (x['small_sp'],x['large_sp']),axis=1)
    loaded_df['edge_id_igraph_str'] = loaded_df['edge_id_igraph'].map(str)
    loaded_df_grp = loaded_df.groupby('undir_uv_sp').agg({
            'hour_flow': np.sum, 
            'voc': np.average,
            'vht': np.sum,
            'edge_id_igraph_str': lambda x: '-'.join(x),
            'geometry': 'first'}).reset_index()
    loaded_df_grp = loaded_df_grp.rename(columns={'hour_flow': 'undirected_hour_flow'})

    loaded_df_grp.to_csv(absolute_path+'/../2_ABM/output/speed_sensor/gis/undirected_case_{}.csv'.format(case), index=False)


def final_incre():

    edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges.csv'.format(folder, scenario))

    loaded_perfect = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/edge_flow_random0_probe1_sigma0.csv')
    loaded_no = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/edge_flow_random0_probe0_sigma10.csv')
    loaded_10pct = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/edge_flow_random0_probe0.1_sigma10.csv')
    loaded_1pct = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/edge_flow_random0_probe0.01_sigma10.csv')

    loaded_network(edges_df, loaded_perfect, 'perfect')
    sys.exit(0)

    agent_1 = pd.read_csv(absolute_path+'/output/speed_sensor/agent_routes_random0_probe0.1_sigma10.csv')
    agent_1['od_id'] = agent_1['origin'].map(str)+'-'+agent_1['destin'].map(str)
    print(agent_1.head())
    agent_2 = pd.read_csv(absolute_path+'/output/speed_sensor/agent_routes_random0_probe0.01_sigma10.csv')
    agent_2['od_id'] = agent_2['origin'].map(str)+'-'+agent_2['destin'].map(str)
    print('finished reading')
    
    route_length(edges_df, loaded_1, loaded_2, agent_1, agent_2)

def main():
    # random_seed = 0
    # sigma = 10
    #probe_ratio = 0.1

    edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges.csv'.format(folder, scenario))
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft', 'geometry']]
    #edges_df = edges_df.sort_values(by=['capacity'], ascending=False).reset_index()
    #edges_df['capacity_id'] = range(edges_df.shape[0])
    edges_df = edges_df.sort_values(by=['fft'], ascending=False).reset_index()
    edges_df['fft_id'] = range(edges_df.shape[0])
    # for probe_ratio in [0.1, 0.01]:
    #     for incre_id in range(19, 20):
    #         incre_edges_df = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/incre/edge_flow_incre{}_random{}_probe{}_sigma{}.csv'.format(incre_id, random_seed, probe_ratio, sigma))
    #         incre_edges_df = incre_edges_df.rename(columns={'hour_flow': 'vol_i{}_p{}'.format(incre_id+1, probe_ratio)})
    #         edges_df = pd.merge(edges_df, incre_edges_df[['edge_id_igraph', 'vol_i{}_p{}'.format(incre_id+1, probe_ratio)]], how='left', on='edge_id_igraph')
    #         edges_df['voc_i{}_p{}'.format(incre_id+1, probe_ratio)] = edges_df['vol_i{}_p{}'.format(incre_id+1, probe_ratio)]/edges_df['capacity']

            # incre_g = sio.mmread(absolute_path+'/../2_ABM/output/speed_sensor/incre_graph/network_i{}_r{}_p{}_s{}.mtx'.format(incre_id, random_seed, probe_ratio, sigma))
            # incre_g_df = pd.DataFrame({'start_sp': incre_g.row, 'end_sp': incre_g.col, 't_believe': incre_g.data})
            # incre_edges_df = pd.merge(incre_edges_df, incre_g_df, how='left', on=['start_sp', 'end_sp'])
            # incre_edges_df['v_believe']
    loaded_perfect = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/edge_flow_random0_probe1_sigma0.csv')
    loaded_perfect = loaded_perfect.rename(columns={'hour_flow': 'vol_perfect_info', 'vht': 'vht_perfect_info'})
    edges_df = pd.merge(edges_df, loaded_perfect[['edge_id_igraph', 'vol_perfect_info', 'vht_perfect_info']], how='left', on='edge_id_igraph')
    edges_df['voc_perfect_info'] = edges_df['vol_perfect_info']/edges_df['capacity']

    loaded_no = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/edge_flow_random0_probe0_sigma10.csv')
    loaded_no = loaded_no.rename(columns={'hour_flow': 'vol_no_info', 'vht': 'vht_no_info'})
    edges_df = pd.merge(edges_df, loaded_no[['edge_id_igraph', 'vol_no_info', 'vht_no_info']], how='left', on='edge_id_igraph')
    edges_df['voc_no_info'] = edges_df['vol_no_info']/edges_df['capacity']

    loaded_10pct = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/edge_flow_random0_probe0.1_sigma10.csv')
    loaded_10pct = loaded_10pct.rename(columns={'hour_flow': 'vol_10pct', 'vht': 'vht_10pct'})
    edges_df = pd.merge(edges_df, loaded_10pct[['edge_id_igraph', 'vol_10pct', 'vht_10pct']], how='left', on='edge_id_igraph')
    edges_df['voc_10pct'] = edges_df['vol_10pct']/edges_df['capacity']

    loaded_1pct = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/edge_flow_random0_probe0.01_sigma10.csv')
    loaded_1pct = loaded_1pct.rename(columns={'hour_flow': 'vol_1pct', 'vht': 'vht_1pct'})
    edges_df = pd.merge(edges_df, loaded_1pct[['edge_id_igraph', 'vol_1pct', 'vht_1pct']], how='left', on='edge_id_igraph')
    edges_df['voc_1pct'] = edges_df['vol_1pct']/edges_df['capacity']

    #edges_df['capacity_grp'] = edges_df.apply(lambda x: int(x['capacity_id']/2), axis=1)
    edges_df['fft_grp'] = edges_df.apply(lambda x: int(x['fft_id']/10), axis=1)
    # edges_df_grp = edges_df.groupby('capacity_grp').agg({
    #     'capacity_id': 'first',
    #     'vol_1': np.mean, 'voc_1': np.mean, 'vol_2': np.mean, 'voc_2': np.mean,
    #     'vol_3': np.mean, 'voc_3': np.mean, 'vol_4': np.mean, 'voc_4': np.mean,
    #     'vol_5': np.mean, 'voc_5': np.mean, 'vol_6': np.mean, 'voc_6': np.mean,
    #     'vol_7': np.mean, 'voc_7': np.mean, 'vol_8': np.mean, 'voc_8': np.mean,
    #     'vol_9': np.mean, 'voc_9': np.mean, 'vol_10': np.mean, 'voc_10': np.mean,
    #     'vol_11': np.mean, 'voc_11': np.mean, 'vol_12': np.mean, 'voc_12': np.mean,
    #     'vol_13': np.mean, 'voc_13': np.mean, 'vol_14': np.mean, 'voc_14': np.mean,
    #     'vol_15': np.mean, 'voc_15': np.mean, 'vol_16': np.mean, 'voc_16': np.mean,
    #     'vol_17': np.mean, 'voc_17': np.mean, 'vol_18': np.mean, 'voc_18': np.mean,
    #     'vol_19': np.mean, 'voc_19': np.mean, 'vol_20': np.mean, 'voc_20': np.mean
    #     }).reset_index()
    # edges_df_grp = edges_df.groupby('fft_grp').agg({
    #     'fft_id': 'first', 'capacity': np.mean,
    #     'vol_perfect_info': np.mean, 'vol_no_info': np.mean,
    #     'vol_10pct': np.mean, 'vol_1pct': np.mean,
    #     'voc_perfect_info': np.mean, 'voc_no_info': np.mean,
    #     'voc_10pct': np.mean, 'voc_1pct': np.mean
    #     })

    fig, ax = plt.subplots(1)

    # color = iter(plt.cm.Spectral(np.linspace(0, 1, 20)))
    # for incre_id in range(1, 21):
    # #for sigma in [1]:
    #     c = next(color)
    #     ax.plot('capacity_id', 'vol_{}'.format(incre_id), data=edges_df_grp, lw=0, marker='.', markersize=0.5, c=c, alpha=1, label='{}'.format(incre_id))
    # #plt.xscale('log')
    # plt.yscale('log')
    # plt.legend(title='incre_id')
    # plt.xlabel('capacity_id')
    # plt.ylabel('volume')
    # plt.show()

    # ax.plot('fft_id', 'voc_10pct', data=edges_df_grp, lw=0.2, marker='.', ms=0, c='red', label='10pct')
    # ax.plot('fft_id', 'voc_1pct', data=edges_df_grp, lw=0.2, marker='.', ms=0, c='blue', label='1pct')
    # ax.plot('fft_id', 'voc_perfect_info', data=edges_df_grp, lw=0.2, marker='.', ms=0, c='green', label='perfect_info')
    # ax.plot('fft_id', 'voc_no_info', data=edges_df_grp, lw=0.2, marker='.', ms=0, c='black', label='no_info')
    
    # #ax.plot('capacity_id', 'capacity', data=edges_df_grp, lw=1, marker='.', ms=0, c='black', label='capacity')
    # plt.yscale('log')
    # plt.legend()
    # plt.xlabel('fft_id')
    # plt.ylabel('voc')
    # plt.show()

    ax.hist('vht_perfect_info', data=edges_df, bins=1000, histtype=u'step', lw=2, color='green', label='perfect_info')
    ax.hist('vht_no_info', data=edges_df, bins=1000, histtype=u'step', lw=2, color='black', label='no_info')
    ax.hist('vht_10pct', data=edges_df, bins=1000, histtype=u'step', lw=2, color='red', label='10pct_info')
    ax.hist('vht_1pct', data=edges_df, bins=1000, histtype=u'step', lw=2, color='blue', label='1pct_info')
    plt.legend()
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel('vht')
    plt.ylabel('frequency')
    plt.show()
        

if __name__ == '__main__':
    main()