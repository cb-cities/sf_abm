import os 
import gc 
import sys 
import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 

absolute_path = os.path.dirname(os.path.abspath(__file__))

def main():
    #edges_df[['edge_id_igraph', 'true_vol', 'tot_vol', 't_avg']].to_csv('{}/edges_df/edges_df_YR{}_DY{}_HR{}_qt{}_res{}_r{}.csv'.format(outdir, year, day, hour, quarter, residual, random_seed), index=False)

    edges_df = pd.read_csv(absolute_path+'/../0_network/data/sf_overpass/edges_elevation.csv')
    edges_df = edges_df[['edge_id_igraph', 'length']]

    compare = []

    random_seed = 0
    outdir = absolute_path+'/../residual_demand_sim/output'
    for year in [0]:
        for day in [2]:
            for hour in range(3, 27):
                for quarter in range(4):

                    res = pd.read_csv('{}/edges_df/edges_df_hpc/edges_df_YR{}_DY{}_HR{}_qt{}_res1_r{}.csv'.format(outdir, year, day, hour, quarter, random_seed))
                    res = pd.merge(res, edges_df, on='edge_id_igraph')
                    res_vht = np.sum(res['true_vol']*res['t_avg'])/3600
                    res_vkmt = np.sum(res['true_vol']*res['length'])/1000
                    compare.append([year, day, hour, quarter, 'res', res_vht, res_vkmt])

                    non_res = pd.read_csv('{}/edges_df/edges_df_hpc/edges_df_YR{}_DY{}_HR{}_qt{}_res0_r{}.csv'.format(outdir, year, day, hour, quarter, random_seed))
                    non_res = pd.merge(non_res, edges_df, on='edge_id_igraph')
                    non_res_vht = np.sum(non_res['true_vol']*non_res['t_avg'])/3600
                    non_res_vkmt = np.sum(non_res['true_vol']*non_res['length'])/1000
                    compare.append([year, day, hour, quarter, 'non_res', non_res_vht, non_res_vkmt])

    compare_df = pd.DataFrame(compare, columns=['year', 'day', 'hour', 'quarter', 'res_b', 'vht', 'vkmt'])
    compare_df['time'] = compare_df['hour'] + compare_df['quarter'] * 0.25
    print(compare_df.groupby('res_b').agg({'vht': np.sum, 'vkmt': np.sum}))
    sys.exit(0)

    fig, ax = plt.subplots()
    res_data = compare_df[compare_df['res_b']=='res']
    non_res_data = compare_df[compare_df['res_b']=='non_res']
    # ax.plot(res_data['time'], res_data['vht'], c='blue', label='residual')
    # ax.plot(non_res_data['time'], non_res_data['vht'], c='red', label='no residual')
    ax.plot(res_data['time'], res_data['vkmt'], c='blue', label='residual')
    ax.plot(non_res_data['time'], non_res_data['vkmt'], c='red', label='no residual')

    plt.legend()
    plt.xticks(np.arange(3, 27, 1.0))
    plt.xlabel('hour')
    # plt.ylabel('VHT per quarter hour')
    plt.ylabel('VKMT per quarter hour')
    plt.show()

def indiv_street_voc(quarter_counts=4):

    ### Get attributes and geometry of each edge
    edges_df = pd.read_csv(absolute_path+'/../0_network/data/sf_overpass/edges_elevation.csv')
    edges_df = edges_df[['edge_id_igraph', 'length', 'capacity']]

    high_voc_list = []

    year = 0
    day = 2
    residual = 1
    random_seed = 0
    quarter_counts = 4
    outdir = absolute_path+'/../residual_demand_sim/output'
    for hour in range(3, 27):
        for quarter in [0,1,2,3]:

            res = pd.read_csv('{}/edges_df/edges_df_hpc/edges_df_YR{}_DY{}_HR{}_qt{}_res1_r{}.csv'.format(outdir, year, day, hour, quarter, random_seed))
            res = pd.merge(res, edges_df, on='edge_id_igraph')
            res['voc'] = res['true_vol'] * quarter_counts / res['capacity']
            res['vkmt'] = res['true_vol'] * res['length']
            res['hour'] = hour
            res['quarter'] = quarter
            # high_voc_list.append(res.nlargest(1, 'voc'))
            high_voc_list.append(res.nlargest(4, 'vkmt'))

            non_res = pd.read_csv('{}/edges_df/edges_df_hpc/edges_df_YR{}_DY{}_HR{}_qt{}_res0_r{}.csv'.format(outdir, year, day, hour, quarter, random_seed))
            non_res = pd.merge(non_res, edges_df, on='edge_id_igraph')
            non_res['voc'] = non_res['true_vol'] * quarter_counts / non_res['capacity']
            non_res['vkmt'] = non_res['true_vol'] * non_res['length']
            non_res['hour'] = hour
            non_res['quarter'] = quarter
            # high_voc_list.append(non_res.nlargest(1, 'voc'))
            high_voc_list.append(non_res.nlargest(4, 'vkmt'))

            gc.collect()

    high_voc_df = pd.concat(high_voc_list, sort=False)
    high_voc_grp = high_voc_df.groupby('edge_id_igraph').agg({'voc': [np.max, np.size]}).reset_index()
    # high_voc_select = high_voc_grp[(high_voc_grp[('voc', 'amax')]>2) | (high_voc_grp[('voc', 'size')]>5)]['edge_id_igraph'].values.tolist()
    high_voc_select = high_voc_grp['edge_id_igraph'].values.tolist()
    print('edges with highest voc: ', high_voc_select)
    # sys.exit(0)
    high_voc_edges_df = edges_df[edges_df['edge_id_igraph'].isin(high_voc_select)]
    high_voc_collect = []

    for hour in range(3, 27):
        for quarter in [0,1,2,3]:

            res = pd.read_csv('{}/edges_df/edges_df_hpc/edges_df_YR{}_DY{}_HR{}_qt{}_res1_r{}.csv'.format(outdir, year, day, hour, quarter, random_seed))
            res = pd.merge(res, high_voc_edges_df, on='edge_id_igraph', how='right')
            res['voc'] = res['true_vol'] * quarter_counts / res['capacity']
            res['time'] = hour + 0.25 * quarter
            res['res_b'] = 'res'
            high_voc_collect.append(res)

            non_res = pd.read_csv('{}/edges_df/edges_df_hpc/edges_df_YR{}_DY{}_HR{}_qt{}_res0_r{}.csv'.format(outdir, year, day, hour, quarter, random_seed))
            non_res = pd.merge(non_res, high_voc_edges_df, on='edge_id_igraph', how='right')
            non_res['voc'] = non_res['true_vol'] * quarter_counts / non_res['capacity']
            non_res['time'] = hour + 0.25 * quarter
            non_res['res_b'] = 'non_res'
            high_voc_collect.append(non_res)

            gc.collect()

    collect_df = pd.concat(high_voc_collect, sort=False)
    print(np.unique(collect_df['edge_id_igraph']))

    fig, axs = plt.subplots(5, 2, figsize=(20, 15))
    # fig, axs = plt.subplots(4, 2, figsize=(16, 15))
    ax_i = 0
    for nm, grp in collect_df.groupby('edge_id_igraph'):
        res_data = grp[grp['res_b']=='res']
        non_res_data = grp[grp['res_b']=='non_res']
        axs[ax_i//2, ax_i%2].plot(res_data['time'], res_data['voc'], c='blue', label='residual')
        axs[ax_i//2, ax_i%2].plot(non_res_data['time'], non_res_data['voc'], c='red', label='no residual')
        axs[ax_i//2, ax_i%2].set_title('road link {}'.format(nm))
        ax_i += 1

    for ax in axs.flat:
        # ax.set(xlabel='time', ylabel='voc 1/4 hour', xticks=np.arange(3, 27, 1), ylim=[0, 3.5])
        ax.set(xlabel='time', ylabel='voc 1/4 hour', xticks=np.arange(3, 27, 1), ylim=[0, 1.5])
    for ax in axs.flat:
        ax.label_outer()

    plt.legend(loc=(-0.3, -0.5), ncol=2)
    plt.show()

if __name__ == '__main__':
    #main()
    indiv_street_voc()