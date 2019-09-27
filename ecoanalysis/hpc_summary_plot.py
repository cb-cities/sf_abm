import os
import sys
import numpy as np 
import pandas as pd 
import matplotlib 
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
import matplotlib.ticker as mtick

absolute_path = os.path.dirname(os.path.abspath(__file__))

pd.set_option('display.max_columns', 15)

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

def traf_plot():
    pass

def emi_plot():

    emi_df_list = []
    for case in ['nr', 'em', 'er', 'ps']:
        case_emi_df = pd.read_csv(absolute_path + '/output_LCA2020/summary/hpc/emi_summary_c{}.csv'.format(case))
        emi_df_list.append(case_emi_df)
    emi_df = pd.concat(emi_df_list, sort=False)
    emi_df['vmt_total'] = emi_df['vkmt_total']/1.60934
    emi_df['vmt_local'] = emi_df['vkmt_local']/1.60934
    emi_df['vmt_highway'] = emi_df['vkmt_highway']/1.60934

    metric = 'emi'
    pct=False
    metric_text = metric
    if metric == 'emi': metric_text = 'CO\u2082 emission'
    if metric == 'vht': metric_text = 'VHT'
    if metric == 'vmt': metric_text = 'VMT'

    if pct == True:
        for sub_metric in ['{}_total'.format(metric), '{}_local'.format(metric), '{}_highway'.format(metric)]:
            base = emi_df[(emi_df['year']==0) & (emi_df['case']=='nr')][sub_metric].iloc[0]
            emi_df['{}_pct'.format(sub_metric)] = (emi_df[sub_metric] - base)/base

    fig, ax = plt.subplots(1, 3, figsize=(15, 5))
    color = iter(cm.rainbow(np.linspace(0.2, 1, 4)))
    label_dict = {'nr': 'do nothing', 'em': 'eco-maintenance', 'er': 'eco-routing', 'ps': 'peak-spreading'}
    marker_dict = {'nr': 's', 'em': '*', 'er': '^', 'ps': 'o'}

    for nm, grp in emi_df.groupby('case'):

        c = next(color)

        if metric == 'pci':
            ax[1].scatter(grp['year'], grp['{}_local'.format(metric)], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none', label=label_dict[nm])
        elif pct == True:
            ax[0].scatter(grp['year'], grp['{}_total_pct'.format(metric)], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')
            ax[1].scatter(grp['year'], grp['{}_local_pct'.format(metric)], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none', label=label_dict[nm])
            ax[2].scatter(grp['year'], grp['{}_highway_pct'.format(metric)], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')
        else:
            ax[0].scatter(grp['year'], grp['{}_total'.format(metric)], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')
            ax[1].scatter(grp['year'], grp['{}_local'.format(metric)], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none', label=label_dict[nm])
            ax[2].scatter(grp['year'], grp['{}_highway'.format(metric)], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')

    def y_fmt(x, y):
        return '{:2.0f}%'.format(x*100)

    if pct == True:
        for i in range(3):
            ax[i].yaxis.set_major_formatter(mtick.FuncFormatter(y_fmt))
            ax[i].set(xticks=np.arange(0, 11, 1))

    if metric == 'pci':
        ax[1].set(title='{}, local roads'.format(metric_text), xlabel='year', ylabel='PCI')
    else:
        ax[0].set(title='(a) {}, total'.format(metric_text), xlabel='year', ylabel='% change')
        ax[1].set(title='(b) {}, local roads'.format(metric_text), xlabel='year')
        ax[2].set(title='(c) {}, highway'.format(metric_text), xlabel='year')

    handles, labels = ax[1].get_legend_handles_labels()
    fig.legend([handles[2], handles[0], handles[1], handles[3]], [labels[2], labels[0], labels[1], labels[3]], loc='lower center', ncol=4)
    fig.tight_layout(rect=(0, 0.05, 1, 1), h_pad=3)

    plt.show()
    #plt.savefig(absolute_path+'/output_LCA2020/summary_plot_{}.png'.format(metric))

def iri_plot():

    emi_df_list = []
    for iri in ['0.0', '0.01', '0.03', '0.1', '0.15', '0.2', '0.25', '0.3', '0.5']:
        iri_emi_df = pd.read_csv(absolute_path + '/output_LCA2020/summary/hpc/emi_summary_cer_i{}.csv'.format(iri))
        emi_df_list.append(iri_emi_df)
    emi_df = pd.concat(emi_df_list, sort=False)
    emi_df['vmt_total'] = emi_df['vkmt_total']/1.60934
    emi_df['vmt_local'] = emi_df['vkmt_local']/1.60934
    emi_df['vmt_highway'] = emi_df['vkmt_highway']/1.60934
    print(emi_df[['iri_impact', 'vmt_total', 'vmt_local', 'vmt_highway']])

    # plt.scatter(emi_df['iri_impact'], emi_df['vht_highway'])
    # plt.show()

def iri_map():

    preprocessing_df = pd.read_csv(absolute_path + '/output_LCA2020/preprocessing.csv')
    
    iri_emi_df_1 = pd.read_csv(absolute_path + '/output_LCA2020/edges_df/HPC/edges_df_YR0_DY2_HR18_qt3_res1_cer_i{}_r0.csv'.format(0.01))
    iri_emi_df = pd.merge(preprocessing_df[['edge_id_igraph', 'juris', 'geometry']], iri_emi_df_1[['edge_id_igraph', 'true_vol']], on='edge_id_igraph', how='left')

    iri_emi_df_2 = pd.read_csv(absolute_path + '/output_LCA2020/edges_df/HPC/edges_df_YR0_DY2_HR18_qt3_res1_cer_i{}_r0.csv'.format(0.5))
    iri_emi_df = pd.merge(iri_emi_df, iri_emi_df_2[['edge_id_igraph', 'true_vol']], on='edge_id_igraph', how='left', suffixes=['_1', '_50'])
    iri_emi_df['diff_vol'] = iri_emi_df['true_vol_50'] - iri_emi_df['true_vol_1']
    print(iri_emi_df.groupby('juris')['diff_vol'].describe())

    #iri_emi_df.to_csv(absolute_path + '/output_LCA2020/edges_df/HPC/emi_diff_geom.csv')

if __name__ == '__main__':
    emi_plot()
    #iri_plot()
    #iri_map()