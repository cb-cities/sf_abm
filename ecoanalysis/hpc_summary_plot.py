import os
import sys
import numpy as np 
import pandas as pd 
import matplotlib 
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
import matplotlib.ticker as mtick

absolute_path = os.path.dirname(os.path.abspath(__file__))

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

def traf_plot():
    pass

def emi_plot():

    emi_df_list = []
    for case in ['nr', 'em', 'er']:
        case_emi_df = pd.read_csv(absolute_path + '/output_LCA2020/summary/hpc/emi_summary_c{}.csv'.format(case))
        emi_df_list.append(case_emi_df)
    emi_df = pd.concat(emi_df_list, sort=False)

    for metric in ['emi_total', 'emi_local', 'emi_highway', 'vht_total', 'vht_local', 'vht_highway', 'vkmt_total', 'vkmt_local', 'vkmt_highway']:
        base = emi_df[(emi_df['year']==0) & (emi_df['case']=='nr')][metric].iloc[0]
        emi_df['{}_pct'.format(metric)] = (emi_df[metric] - base)/base

    fig, ax = plt.subplots(3, 3, figsize=(20, 17))
    color = iter(cm.rainbow(np.linspace(0.2, 1, 3)))
    label_dict = {'nr': 'do nothing', 'em': 'eco-maintenance', 'er': 'eco-routing'}
    marker_dict = {'nr': 's', 'em': '*', 'er': '^'}

    for nm, grp in emi_df.groupby('case'):

        c = next(color)

        ax[0, 0].scatter(grp['year'], grp['emi_total_pct'], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none', label=label_dict[nm])
        ax[0, 1].scatter(grp['year'], grp['emi_local_pct'], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')
        ax[0, 2].scatter(grp['year'], grp['emi_highway_pct'], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')

        ax[1, 0].scatter(grp['year'], grp['vht_total_pct'], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')
        ax[1, 1].scatter(grp['year'], grp['vht_local_pct'], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')
        ax[1, 2].scatter(grp['year'], grp['vht_highway_pct'], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')

        ax[2, 0].scatter(grp['year'], grp['vkmt_total_pct'], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')
        ax[2, 1].scatter(grp['year'], grp['vkmt_local_pct'], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')
        ax[2, 2].scatter(grp['year'], grp['vkmt_highway_pct'], edgecolors=c, marker=marker_dict[nm], s=100, facecolors='none')

        # ax[2, 0].plot(emi_df['year'], emi_df['pci_average'], c=c)
        # ax[2, 1].plot(emi_df['year'], emi_df['pci_local'], c=c)
        # ax[2, 2].plot(emi_df['year'], emi_df['pci_highway'], c=c)

    def y_fmt(x, y):
        return '{:2.0f}%'.format(x*100)

    for i in range(9):
        ax[i//3, i%3].yaxis.set_major_formatter(mtick.FuncFormatter(y_fmt))
        ax[i//3, i%3].set(xticks=np.arange(0, 11, 1))

    ax[0, 0].set(title='emi_total', ylabel='% change')
    ax[0, 1].set(title='emi_local')
    ax[0, 2].set(title='emi_highway')

    ax[1, 0].set(title='vht_total', ylabel='% change')
    ax[1, 1].set(title='vht_local')
    ax[1, 2].set(title='vht_highway')

    ax[2, 0].set(title='vkmt_total', xlabel='year', ylabel='% change')
    ax[2, 1].set(title='vkmt_local', xlabel='year')
    ax[2, 2].set(title='vkmt_highway', xlabel='year')

    # ax[2, 0].set(title='pci_average', xticks=np.arange(0, 11, 1))
    # ax[2, 1].set(title='pci_local', xticks=np.arange(0, 11, 1))
    # ax[2, 2].set(title='pci_highway', xticks=np.arange(0, 11, 1))

    handles, labels = ax[0, 0].get_legend_handles_labels()
    fig.legend([handles[2], handles[0], handles[1]], [labels[2], labels[0], labels[1]], loc=(0.37, 0.02), ncol=3)
    fig.tight_layout(rect=(0, 0.05, 1, 1), h_pad=3)

    # plt.show()
    plt.savefig(absolute_path+'/output_LCA2020/summary_plot.png')


if __name__ == '__main__':
    emi_plot()