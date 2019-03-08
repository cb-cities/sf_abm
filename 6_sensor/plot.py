import os
import sys
import pandas as pd 
import numpy as np 
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
from matplotlib.lines import Line2D
from pandas.plotting import parallel_coordinates
import gc 

absolute_path = os.path.dirname(os.path.abspath(__file__))
pd.set_option('display.max_columns', 10)
plt.rcParams.update({'font.size': 18, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

folder = 'sf_overpass'
scenario = 'original'

def plot_hourly_trend(var):

    fig, ax = plt.subplots(1)
    fig.set_size_inches(10.5, 5.5)
    color = iter(cm.rainbow(np.linspace(0, 1, 6)))
    for probe_ratio in [1.0, 0.1, 0.01, 0.005, 0.001, 0.0]:

        c = next(color)
        summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor/summary_df/summary_p{}.csv'.format(probe_ratio))
        summary_df['VHT'] = summary_df['VHT']/3600
        summary_df['VKMT'] = summary_df['VKMT']/1000
        summary_grp = summary_df.groupby('hour').agg({var: [np.mean, np.std]}).reset_index()
        summary_grp.columns = ['_'.join(col).strip() for col in summary_grp.columns.values]

        summary_grp = summary_grp.loc[(summary_grp['hour_']>10) & (summary_grp['hour_']<24)].copy()
        ax.plot('hour_', '{}_mean'.format(var), data=summary_grp, label='{}'.format(probe_ratio), c=c)
        ax.fill_between(summary_grp['hour_'], 
            summary_grp['{}_mean'.format(var)] - summary_grp['{}_std'.format(var)],
            summary_grp['{}_mean'.format(var)] + summary_grp['{}_std'.format(var)],
            facecolor=c, alpha=0.5)
        #plt.errorbar('hour_', '{}_mean'.format(var), '{}_std'.format(var), data=summary_grp, fmt='.-', lw=0.5, color=c, elinewidth=2, ecolor=c, capsize=10, capthick=1)
    
    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0, box.y0, box.width*0.9, box.height])

    plt.yscale('log')
    ax.set_title('Hourly {} (Friday)'.format(var))
    ax.legend(title='Probe ratio', bbox_to_anchor=(1, 0.7))
    ax.set_xlabel('Hour')
    ax.set_ylabel('{}'.format(var))
    plt.savefig(absolute_path+'/Figs/hourly_{}.png'.format(var))
    #plt.show()

def plot_pcp(var, hour, random_seed):

    pcp_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges_elevation.csv'.format(folder, scenario))
    pcp_df = pcp_df[['edge_id_igraph']]

    probe_ratio_list = [1.0, 0.1, 0.01, 0.005, 0.001, 0.0]
    for probe_ratio in probe_ratio_list:
        edges_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor/edges_df/edges_df_DY4_HR{}_r{}_p{}.csv'.format(hour, random_seed, probe_ratio))
        edges_df['{}_{}'.format(var, probe_ratio)] = edges_df['{}'.format(var)]
        pcp_df = pd.merge(pcp_df, edges_df[['edge_id_igraph', '{}_{}'.format(var, probe_ratio)]], on='edge_id_igraph', how='left')

    pcp_df['std'] = pcp_df.set_index('edge_id_igraph').std(axis=1)
    pcp_df = pcp_df.loc[pcp_df['std']>1].copy()
    #pcp_df['highlight'] = pd.qcut(pcp_df['std'], [0, 0.9, 0.99, 0.999, 1], labels=['neglect', 'low', 'mid', 'top'])
    pcp_df['highlight'] = pd.qcut(pcp_df['{}_1.0'.format(var)], [0, 0.9, 0.99, 0.999, 1], labels=['neglect', 'low', 'mid', 'top'])

    fig, ax = plt.subplots()
    fig.set_size_inches(18, 8)
    ### Filter column
    var_cols = [col for col in pcp_df.columns if str(var) in col]
    var_cols.append('highlight')

    #parallel_coordinates(pcp_df[var_cols], 'highlight', color=[[1,0,0,0.05], [0,1,0,0.05], [0,0,1,0.05], [1,0,1, 1]])
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='neglect', var_cols], 'highlight', color='k', alpha=0.01)
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='low', var_cols], 'highlight', color='c', alpha=0)
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='mid', var_cols], 'highlight', color='b', alpha=0)
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='top', var_cols], 'highlight', color='r', alpha=0)

    ### Shrink current axis's height
    box = ax.get_position()
    ax.set_position([box.x0-box.width*0.1, box.y0+box.height*0.14, box.width*1.2, box.height*0.9])

    plt.xlabel("Probe ratio")
    ax.set_xticklabels(probe_ratio_list)
    plt.ylabel("Hourly link volume (log)")
    plt.yscale('log')
    plt.title("Change in road link usage by probe ratio (0-90%), Friday {} o'clock".format(hour))

    # remove the pandas legend
    plt.gca().legend_.remove()
    # add new legend
    topHandle = Line2D([],[], color='red', ls="-", label=">99.9%")
    midHandleOne = Line2D([],[], color='blue', ls="-", label="99~99.9%")
    lowHandle = Line2D([],[], color='c', ls="-", label="90~99%")
    negHandle = Line2D([],[], color='black', ls="-", label="0~90%")
    plt.legend(handles=[topHandle, midHandleOne,lowHandle, negHandle], title='Percentile, hourly link volume in the perfect information case', bbox_to_anchor=(0.5, -0.3), loc='lower center', ncol=4, columnspacing=1.5, labelspacing=0.8)
    plt.savefig(absolute_path+'/Figs/pcp_{}_HR{}_3.png'.format(var, hour))


if __name__ == '__main__':
    #plot_hourly_trend('max10')
    plot_pcp('true_flow', 18, 0) ### max flow hour is 18
