import os
import sys
import pandas as pd 
import numpy as np 
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

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

def plot_peak_hour(var, hour):

    plot_df = pd.DataFrame(columns=['probe_ratio', 'cov', 'random_seed', 'AMT'])
    for probe_ratio in [1.0, 0.1, 0.01, 0.005]:
        ### COV 0 case
        summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor/summary_df/summary_p{}.csv'.format(probe_ratio))
        summary_df['VHT'] = summary_df['VHT']/3600
        summary_df['VKMT'] = summary_df['VKMT']/1000
        summary_df['AMT'] = summary_df['VHT']/summary_df['hour_demand']*60 ### Agent minutes travelled
        summary_df['cov'] = 0.0

        plot_df = pd.concat([plot_df, summary_df.loc[summary_df['hour']==hour, ['probe_ratio', 'cov', 'random_seed', 'AMT']]])

        for cov in [0.5, 1.0, 2.0]:
            summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor_cov/summary_df/summary_p{}_cov{}.csv'.format(probe_ratio, cov))
            summary_df['VHT'] = summary_df['VHT']/3600
            summary_df['VKMT'] = summary_df['VKMT']/1000
            summary_df['AMT'] = summary_df['VHT']/summary_df['hour_demand']*60 ### Agent minutes travelled
            summary_df['cov'] = cov

            plot_df = pd.concat([plot_df, summary_df.loc[summary_df['hour']==hour, ['probe_ratio', 'cov', 'random_seed', 'AMT']]])

    plot_df['probe_ratio_str'] = plot_df['probe_ratio'].astype(str)
    fig, ax = plt.subplots(1)
    fig.set_size_inches(10.5, 5.5)
    color = iter(cm.viridis(np.linspace(0, 1, 4)))

    for cov in [0.0, 0.5, 1.0, 2.0]:
        c=next(color)
        slice_df = plot_df.loc[plot_df['cov']==cov].copy()
        slice_pivot = slice_df.pivot(columns='probe_ratio_str', values=var)
        slice_labels = slice_pivot.columns.values
        slice_values = slice_pivot.values

        boxprops = dict(color=c, linewidth=1, alpha=0.5)
        flierprops = dict(markerfacecolor=c, marker='s', markeredgecolor=c)
        box = ax.boxplot(slice_values, labels=slice_labels, boxprops=boxprops, widths=0.1, patch_artist=True, flierprops=flierprops)
        for patch in box['boxes']:
            patch.set_facecolor(c)

        slice_grp = slice_df.groupby('probe_ratio_str', as_index=False).agg({var: percentile(50)}).reset_index()
        ax.plot([1,2,3,4], 'AMT', data=slice_grp, label='{}'.format(cov), c=c)

    ax.set_title("Average agent travel time (Friday {} o'clock)".format(hour), y=1.05)
    ax.legend(title='Probe info\nvariability\n(coef. of\nvariation)', bbox_to_anchor=(1, 1))
    ax.set_xlabel('Probe ratio')
    #plt.xscale('log')
    ax.set_ylabel('Average agent travel time (min)')
    #ax.set_ylim([30, 42])
    #plt.yscale('log')
    plt.savefig(absolute_path+'/Figs/{}oc_{}_quantiles.png'.format(hour, var))
    #plt.show()


def plot_hourly_amt(var, probe_ratio):

    fig, ax = plt.subplots(1)
    fig.set_size_inches(10.5, 5.5)
    color = iter(cm.viridis(np.linspace(0, 1, 4)))

    for cov in [0, 0.5, 1.0, 2.0]:

        c = next(color)
        if cov == 0:
            summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor/summary_df/summary_p{}.csv'.format(probe_ratio))
        else:
            summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor_cov/summary_df/summary_p{}_cov{}.csv'.format(probe_ratio, cov))
        summary_df['VHT'] = summary_df['VHT']/3600
        summary_df['VKMT'] = summary_df['VKMT']/1000
        summary_df['AMT'] = summary_df['VHT']/summary_df['hour_demand']*60 ### Agent minutes travelled

        pivot_labels = []
        pivot_positions = []
        pivot_values = []
        for nm, grp in summary_df.groupby('hour'):
            pivot_labels.append(int(nm))
            pivot_positions.append(int(nm))
            pivot_values.append(grp[var].values)

        boxprops = dict(color=c, linewidth=1, alpha=0.5)
        flierprops = dict(markerfacecolor=c, marker='s', markeredgecolor=c)
        box = ax.boxplot(pivot_values, labels=pivot_labels, boxprops=boxprops, widths=0.1, patch_artist=True, flierprops=flierprops, positions=pivot_positions)
        for patch in box['boxes']:
            patch.set_facecolor(c)

        summary_grp = summary_df.groupby('hour').agg({var: [percentile(50)]}).reset_index()
        ax.plot(pivot_positions, summary_grp['{}'.format(var)], label='{}'.format(cov), c=c)

    
    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0, box.y0, box.width*0.9, box.height])

    ax.set_title('Hourly average agent travel time (Friday), Probe ratio {}'.format(probe_ratio), y=1.05)
    ax.legend(title='Probe coef.\nof variation', bbox_to_anchor=(1, 0.7))
    ax.set_xlabel('Hour')
    ax.set_ylabel('Average agent travel time (min)')
    ax.set_xticks(np.arange(3, 27, 3))
    ax.set_xticklabels(np.arange(3, 27, 3))
    ax.set_ylim([15, 45])
    plt.savefig(absolute_path+'/Figs/hourly_{}_p{}_quantiles.png'.format(var, probe_ratio))
    #plt.show()

def plot_hourly_trend(var, cov, ylabel, zoom=False):

    fig, ax = plt.subplots(1)
    fig.set_size_inches(10.5, 5.5)
    color = iter(cm.rainbow(np.linspace(0, 1, 6)))
    probe_ratio_list = [1.0, 0.1, 0.01, 0.005, 0.001, 0.0]

    for probe_ratio in probe_ratio_list:

        c = next(color)

        ### COV 0 case
        summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor/summary_df/summary_p{}.csv'.format(probe_ratio, cov))
        summary_df['VHT'] = summary_df['VHT']/3600
        summary_df['VKMT'] = summary_df['VKMT']/1000
        summary_df['AMT'] = summary_df['VHT']/summary_df['hour_demand']*60 ### Agent minutes travelled

        pivot_labels = []
        pivot_positions = []
        pivot_values = []
        for nm, grp in summary_df.groupby('hour'):
            pivot_labels.append(int(nm))
            pivot_positions.append(int(nm))
            pivot_values.append(grp[var].values)

        boxprops = dict(color=c, linewidth=1, alpha=0.5)
        flierprops = dict(markerfacecolor=c, marker='s', markeredgecolor=c)
        box = ax.boxplot(pivot_values, labels=pivot_labels, boxprops=boxprops, widths=0.3, patch_artist=True, flierprops=flierprops, positions=pivot_positions)
        for patch in box['boxes']:
            patch.set_facecolor(c)

        summary_grp = summary_df.groupby('hour').agg({var: percentile(50)}).reset_index()
        ax.plot(pivot_positions, summary_grp['{}'.format(var)], label='{}%'.format(probe_ratio*100), c=c, linestyle='-')
    
    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0-box.width*0.03, box.y0, box.width*0.87, box.height])

    plt.yscale('log')
    ax.set_title('Hourly {} (Friday), COV {}'.format(ylabel, cov), y=1.05)
    l = ax.legend(title='Probe\npenetration\nrate', bbox_to_anchor=(1, 0.85))
    plt.setp(l.get_title(), multialignment='center')
    ax.set_xlabel('Hour')
    ax.set_xticks(np.arange(3, 27, 3))
    ax.set_xticklabels(np.arange(3, 27, 3))
    ax.set_ylabel('{}'.format(ylabel))
    plt.savefig(absolute_path+'/Figs/hourly_{}_cov{}_zoom{}_quantiles.png'.format(var, cov, zoom))
    #plt.show()

def plot_pcp(var, hour, random_seed):

    pcp_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges_elevation.csv'.format(folder, scenario))
    pcp_df = pcp_df[['edge_id_igraph']]

    probe_ratio_list = [1, 0.01, 0.001] # [1.0, 0.1, 0.01, 0.005, 0.001, 0.0]
    for probe_ratio in probe_ratio_list:
        edges_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor/edges_df/edges_df_DY4_HR{}_r{}_p{}.csv'.format(hour, random_seed, probe_ratio))
        edges_df['{}_{}'.format(var, probe_ratio)] = edges_df['{}'.format(var)]
        pcp_df = pd.merge(pcp_df, edges_df[['edge_id_igraph', '{}_{}'.format(var, probe_ratio)]], on='edge_id_igraph', how='left')

    pcp_df['std'] = pcp_df.set_index('edge_id_igraph').std(axis=1)
    pcp_df = pcp_df.loc[pcp_df['std']>1].copy()
    pcp_df['highlight'] = pd.qcut(pcp_df['std'], [0, 0.9, 0.99, 0.999, 1], labels=['neglect', 'low', 'mid', 'top'])

    fig, ax = plt.subplots()
    fig.set_size_inches(18, 8)
    ### Filter column
    var_cols = [col for col in pcp_df.columns if str(var) in col]
    var_cols.append('highlight')

    #parallel_coordinates(pcp_df[var_cols], 'highlight', color=[[1,0,0,0.05], [0,1,0,0.05], [0,0,1,0.05], [1,0,1, 1]])
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='neglect', var_cols], 'highlight', color='k', alpha=0.01)
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='low', var_cols], 'highlight', color='c', alpha=0.1)
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='mid', var_cols], 'highlight', color='b', alpha=0.4)
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='top', var_cols], 'highlight', color='r', alpha=1)

    ### Shrink current axis's height
    box = ax.get_position()
    ax.set_position([box.x0-box.width*0.1, box.y0+box.height*0.14, box.width*1.2, box.height*0.9])

    plt.xlabel("Probe ratio")
    ax.set_xticklabels(probe_ratio_list)
    plt.ylabel("Hourly link volume (log)")
    plt.yscale('log')
    plt.title("Change in road link usage by probe ratio, Friday {} o'clock".format(hour))

    # remove the pandas legend
    plt.gca().legend_.remove()
    # add new legend
    topHandle = Line2D([],[], color='red', ls="-", label=">99.9%")
    midHandleOne = Line2D([],[], color='blue', ls="-", label="99~99.9%")
    lowHandle = Line2D([],[], color='c', ls="-", label="90~99%")
    negHandle = Line2D([],[], color='black', ls="-", label="0~90%")
    plt.legend(handles=[topHandle, midHandleOne,lowHandle, negHandle], title='Percentile, stdev of link volume across probe ratios', bbox_to_anchor=(0.5, -0.3), loc='lower center', ncol=4, columnspacing=1.5, labelspacing=0.8)
    plt.savefig(absolute_path+'/Figs/pcp_{}_HR{}.png'.format(var, hour))


if __name__ == '__main__':
    #plot_peak_hour('AMT', 18)
    #plot_hourly_amt('VHT', 0.005)
    plot_hourly_trend('VHT', 0.0, 'Total Vehicle Hours Travelled', zoom=False) ### Can also plot AMT, average travel time per agent
    #plot_pcp('true_flow', 3, 0) ### max flow hour is 18