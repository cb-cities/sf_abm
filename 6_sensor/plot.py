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
        slice_grp = slice_df.groupby('probe_ratio_str').agg({var: [np.mean, np.std]}).reset_index()
        slice_grp.columns = ['_'.join(col).strip() for col in slice_grp.columns.values]

        ax.plot('probe_ratio_str_', '{}_mean'.format(var), data=slice_grp, label='{}'.format(cov), c=c)
        ax.fill_between(slice_grp['probe_ratio_str_'], 
            slice_grp['{}_mean'.format(var)] - slice_grp['{}_std'.format(var)],
            slice_grp['{}_mean'.format(var)] + slice_grp['{}_std'.format(var)],
            facecolor=c, alpha=0.5)

    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0, box.y0, box.width*0.9, box.height])

    ax.set_title("Average agent travel time (Friday {} o'clock)".format(hour), y=1.05)
    ax.legend(title='Probe info\nvariability\n(coef. of\nvariation)', bbox_to_anchor=(1, 0.8))
    ax.set_xlabel('Probe ratio')
    #plt.xscale('log')
    ax.set_ylabel('Average agent travel time (min)')
    #ax.set_ylim([30, 42])
    #plt.yscale('log')
    plt.savefig(absolute_path+'/Figs/{}oc_{}.png'.format(hour, var))
    #plt.show()


def plot_hourly_amt(var, probe_ratio):

    fig, ax = plt.subplots(1)
    fig.set_size_inches(10.5, 5.5)
    color = iter(cm.viridis(np.linspace(0, 1, 4)))

    ### COV 0 case
    summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor/summary_df/summary_p{}.csv'.format(probe_ratio))
    summary_df['VHT'] = summary_df['VHT']/3600
    summary_df['VKMT'] = summary_df['VKMT']/1000
    summary_df['AMT'] = summary_df['VHT']/summary_df['hour_demand']*60 ### Agent minutes travelled
    summary_grp = summary_df.groupby('hour').agg({var: [np.mean, np.std]}).reset_index()
    summary_grp.columns = ['_'.join(col).strip() for col in summary_grp.columns.values]

    #summary_grp = summary_grp.loc[(summary_grp['hour_']>10) & (summary_grp['hour_']<24)].copy()
    c = next(color)
    ax.plot('hour_', '{}_mean'.format(var), data=summary_grp, label='0', c=c)

    for cov in [0.5, 1.0, 2.0]:

        c = next(color)
        summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor_cov/summary_df/summary_p{}_cov{}.csv'.format(probe_ratio, cov))
        summary_df['VHT'] = summary_df['VHT']/3600
        summary_df['VKMT'] = summary_df['VKMT']/1000
        summary_df['AMT'] = summary_df['VHT']/summary_df['hour_demand']*60 ### Agent minutes travelled
        summary_grp = summary_df.groupby('hour').agg({var: [np.mean, np.std]}).reset_index()
        summary_grp.columns = ['_'.join(col).strip() for col in summary_grp.columns.values]

        #summary_grp = summary_grp.loc[(summary_grp['hour_']>10) & (summary_grp['hour_']<24)].copy()
        ax.plot('hour_', '{}_mean'.format(var), data=summary_grp, label='{}'.format(cov), c=c)
        ax.fill_between(summary_grp['hour_'], 
            summary_grp['{}_mean'.format(var)] - summary_grp['{}_std'.format(var)],
            summary_grp['{}_mean'.format(var)] + summary_grp['{}_std'.format(var)],
            facecolor=c, alpha=0.5)
        #plt.errorbar('hour_', '{}_mean'.format(var), '{}_std'.format(var), data=summary_grp, fmt='.-', lw=0.5, color=c, elinewidth=2, ecolor=c, capsize=10, capthick=1)
    
    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0, box.y0, box.width*0.9, box.height])

    ax.set_title('Hourly average agent travel time (Friday), Probe ratio {}'.format(probe_ratio), y=1.05)
    ax.legend(title='Probe coef.\nof variation', bbox_to_anchor=(1, 0.7))
    ax.set_xlabel('Hour')
    ax.set_ylabel('Average agent travel time (min)')
    ax.set_ylim([15, 42])
    plt.savefig(absolute_path+'/Figs/hourly_{}_p{}_test.png'.format(var, probe_ratio))
    #plt.show()

def plot_hourly_trend(var, cov, ylabel, zoom=False):

    fig, ax = plt.subplots(1)
    fig.set_size_inches(10.5, 5.5)
    color = iter(cm.rainbow(np.linspace(0, 1, 6)))
    if zoom == False: 
        probe_ratio_list = [1.0, 0.1, 0.01, 0.005, 0.001, 0.0]
    else: 
        probe_ratio_list = [1.0, 0.1, 0.01, 0.005]
    for probe_ratio in probe_ratio_list:

        c = next(color)
        # summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor_cov/summary_df/summary_p{}_cov{}.csv'.format(probe_ratio, cov))
        # summary_df['VHT'] = summary_df['VHT']/3600
        # summary_df['VKMT'] = summary_df['VKMT']/1000
        # summary_df['AMT'] = summary_df['VHT']/summary_df['hour_demand']*60 ### Agent minutes travelled
        # summary_grp = summary_df.groupby('hour').agg({var: [np.mean, np.std]}).reset_index()
        # summary_grp.columns = ['_'.join(col).strip() for col in summary_grp.columns.values]

        # #summary_grp = summary_grp.loc[(summary_grp['hour_']>10) & (summary_grp['hour_']<24)].copy()
        # ax.plot('hour_', '{}_mean'.format(var), data=summary_grp, label='{}'.format(probe_ratio), c=c)
        # ax.fill_between(summary_grp['hour_'], 
        #     summary_grp['{}_mean'.format(var)] - summary_grp['{}_std'.format(var)],
        #     summary_grp['{}_mean'.format(var)] + summary_grp['{}_std'.format(var)],
        #     facecolor=c, alpha=0.5)
        # #plt.errorbar('hour_', '{}_mean'.format(var), '{}_std'.format(var), data=summary_grp, fmt='.-', lw=0.5, color=c, elinewidth=2, ecolor=c, capsize=10, capthick=1)

        ### COV 0 case
        summary_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor/summary_df/summary_p{}.csv'.format(probe_ratio, cov))
        summary_df['VHT'] = summary_df['VHT']/3600
        summary_df['VKMT'] = summary_df['VKMT']/1000
        summary_df['AMT'] = summary_df['VHT']/summary_df['hour_demand']*60 ### Agent minutes travelled
        summary_grp = summary_df.groupby('hour').agg({var: [np.mean, np.std]}).reset_index()
        summary_grp.columns = ['_'.join(col).strip() for col in summary_grp.columns.values]

        #summary_grp = summary_grp.loc[(summary_grp['hour_']>10) & (summary_grp['hour_']<24)].copy()
        ax.plot('hour_', '{}_mean'.format(var), data=summary_grp, label='{}%'.format(probe_ratio*100), c=c, linestyle='-')
        ax.fill_between(summary_grp['hour_'], 
            summary_grp['{}_mean'.format(var)] - summary_grp['{}_std'.format(var)],
            summary_grp['{}_mean'.format(var)] + summary_grp['{}_std'.format(var)],
            facecolor=c, alpha=0.5)
    
    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0-box.width*0.03, box.y0, box.width*0.87, box.height])

    plt.yscale('log')
    ax.set_title('Hourly {} (Friday), COV {}'.format(ylabel, cov), y=1.05)
    l = ax.legend(title='Probe\npenetration\nrate', bbox_to_anchor=(1, 0.85))
    plt.setp(l.get_title(), multialignment='center')
    ax.set_xlabel('Hour')
    ax.set_ylabel('{}'.format(ylabel))
    plt.savefig(absolute_path+'/Figs/hourly_{}_cov{}_zoom{}.png'.format(var, cov, zoom))
    #plt.show()

def plot_pcp(var, hour, random_seed, cov):

    pcp_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges_elevation.csv'.format(folder, scenario))
    pcp_df = pcp_df[['edge_id_igraph']]

    probe_ratio_list = [1.0, 0.1, 0.01, 0.005, 0.001, 0.0]
    for probe_ratio in probe_ratio_list:
        edges_df = pd.read_csv(absolute_path+'/../2_ABM/output/sensor_cov/edges_df/edges_df_DY4_HR{}_r{}_p{}_cov{}.csv'.format(hour, random_seed, probe_ratio, cov))
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
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='low', var_cols], 'highlight', color='c', alpha=0.1)
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='mid', var_cols], 'highlight', color='b', alpha=0.4)
    parallel_coordinates(pcp_df.loc[pcp_df['highlight']=='top', var_cols], 'highlight', color='r', alpha=0.8)

    ### Shrink current axis's height
    box = ax.get_position()
    ax.set_position([box.x0-box.width*0.1, box.y0+box.height*0.14, box.width*1.2, box.height*0.9])

    plt.xlabel("Probe ratio")
    ax.set_xticklabels(probe_ratio_list)
    plt.ylabel("Hourly link volume (log)")
    plt.yscale('log')
    plt.title("Change in road link usage by probe ratio (0-90%), Friday {} o'clock, COV {}".format(hour, cov))

    # remove the pandas legend
    plt.gca().legend_.remove()
    # add new legend
    topHandle = Line2D([],[], color='red', ls="-", label=">99.9%")
    midHandleOne = Line2D([],[], color='blue', ls="-", label="99~99.9%")
    lowHandle = Line2D([],[], color='c', ls="-", label="90~99%")
    negHandle = Line2D([],[], color='black', ls="-", label="0~90%")
    plt.legend(handles=[topHandle, midHandleOne,lowHandle, negHandle], title='Percentile, hourly link volume in the perfect information case', bbox_to_anchor=(0.5, -0.3), loc='lower center', ncol=4, columnspacing=1.5, labelspacing=0.8)
    plt.savefig(absolute_path+'/Figs/pcp_{}_HR{}_cov{}_full.png'.format(var, hour, cov))


if __name__ == '__main__':
    
    #plot_peak_hour('AMT', 18)
    #plot_hourly_amt('AMT', 1.0)
    #plot_hourly_trend('AMT', 0.0, 'Average Agent Travel Time (min)', zoom=False)
    plot_pcp('true_flow', 18, 0, 2.0) ### max flow hour is 18
