import os
import sys
import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
#from matplotlib.pyplot import cm

pd.set_option('display.max_columns', 10)
absolute_path = os.path.dirname(os.path.abspath(__file__))

folder = 'sf_overpass'
scenario = 'original'


if __name__ == '__main__':

    metrics_df = pd.DataFrame(columns=['sigma', 'probe_ratio', 'probe_veh_counts', 'links_probed_norepe', 'links_probed_repe', 'VHT', 'VKMT', 'max10'])
    for rep_no in [10, 11, 12, 13, 18, 19]:
        one_rep_df = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/HPC_random_test/random_seed_{}.csv'.format(rep_no))
        metrics_df = pd.concat([metrics_df, one_rep_df], ignore_index=True)

    metrics_df_grp = metrics_df.groupby(['sigma', 'probe_ratio']).agg({
        'VHT': [np.mean, np.std, np.max, np.min],
        'VKMT': [np.mean, np.std, np.max, np.min]}).reset_index()
    metrics_df_grp.columns = metrics_df_grp.columns.map('|'.join)
    metrics_df_grp['+stdev'] = metrics_df_grp['VHT|mean'] + metrics_df_grp['VHT|std']
    metrics_df_grp['-stdev'] = metrics_df_grp['VHT|mean'] - metrics_df_grp['VHT|std']
    #metrics_df_grp['probe_ratio_2'] = metrics_df_grp['probe_ratio|']
    #metrics_df_grp.loc[metrics_df_grp['probe_ratio_2']==0, 'probe_ratio_2'] = 0.000000001
    metrics_df_grp['probe_ratio_3'] = metrics_df_grp['probe_ratio|']

    fig, ax = plt.subplots(1)
    color = iter(plt.cm.Spectral(np.linspace(0, 1, 5)))
    for sigma in [10, 5, 2, 1, 0]:
    #for sigma in [1]:
        c = next(color)
        sub_metrics = metrics_df_grp.loc[metrics_df_grp['sigma|']==sigma]
        ax.plot('probe_ratio_3', 'VHT|mean', data=sub_metrics, lw=1, ls='--', marker='.', markersize=5, c=c, label='{}'.format(sigma))
        ax.fill_between('probe_ratio_3', '+stdev', '-stdev', data=sub_metrics, facecolor=c, alpha=0.3)
    plt.xscale('log')
    plt.legend(title='sigma')
    plt.xlabel('probe ratio')
    plt.ylabel('Vehicle Hours Travelled (VHT)')
    plt.show()

