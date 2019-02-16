import os
import sys
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
import gc 

absolute_path = os.path.dirname(os.path.abspath(__file__))
pd.set_option('display.max_columns', 10)

def vht(hour, probe_ratio, random_seed):

    edges_df = pd.read_csv(absolute_path+'/../2_ABM/output/speed_sensor/edges_df_carry_1step/edges_df_DY4_HR{}_r{}_p{}.csv'.format(hour, random_seed, probe_ratio))
    vht = sum(edges_df['hour_flow']*edges_df['t_avg'])/3600

    return vht

def main():

    vht_l = []
    for random_seed in [0, 1, 2, 3, 4]:
    #for random_seed in [0]:
        #for probe_ratio in [1, 0.1, 0.01, 0.005, 0.001, 0]:
        for probe_ratio in [0.001]:
            for hour in range(3, 27):
                vht_hour = vht(hour, probe_ratio, random_seed)
                vht_l.append([random_seed, probe_ratio, hour, vht_hour])
                gc.collect()

    vht_df = pd.DataFrame(vht_l, columns=['random_seed', 'probe_ratio', 'HR', 'VHT'])
    print(vht_df[vht_df['HR'].isin([10, 14, 24])].sort_values(by='HR'))
    sys.exit(0)
    vht_df_grp = vht_df.groupby(['probe_ratio', 'HR']).agg({'VHT': [np.mean, np.std]}).reset_index()
    vht_df_grp.columns = ['_'.join(col).strip() for col in vht_df_grp.columns.values]
    vht_df_grp['mean+std'] = vht_df_grp['VHT_mean'] + vht_df_grp['VHT_std']
    vht_df_grp['mean-std'] = vht_df_grp['VHT_mean'] - vht_df_grp['VHT_std']
    print(vht_df_grp)
    sys.exit(0)

def plot_vht():
    
    vht_df_grp = pd.read_csv('vht_df_grp_carry_1step.csv')

    fig, ax = plt.subplots(1)
    fig.set_size_inches(10.5, 5.5)
    color = iter(cm.rainbow(np.linspace(0, 1, 6)))
    for probe_ratio in [1, 0.1, 0.01, 0.005]:
    #for probe_ratio in [0.001]:
        series_df = vht_df_grp[vht_df_grp['probe_ratio_']==probe_ratio]
        c = next(color)
        ax.plot('HR_', 'VHT_mean', data=series_df, label='{}'.format(probe_ratio), c=c)
        ax.fill_between('HR_', 'mean-std', 'mean+std', data=series_df, facecolor=c, alpha=0.5)
    ax.set_title('VHT on a typical Friday and probe ratio')
    ax.legend(title='probe ratio', bbox_to_anchor=(0.15, 0.9))
    ax.set_xlabel('Hour')
    ax.set_ylabel('VHT')
    ax.set_yscale('log')
    plt.savefig('Figs/vht_by_hour_carry_1step_0.5above.png')


if __name__ == '__main__':
    #main()
    plot_vht()