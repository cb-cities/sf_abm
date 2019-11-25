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

def emi_plot():

    emi_df_list = []
    for case in ['nr', 'em']:
        for random_seed in range(1,11):
            one_emi_df = pd.read_csv(absolute_path + '/output_LCA2020/summary/hpc/emi_summary_r{}_c{}.csv'.format(random_seed, case))
            emi_df_list.append(one_emi_df)
    emi_df = pd.concat(emi_df_list, sort=False)

    fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    ax.scatter(emi_df[emi_df['case']=='nr']['year'], emi_df[emi_df['case']=='nr']['emi_local'], s=50, color='red', alpha=0.5, marker='_', label='random from lower quantile')
    ax.scatter(emi_df[emi_df['case']=='em']['year'], emi_df[emi_df['case']=='em']['emi_local'], s=50, color='blue', alpha=0.5, marker='_', label='eco-maintenance')
    ax.set(xticks=np.arange(0, 5, 1))
    plt.xlabel('Year')
    plt.ylabel('Annual CO2 emission in tonne, local')
    plt.legend()

    # plt.show()
    plt.savefig(absolute_path+'/output_LCA2020/summary_plot_repeat_nr_em_local.png')

if __name__ == '__main__':
    emi_plot()