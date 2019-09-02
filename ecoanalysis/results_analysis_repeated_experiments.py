### CHECK FACTS: https://www.epa.gov/greenvehicles/greenhouse-gas-emissions-typical-passenger-vehicle
### FUEL ECONOMY: 22 miles per gallon
### 8,887 grams CO2/ gallon
### 404 grams per mile per car
import sys
import numpy as np
import scipy.sparse 
import scipy.io as sio 
import time 
import os
import pandas as pd 
import itertools 
import glob
import gc
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.ticker as ticker 
from matplotlib.lines import Line2D
from matplotlib.patches import Patch 

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})
pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

def scen34_results(outdir):

    subscen_list = []
    for f in glob.glob(absolute_path+'/{}/repeat_experiments/scen34_results*.csv'.format(outdir)):
        subscen = pd.read_csv(f)
        #subscen = subscen.loc[subscen['year']<=10]
        subscen = subscen[['case','budget','iri_impact','eco_route_ratio','year','random_seed','emi_total','emi_local','emi_highway','pci_average','pci_local','pci_highway','vht_total','vht_local','vht_highway','vkmt_total','vkmt_local','vkmt_highway']]
        subscen_list.append(subscen)
    
    scen34_df = pd.concat(subscen_list, ignore_index=True)

    return scen34_df

def plot_scen34_results(data, variable, ylim=[0,100], ylabel='None', scen_no=4, half_title='Eco maintenance + ', base_color_map={0.1: [0, 0.6, 1], 0.5: [0, 0, 1], 1.0: [0.6, 0, 1]}):

    fig, ax = plt.subplots()
    fig.set_size_inches(12, 5)

    legend_list = []

    for eco_route_ratio in [0.1, 0.5, 1.0]:
        base_color = base_color_map[eco_route_ratio]
        main_color = base_color+[1]
        second_color = base_color+[0.2]
        color_dict = {0.01: np.array(main_color), 0.03: np.array(second_color)}
        single_legend_dict = {} ### custom legend

        boxprops = dict(color=second_color, linewidth=1, alpha=0.5)
        flierprops = dict(markerfacecolor=base_color, marker='s', markersize=5, markeredgecolor=second_color)
        data_slice = data.loc[(data['eco_route_ratio']==eco_route_ratio)&(data['budget']==700)&(data['iri_impact']==0.03)]
        slice_pivot = data_slice.pivot(columns='year', index='random_seed', values=variable)

        data_grp = data_slice.groupby('year', as_index=False).agg({variable: percentile(50)}).reset_index()
        ax.plot(data_grp['year'], data_grp[variable], c=second_color, lw=2, linestyle='solid')

        box = ax.boxplot(slice_pivot.values, positions=slice_pivot.columns.values, labels=slice_pivot.columns.values-1, boxprops=boxprops, widths=0.5, patch_artist=True, flierprops=flierprops)
        for patch in box['boxes']:
            patch.set_facecolor(second_color)

        legend_list.append(Line2D([], [], lw=6, linestyle = 'solid', c=second_color, label='Eco-route ratio {}'.format(eco_route_ratio)))

    # ### Shrink current axis's height
    box = ax.get_position()
    ax.set_position([box.x0, box.y0+0.18*box.height, box.width, 0.88*box.height])
    ### legend
    legend = plt.legend(title = 'Budget 700, IRI_impact 3%', handles=legend_list, bbox_to_anchor=(0.5, -0.35), loc='lower center', frameon=False, ncol=3, labelspacing=0.5)
    plt.setp(legend.get_title(), weight='bold')
    plt.gca().add_artist(legend)

    plt.xlabel('Year', fontdict={'size': '16'}, labelpad=10)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.xaxis.set_label_coords(1.08, -0.02)
    plt.ylim(ylim)
    plt.ylabel(ylabel, fontdict={'size': '16'}, labelpad=10)
    if variable != 'pci_average': plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    #plt.show()
    plt.savefig('Figs/{}_scen{}_reapeat.png'.format(variable, scen_no), dpi=300, transparent=True)  


if __name__ == '__main__':

    outdir = 'output_march19'
    data = scen34_results(outdir)
    print(data.shape)

    variable = 'emi_highway' ### 'emi_total', 'vkmt_total', 'vht_total', 'pci_average'
    ylim_dict = {
        'emi_total': [3600, 4000], 'emi_local': [1750, 2050], 'emi_highway': [1400, 1700],
        'vkmt_total': [1.45e7, 1.6e7], 'vkmt_local': [7.3e6, 7.8e6], 'vkmt_highway': [7.0e6, 8.6e6],
        'vht_total': [6e5, 0.9e6], 'vht_local': [4e5, 6.5e5], 'vht_highway':[2e5, 2.4e5],
        'pci_average': [20, 90], 'pci_local': [20, 90]}
    ylabel_dict = {
        'emi_total': 'Annual Average Daily CO\u2082 (t)', 
        'emi_local': 'Annual Averagy Daily CO\u2082 (t) \n on local roads', 
        'emi_highway': 'Annual Averagy Daily CO\u2082 (t) \n on highway', 
        'vkmt_total': 'Annual Average Daily Vehicle \n Kilometers Travelled (AAD-VKMT)', 
        'vkmt_local': 'Annual Average Daily Vehicle Kilometers\n Travelled (AAD-VKMT) on local roads',
        'vkmt_highway': 'Annual Average Daily Vehicle Kilometers\n Travelled (AAD-VKMT) on highway',
        'vht_total': 'Annual Average Daily Vehicle \n Hours Travelled (AAD-VHT)', 
        'vht_local': 'Annual Average Daily Vehicle Hours \n Travelled (AAD-VHT) on local roads', 
        'vht_highway': 'Annual Average Daily Vehicle Hours \n Travelled (AAD-VHT) on highway', 
        'pci_average': 'Network-wide Average Pavement\nCondition Index (PCI)', 
        'pci_local': 'Average Pavement Condition Index (PCI)\n of local roads'}

    plot_scen34_results(data, variable, ylim=ylim_dict[variable], ylabel=ylabel_dict[variable], scen_no=3, half_title='', base_color_map={0.1: [1, 0.8, 0], 0.5: [0, 0.8, 0], 1.0: [0, 0.2, 0]})
    sys.exit(0)


