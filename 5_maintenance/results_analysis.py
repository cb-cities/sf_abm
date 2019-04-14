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

def scen34_results(outdir):

    subscen_list = []
    for f in glob.glob(absolute_path+'/{}/results/scen34_results_b*.csv'.format(outdir)):
        subscen = pd.read_csv(f)
        subscen = subscen.loc[subscen['year']<=10]
        subscen = subscen[['case','budget','iri_impact','eco_route_ratio','year','emi_total','emi_local','emi_highway','pci_average','pci_local','pci_highway','vht_total','vht_local','vht_highway','vkmt_total','vkmt_local','vkmt_highway']]
        subscen_list.append(subscen)
    
    scen34_results_df = pd.concat(subscen_list, ignore_index=True)
    #scen34_results_df = pd.concat([pd.read_csv(f) for f in glob.glob(absolute_path+'/{}/results/scen34_results*.csv'.format(outdir))], ignore_index=True)
    #scen34_results_df = scen34_results_df.drop(columns=['Unnamed: 0'])
    print(scen34_results_df.head())

    scen34_results_df.to_csv(absolute_path+'/{}/results/scen34_results.csv'.format(outdir), index=False)


def plot_scen12_results(data, variable, ylim=[0,100], ylabel='None', scen_no=0, title = '', base_color=[0, 0, 1]):

    fig, ax = plt.subplots()
    fig.set_size_inches(9, 5)

    main_color = base_color+[1]
    second_color = base_color+[0.2]
    color_dict = {0.01: np.array(main_color), 0.03: np.array(second_color)}
    lw_dict = {0.01: 1, 0.03: 6}
    linestyle_dict = {200: ':', 700: 'solid'}
    legend_elements_dict = {} ### custom legend

    for budget in [200, 700]:
        for iri_impact in [0.03, 0.01]:
            data_slice = data.loc[(data['budget']==budget)&(data['iri_impact']==iri_impact)]
            #print(data_slice.shape, np.min(data_slice[variable]), np.max(data_slice[variable]))
            ax.plot(data_slice['year'], data_slice[variable], c=color_dict[iri_impact], lw=lw_dict[iri_impact], linestyle=linestyle_dict[budget], marker='.', ms=1)
            legend_elements_dict['{}_{}'.format(budget, iri_impact)] = Line2D([], [], lw=lw_dict[iri_impact], linestyle = linestyle_dict[budget], c=color_dict[iri_impact], label='Budget: {},\nIRI_impact: {}'.format(budget, iri_impact))

    legend_elements_list = [legend_elements_dict[('200_0.03')], legend_elements_dict['700_0.03'], legend_elements_dict['200_0.01'], legend_elements_dict['700_0.01']]

    ### Not considering PCI
    ### 2152.737t CO2 per day on local roads
    if variable == 'emi_local':
        ax.axhline(y=data.iloc[0]['emi_localroads_base'], color='black', linestyle='-.', lw=1)
        legend_elements_list.append(Line2D([], [], lw=1, linestyle='-.', c='black', label='No degradation'))

    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0+box.width*0.03, box.y0, box.width*0.7, box.height])
    legend = plt.legend(title = title, handles=legend_elements_list, bbox_to_anchor=(1.27, 0.0), loc='lower center', frameon=False, ncol=1, labelspacing=1.5)
    #plt.setp(legend.get_title(), fontsize=14)
    plt.setp(legend.get_title(), weight='bold')
    plt.xlabel('Year', fontdict={'size': '16'}, labelpad=10)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.xaxis.set_label_coords(1.08, -0.02)
    #plt.xlim([1,10])
    plt.ylim(ylim)
    plt.ylabel(ylabel, fontdict={'size': '16'}, labelpad=10)
    if variable[0:3] != 'pci': plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    #plt.show()
    plt.savefig('Figs/{}_scen{}.png'.format(variable, scen_no), dpi=300, transparent=True)

def plot_scen34_results(data, variable, ylim=[0,100], ylabel='None', scen_no=4, half_title='Eco maintenance + ', base_color_map={0.1: [0, 0.6, 1], 0.5: [0, 0, 1], 1.0: [0.6, 0, 1]}):

    fig, ax = plt.subplots()
    fig.set_size_inches(15, 5)

    lw_dict = {0.01: 1, 0.03: 6}
    linestyle_dict = {200: ':', 700: 'solid'}
    all_legends_dict = {}

    for eco_route_ratio in [0.1, 0.5, 1.0]:
        base_color = base_color_map[eco_route_ratio]
        main_color = base_color+[1]
        second_color = base_color+[0.2]
        color_dict = {0.01: np.array(main_color), 0.03: np.array(second_color)}
        single_legend_dict = {} ### custom legend
        for budget in [200, 700]:
            for iri_impact in [0.03, 0.01]:
                data_slice = data.loc[(data['eco_route_ratio']==eco_route_ratio)&(data['budget']==budget)&(data['iri_impact']==iri_impact)]
                ax.plot(data_slice['year'], data_slice[variable], c=color_dict[iri_impact], lw=lw_dict[iri_impact], linestyle=linestyle_dict[budget], marker='.', ms=1)
                single_legend_dict['{}_{}'.format(budget, iri_impact)] = Line2D([], [], lw=lw_dict[iri_impact], linestyle = linestyle_dict[budget], c=color_dict[iri_impact], label='Budget: {},\nIRI_impact: {}'.format(budget, iri_impact))
        all_legends_dict[eco_route_ratio] = [single_legend_dict[('200_0.03')], single_legend_dict['700_0.03'], single_legend_dict['200_0.01'], single_legend_dict['700_0.01']]

    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0, box.y0, box.width*0.45, box.height])
    ### legend 1
    legend1 = plt.legend(title = half_title+'\n10% eco-routing', handles=all_legends_dict[0.1], bbox_to_anchor=(1.27, 0.08), loc='lower center', frameon=False, ncol=1, labelspacing=1.5)
    #plt.setp(legend.get_title(), fontsize=14)
    plt.setp(legend1.get_title(), weight='bold')
    ### legend 2
    legend2 = plt.legend(title = half_title+'\n50% eco-routing', handles=all_legends_dict[0.5], bbox_to_anchor=(1.75, 0.08), loc='lower center', frameon=False, ncol=1, labelspacing=1.5)
    #plt.setp(legend.get_title(), fontsize=14)
    plt.setp(legend2.get_title(), weight='bold')
    ### legend 4
    legend3 = plt.legend(title = half_title+'\n100% eco-routing', handles=all_legends_dict[1.0], bbox_to_anchor=(2.25, 0.08), loc='lower center', frameon=False, ncol=1, labelspacing=1.5)
    #plt.setp(legend.get_title(), fontsize=14)
    plt.setp(legend3.get_title(), weight='bold')
    plt.gca().add_artist(legend1)
    plt.gca().add_artist(legend2)
    plt.gca().add_artist(legend3)

    plt.xlabel('Year', fontdict={'size': '16'}, labelpad=10)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.xaxis.set_label_coords(1.08, -0.02)
    plt.ylim(ylim)
    plt.ylabel(ylabel, fontdict={'size': '16'}, labelpad=10)
    if variable != 'pci_average': plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    #plt.show()
    plt.savefig('Figs/{}_scen{}.png'.format(variable, scen_no), dpi=300, transparent=True)  


if __name__ == '__main__':

    outdir = 'output_march19'
    # scen34_results(outdir)
    # sys.exit(0)

    variable = 'emi_highway' ### 'emi_total', 'vkmt_total', 'vht_total', 'pci_average'
    ylim_dict = {
        'emi_total': [3600, 4000], 'emi_local': [1750, 2050], 'emi_highway': [1400, 1700],
        'vkmt_total': [1.45e7, 1.6e7], 'vkmt_local': [7.3e6, 7.8e6], 'vkmt_highway': [7.0e6, 8.6e6],
        'vht_total': [6e5, 0.9e6], 'vht_local': [4e5, 6.5e5], 'vht_highway':[2e5, 2.4e5],
        'pci_average': [20, 90], 'pci_local': [20, 90]}
    ylabel_dict = {
        'emi_total': 'Annual Average Daily CO\u2082 (t)', 
        'emi_local': 'Annual Averagy Daily CO\u2082 (t)\n on local roads', 
        'emi_highway': 'Annual Averagy Daily CO\u2082 (t)\n on highway', 
        'vkmt_total': 'Annual Average Daily Vehicle \n Kilometers Travelled (AAD-VKMT)', 
        'vkmt_local': 'Annual Average Daily Vehicle Kilometers\n Travelled (AAD-VKMT) on local roads',
        'vkmt_highway': 'Annual Average Daily Vehicle Kilometers\n Travelled (AAD-VKMT) on highway',
        'vht_total': 'Annual Average Daily Vehicle \n Hours Travelled (AAD-VHT)', 
        'vht_local': 'Annual Average Daily Vehicle Hours \n Travelled (AAD-VHT) on local roads', 
        'vht_highway': 'Annual Average Daily Vehicle Hours \n Travelled (AAD-VHT) on highway', 
        'pci_average': 'Network-wide Average Pavement\nCondition Index (PCI)', 
        'pci_local': 'Average Pavement Condition Index (PCI)\n of local roads'}

    # results_df = pd.read_csv('{}/results/scen12_results.csv'.format(outdir))
    # data = results_df[results_df['case']=='normal']
    # plot_scen12_results(data, variable, ylim=ylim_dict[variable], ylabel=ylabel_dict[variable], scen_no=1, title = 'PCI-based maintenance', base_color=[0, 0, 0])
    # data = results_df[results_df['case']=='eco']
    # plot_scen12_results(data, variable, ylim=ylim_dict[variable], ylabel=ylabel_dict[variable], scen_no=2, title = 'Eco-maintenance', base_color=[1, 0, 0])
    # sys.exit(0)

    # results_df = pd.read_csv('{}/results/scen34_results.csv'.format(outdir))
    # data = results_df[results_df['case']=='er']
    # plot_scen34_results(data, variable, ylim=ylim_dict[variable], ylabel=ylabel_dict[variable], scen_no=3, half_title='', base_color_map={0.1: [1, 0.8, 0], 0.5: [0, 0.8, 0], 1.0: [0, 0.2, 0]})
    # data = results_df[results_df['case']=='ee']
    # plot_scen34_results(data, variable, ylim=ylim_dict[variable], ylabel=ylabel_dict[variable], scen_no=4, half_title='Eco-maintenance + ', base_color_map={0.1: [0, 0.6, 1], 0.5: [0, 0, 1], 1.0: [0.6, 0, 1]})
    # sys.exit(0)

    ### Degradation model sensitivity analysis
    #data = pd.read_csv('{}/results/scen12_results_model_sensitivity_offset.csv'.format(outdir))
    #data = pd.read_csv('{}/results/scen12_results_model_sensitivity_improve_pct.csv'.format(outdir))
    data = pd.read_csv('{}/results/scen12_results_model_sensitivity_slope_mlt.csv'.format(outdir))
    fig, ax = plt.subplots()
    fig.set_size_inches(12, 6)
    variable = 'emi_local'
    color = iter(cm.viridis(np.linspace(0, 1, 3)))
    linestyle_dict = {'normal': '-', 'eco': ':'}
    legend_list = []
    
    ### Sensitivity parameters

    case_dict = {'normal': 'PCI-based maintenance', 'eco': 'Eco-maintenance'}
    start_condition_dict={'True':74, 'False': 79}

    offset = True
    improv_pct = 1
    #slope_mlt = 1
    #for offset in [True, False]:
    #for improv_pct in [1.0, 0.75, 0.5]:
    for slope_mlt in [1, 3, 5]:

        c = next(color)
        for case in ['normal', 'eco']:
            data_slice = data.loc[(data['case']==case) & (data['improv_pct']==improv_pct) & (data['slope_mlt']==slope_mlt) & (data['offset']==offset)]
            ax.plot(data_slice['year'], data_slice[variable], c=c, linestyle=linestyle_dict[case], marker='.', ms=1)
            single_legend = Line2D([], [], lw=1, linestyle = linestyle_dict[case], c=c, 
                #label='start condition: {}, {}'.format(start_condition_dict[str(offset)], case_dict[case])
                #label='improve: {}%, {}'.format(improv_pct*100, case_dict[case])
                label='degradation rate x {}, {}'.format(slope_mlt, case_dict[case])
                )
            legend_list.append(single_legend)
    
    ### Shrink current axis's height
    box = ax.get_position()
    ax.set_position([box.x0, box.y0+box.height*0.22, box.width, box.height*0.84])
    ### legend 1
    legend_list = [legend_list[0], legend_list[2], legend_list[4], legend_list[1], legend_list[3], legend_list[5]]
    legend = plt.legend(handles=legend_list, bbox_to_anchor=(1.05, -0.14), frameon=False, ncol=2, labelspacing=0.8)
    plt.gca().add_artist(legend)

    plt.xlabel('Year', fontdict={'size': '16'}, labelpad=10)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    plt.xlim([-0.5, 20])
    plt.ylabel(ylabel_dict[variable], fontdict={'size': '16'}, labelpad=10)
    if variable != 'pci_average': plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    plt.savefig('Figs/degradation_sensitivity_slope_mlt.png', dpi=300, transparent=True)  


