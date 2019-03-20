### CHECK FACTS: https://www.epa.gov/greenvehicles/greenhouse-gas-emissions-typical-passenger-vehicle
### FUEL ECONOMY: 22 miles per gallon
### 8,887 grams CO2/ gallon
### 404 grams per mile per car
import json
import sys
import numpy as np
import scipy.sparse 
import scipy.io as sio 
import time 
import os
import pandas as pd 
import itertools 
import gc
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.ticker as ticker 
from matplotlib.lines import Line2D
from matplotlib.patches import Patch 

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})
pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))

def base_co2(mph_array):
    ### CO2 - speed function constants (Barth and Boriboonsomsin, "Real-World Carbon Dioxide Impacts of Traffic Congestion")
    b0 = 7.362867270508520
    b1 = -0.149814315838651
    b2 = 0.004214810510200
    b3 = -0.000049253951464
    b4 = 0.000000217166574
    return np.exp(b0 + b1*mph_array + b2*mph_array**2 + b3*mph_array**3 + b4*mph_array**4)

def aad_vol_vmt_baseemi(aad_df, hour_volume_df):
    ### volume_df[['edge_id_igraph', 'length', 'aad_vol', 'aad_vmt', 'aad_base_emi']]
    ### hour_volume_df[['edge_id_igraph', 'hour_flow', 'carryover_flow', 't_avg']]

    aad_df = pd.merge(aad_df, hour_volume_df, on='edge_id_igraph', how='left')
    aad_df['vht'] = aad_df['true_flow'] * aad_df['t_avg']/3600
    aad_df['v_avg_mph'] = aad_df['length']/aad_df['t_avg'] * 2.23694 ### time step link speed in mph
    aad_df['base_co2'] = base_co2(aad_df['v_avg_mph']) ### link-level co2 eimission in gram per mile per vehicle
    aad_df['base_co2'] = aad_df['base_co2'] * aad_df['slope_factor']
    aad_df['base_emi'] = aad_df['base_co2'] * aad_df['length'] /1609.34 * aad_df['true_flow'] ### speed related CO2 x length x flow. Final results unit is gram.

    aad_df['aad_vol'] += aad_df['true_flow']
    aad_df['aad_vht'] += aad_df['vht']
    aad_df['aad_vmt'] += aad_df['true_flow']*aad_df['length']
    aad_df['aad_base_emi'] += aad_df['base_emi']
    aad_df = aad_df[['edge_id_igraph', 'length', 'slope_factor', 'aad_vol', 'aad_vht', 'aad_vmt', 'aad_base_emi']]
    return aad_df

def eco_incentivize_analysis():

    day = 2
    random_seed = 0
    probe_ratio = 1
    results_list = []

    budget_list = [400, 1500]
    eco_route_ratio_list = [0.1, 0.5, 1.0]
    iri_impact_list = [0.01, 0.03]
    case_list = ['ee', 'er']

    for (budget, eco_route_ratio, iri_impact, case) in list(itertools.product(budget_list, eco_route_ratio_list, iri_impact_list, case_list)):
        for year in range(10):
            ### ['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft', 'pci_current', 'eco_wgh']
            edges_df = pd.read_csv(absolute_path+'/output_march/edge_df/edges_b{}_e{}_i{}_c{}_y{}.csv'.format(budget, eco_route_ratio, iri_impact, case, year))
            aad_df = edges_df[['edge_id_igraph', 'length', 'slope_factor', 'pci_current']].copy()
            aad_df['aad_vol'] = 0
            aad_df['aad_vht'] = 0 ### daily vehicle hours travelled
            aad_df['aad_vmt'] = 0
            aad_df['aad_base_emi'] = 0

            for hour in range(3, 27):
                hour_volume_df = pd.read_csv(absolute_path+'/output_march/edges_df_abm/edges_df_b{}_e{}_i{}_c{}_y{}_HR{}.csv'.format(budget, eco_route_ratio, iri_impact, case, year, hour))
                ### ['edge_id_igraph', 'length', 'aad_vol', 'aad_vht', 'aad_vmt', 'aad_base_emi']
                aad_df = aad_vol_vmt_baseemi(aad_df, hour_volume_df)
                gc.collect()

            aad_df = pd.merge(aad_df, edges_df[['edge_id_igraph', 'pci_current']], on='edge_id_igraph', how='left')
            ### Adjust emission by considering the impact of pavement degradation
            aad_df['aad_pci_emi'] = aad_df['aad_base_emi']*(1+0.0714*iri_impact*(100-aad_df['pci_current'])) ### daily emission (aad) in gram

            vkmt_total = np.sum(aad_df['aad_vmt'])/1000 ### vehicle kilometers travelled
            vht_total = np.sum(aad_df['aad_vht']) ### vehicle hours travelled
            emi_total = np.sum(aad_df['aad_pci_emi'])/1e6 ### co2 emission in t
            pci_average = np.mean(aad_df['pci_current'])
            results_list.append([budget, eco_route_ratio, iri_impact, year, emi_total, vkmt_total, vht_total, pci_average])

    results_df = pd.DataFrame(results_list, columns=['case', 'budget', 'eco_route_ratio', 'iri_impact', 'year', 'emi_total', 'vkmt_total', 'vht_total', 'pci_average'])
    results_df.to_csv('output_march/scen34_results.csv', index=False)


def plot_scen12_results(data, variable, ylim=[0,100], ylabel='None', scen_no=0, title = '', base_color=[0, 0, 1]):

    fig, ax = plt.subplots()
    fig.set_size_inches(9, 5)

    main_color = base_color+[1]
    second_color = base_color+[0.2]
    color_dict = {0.01: np.array(main_color), 0.03: np.array(second_color)}
    lw_dict = {0.01: 1, 0.03: 6}
    linestyle_dict = {400: ':', 1500: 'solid'}
    legend_elements_dict = {} ### custom legend

    for budget in [400, 1500]:
        for iri_impact in [0.03, 0.01]:
            data_slice = data.loc[(data['budget']==budget)&(data['iri_impact']==iri_impact)]
            ax.plot(data_slice['year']+1, data_slice[variable], c=color_dict[iri_impact], lw=lw_dict[iri_impact], linestyle=linestyle_dict[budget], marker='.', ms=1)
            legend_elements_dict['{}_{}'.format(budget, iri_impact)] = Line2D([], [], lw=lw_dict[iri_impact], linestyle = linestyle_dict[budget], c=color_dict[iri_impact], label='Budget: {},\nIRI_impact: {}'.format(budget, iri_impact))

    legend_elements_list = [legend_elements_dict[('400_0.03')], legend_elements_dict['1500_0.03'], legend_elements_dict['400_0.01'], legend_elements_dict['1500_0.01']]

    ### Shrink current axis's height
    box = ax.get_position()
    #ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    ax.set_position([box.x0+box.width*0.03, box.y0, box.width*0.7, box.height])
    legend = plt.legend(title = title, handles=legend_elements_list, bbox_to_anchor=(1.27, 0.08), loc='lower center', frameon=False, ncol=1, labelspacing=1.5)
    #plt.setp(legend.get_title(), fontsize=14)
    plt.setp(legend.get_title(), weight='bold')
    plt.xlabel('Year', fontdict={'size': '16'}, labelpad=10)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.xaxis.set_label_coords(1.08, -0.02)
    plt.ylim(ylim)
    plt.ylabel(ylabel, fontdict={'size': '16'}, labelpad=10)
    if variable != 'pci_average': plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    #plt.show()
    plt.savefig('Figs/{}_scen{}.png'.format(variable, scen_no), dpi=300, transparent=True)

def plot_scen34_results(data, variable, ylim=[0,100], ylabel='None', scen_no=4, half_title='Eco maintenance + ', base_color_map={0.1: [0, 0.6, 1], 0.5: [0, 0, 1], 1.0: [0.6, 0, 1]}):

    fig, ax = plt.subplots()
    fig.set_size_inches(15, 5)

    lw_dict = {0.01: 1, 0.03: 6}
    linestyle_dict = {400: ':', 1500: 'solid'}
    all_legends_dict = {}

    for eco_route_ratio in [0.1, 0.5, 1.0]:
        base_color = base_color_map[eco_route_ratio]
        main_color = base_color+[1]
        second_color = base_color+[0.2]
        color_dict = {0.01: np.array(main_color), 0.03: np.array(second_color)}
        single_legend_dict = {} ### custom legend
        for budget in [400, 1500]:
            for iri_impact in [0.03, 0.01]:
                data_slice = data.loc[(data['eco_route_ratio']==eco_route_ratio)&(data['budget']==budget)&(data['iri_impact']==iri_impact)]
                ax.plot(data_slice['year']+1, data_slice[variable], c=color_dict[iri_impact], lw=lw_dict[iri_impact], linestyle=linestyle_dict[budget], marker='.', ms=1)
                single_legend_dict['{}_{}'.format(budget, iri_impact)] = Line2D([], [], lw=lw_dict[iri_impact], linestyle = linestyle_dict[budget], c=color_dict[iri_impact], label='Budget: {},\nIRI_impact: {}'.format(budget, iri_impact))
        all_legends_dict[eco_route_ratio] = [single_legend_dict[('400_0.03')], single_legend_dict['1500_0.03'], single_legend_dict['400_0.01'], single_legend_dict['1500_0.01']]

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
    #eco_incentivize_analysis()

    variable = 'vht_total' ### 'emi_total', 'vkmt_total', 'vht_total', 'pci_average'
    ylim_dict = {'emi_total': [3600, 4000], 'vkmt_total': [1.45e7, 1.6e7], 'vht_total': [6e5, 0.9e6], 'pci_average': [20, 90]}
    ylabel_dict = {'emi_total': 'Annual Average Daily CO\u2082 (t)', 'vkmt_total': 'Annual Average Daily Vehicle \n Kilometers Travelled (AAD-VKMT)', 'vht_total': 'Annual Average Daily Vehicle \n Hours Travelled (AAD-VHT)', 'pci_average': 'Network-wide Average Pavement\nCondition Index (PCI)'}

    results_df = pd.read_csv('output_march/scen12_results.csv')
    data = results_df[results_df['case']=='normal']
    plot_scen12_results(data, variable, ylim=ylim_dict[variable], ylabel=ylabel_dict[variable], scen_no=1, title = 'Normal maintenance', base_color=[0, 0, 0])
    data = results_df[results_df['case']=='eco']
    plot_scen12_results(data, variable, ylim=ylim_dict[variable], ylabel=ylabel_dict[variable], scen_no=2, title = 'Eco maintenance', base_color=[1, 0, 0])
    # sys.exit(0)

    results_df = pd.read_csv('output_march/scen34_results.csv')
    data = results_df[results_df['case']=='er']
    plot_scen34_results(data, variable, ylim=ylim_dict[variable], ylabel=ylabel_dict[variable], scen_no=3, half_title='', base_color_map={0.1: [1, 0.8, 0], 0.5: [0, 0.8, 0], 1.0: [0, 0.2, 0]})
    data = results_df[results_df['case']=='ee']
    plot_scen34_results(data, variable, ylim=ylim_dict[variable], ylabel=ylabel_dict[variable], scen_no=4, half_title='Eco maintenance + ', base_color_map={0.1: [0, 0.6, 1], 0.5: [0, 0, 1], 1.0: [0.6, 0, 1]})

