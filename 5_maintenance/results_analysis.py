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
import gc
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.ticker as ticker 
from matplotlib.lines import Line2D
from matplotlib.patches import Patch 

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
    aad_df['net_vol'] = aad_df['hour_flow'] - aad_df['carryover_flow']
    aad_df['net_vht'] = aad_df['net_vol'] * aad_df['t_avg']/3600
    aad_df['v_avg_mph'] = aad_df['length']/aad_df['t_avg'] * 2.23694 ### time step link speed in mph
    aad_df['base_co2'] = base_co2(aad_df['v_avg_mph']) ### link-level co2 eimission in gram per mile per vehicle
    aad_df['base_emi'] = aad_df['base_co2'] * aad_df['length'] /1609.34 * aad_df['net_vol'] ### speed related CO2 x length x flow. Final results unit is gram.

    aad_df['aad_vol'] += aad_df['net_vol']
    aad_df['aad_vht'] += aad_df['net_vht']
    aad_df['aad_vmt'] += aad_df['net_vol']*aad_df['length']
    aad_df['aad_base_emi'] += aad_df['base_emi']
    aad_df = aad_df[['edge_id_igraph', 'length', 'aad_vol', 'aad_vht', 'aad_vmt', 'aad_base_emi']]
    return aad_df

def eco_incentivize_analysis():

    day = 2
    random_seed = 0
    probe_ratio = 0.01
    results_list = []

    for budget in [400, 1500]:
        for eco_route_ratio in [0.1, 0.5, 1]:
            for iri_impact in [0.01, 0.03]:
                for year in range(10):
                    ### ['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft', 'pci_current', 'eco_wgh']
                    edges_df = pd.read_csv(absolute_path+'/output/edge_df/edges_b{}_e{}_i{}_y{}.mtx'.format(budget, eco_route_ratio, iri_impact, year))
                    aad_df = edges_df[['edge_id_igraph', 'length', 'pci_current']].copy()
                    aad_df['aad_vol'] = 0
                    aad_df['aad_vht'] = 0 ### daily vehicle hours travelled
                    aad_df['aad_vmt'] = 0
                    aad_df['aad_base_emi'] = 0

                    for hour in range(3, 27):
                        hour_volume_df = pd.read_csv(absolute_path+'/output/edges_df_abm/edges_df_b{}_e{}_i{}_y{}_HR{}.csv'.format(budget, eco_route_ratio, iri_impact, year, hour))
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

    results_df = pd.DataFrame(results_list, columns=['budget', 'eco_route_ratio', 'iri_impact', 'year', 'emi_total', 'vkmt_total', 'vht_total', 'pci_average'])
    results_df.to_csv('results.csv', index=False)

def plot_scen12_results(variable):
    results_df = pd.read_csv('scen12_results.csv')

    fig, ax = plt.subplots()
    fig.set_size_inches(7, 6)

    color_dict = {0.01: np.array([0, 0.135112, 0.304751, 1]), 0.03: np.array([0.795737, 0.709344, 0.217772, 1])}
    marker_dict = {'normal': '$N$', 'eco': '$E$'}
    legend_elements_dict = {} ### custom legend

    results_df_grp = results_df.loc[results_df['budget']==400].groupby(['iri_impact', 'case'])
    for (iri_impact, case), grp in results_df_grp:
        ax.plot(grp['year'], grp[variable], c=color_dict[iri_impact], lw=1, linestyle='dashed', marker=marker_dict[case], ms=7)

    results_df_grp = results_df.loc[results_df['budget']==1500].groupby(['iri_impact', 'case'])
    for (iri_impact, case), grp in results_df_grp:
        ax.plot(grp['year'], grp[variable], c=color_dict[iri_impact], lw=1, marker=marker_dict[case], ms=7)
        ### Legend
        legend_elements_dict[(iri_impact, case)]=Line2D([], [], marker=marker_dict[case], ms=7, ls='', mec=None, c=color_dict[iri_impact], label='{} maintenance, IRI impact {}'.format(case, iri_impact))

    legend_elements_dict['budget_400']=Line2D([], [], lw=2, linestyle='dashed', c='black', label='Bugdet: 400 blocks per year')
    legend_elements_dict['budget_1500']=Line2D([], [], lw=2, c='black', label='Bugdet: 1500 blocks per year')
    legend_elements_list = [legend_elements_dict['budget_400'], legend_elements_dict[(0.01, 'normal')], legend_elements_dict[(0.01, 'eco')], legend_elements_dict['budget_1500'], legend_elements_dict[(0.03, 'normal')], legend_elements_dict[(0.03, 'eco')]]

    ### Shrink current axis's height
    box = ax.get_position()
    ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    plt.legend(handles=legend_elements_list, bbox_to_anchor=(0.5, -0.35), loc='lower center', fancybox=True, ncol=2)
    plt.xlabel('Year')
    plt.ylim([3400, 3750])
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    plt.ylabel('Annual Average Daily CO\u2082 (t)')
    #plt.show()
    plt.savefig('Figs/{}_scen12.png'.format(variable))


def plot_scen345_results(variable):
    results_df = pd.read_csv('scen345_results.csv')
    #print(results_df.head())

    fig, ax = plt.subplots()
    fig.set_size_inches(7, 6)

    # color = iter(cm.rainbow(np.linspace(0, 1, 6)))
    # color_dict = {}
    color_dict = {0.01: np.array([0.1, 0.58778525, 0.95105652, 1]), 0.03: np.array([1, 1.2246468e-16, 6.1232340e-17, 1])}
    marker_dict = {0.1: '$10$', 0.5: '$50$', 1.0: '$100$'}
    legend_elements_dict = {} ### custom legend

    results_df_grp = results_df.loc[results_df['budget']==400].groupby(['iri_impact', 'eco_route_ratio'])
    for (iri_impact, eco_route_ratio), grp in results_df_grp:
        ax.plot(grp['year'], grp[variable], c=color_dict[iri_impact], lw=1, linestyle='dashed', marker=marker_dict[eco_route_ratio], ms=10)

    results_df_grp = results_df.loc[results_df['budget']==1500].groupby(['iri_impact', 'eco_route_ratio'])
    for (iri_impact, eco_route_ratio), grp in results_df_grp:
        ax.plot(grp['year'], grp[variable], c=color_dict[iri_impact], lw=1, marker=marker_dict[eco_route_ratio], ms=10)
        ### Legend
        legend_elements_dict[(iri_impact, eco_route_ratio)]=Line2D([], [], marker=marker_dict[eco_route_ratio], ms=10, ls='', mec=None, c=color_dict[iri_impact], label='{}% eco-routing, IRI impact {}'.format(int(eco_route_ratio*100), iri_impact))

    legend_elements_dict['budget_400']=Line2D([], [], lw=2, linestyle='dashed', c='black', label='Bugdet: 400 blocks per year')
    legend_elements_dict['budget_1500']=Line2D([], [], lw=2, c='black', label='Bugdet: 1500 blocks per year')
    legend_elements_list = [legend_elements_dict['budget_400'], legend_elements_dict[(0.01, 0.1)], legend_elements_dict[(0.01, 0.5)], legend_elements_dict[(0.01, 1.0)], legend_elements_dict['budget_1500'], legend_elements_dict[(0.03, 0.1)], legend_elements_dict[(0.03, 0.5)], legend_elements_dict[(0.03, 1.0)]]

    ### Shrink current axis's height
    box = ax.get_position()
    ax.set_position([box.x0, box.y0+box.height*0.2, box.width, box.height*0.9])
    #plt.legend()
    plt.legend(handles=legend_elements_list, bbox_to_anchor=(0.5, -0.35), loc='lower center', fancybox=True, ncol=2)
    plt.xlabel('Year')
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    plt.ylabel('Annual Average Daily CO\u2082 (t)')
    plt.ylim([3400, 3750])
    #plt.show()
    plt.savefig('Figs/{}_scen345.png'.format(variable))

if __name__ == '__main__':
    #eco_incentivize_analysis()
    variable = 'emi_total' ### 'emi_total', 'vkmt_total', 'vht_total', 'pci_average'
    plot_scen12_results(variable)
    #plot_scen345_results(variable)

