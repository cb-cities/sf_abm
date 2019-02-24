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
#import matplotlib.pyplot as plt

pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))
folder = 'sf_overpass'
scenario = 'original'

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

    day = 4
    random_seed = 0
    probe_ratio = 0.01
    results_list = []

    for budget in [400, 1500]:
        for eco_route_ratio in [0.1, 0.5, 1.0]:
            for iri_impact in [0.01, 0.03]:
                for year in range(10):
                    print(budget, eco_route_ratio, iri_impact, year)
                    ### ['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft', 'pci_current', 'eco_wgh']
                    edges_df = pd.read_csv(absolute_path+'/output/edge_df/edges_b{}_e{}_i{}_y{}.csv'.format(budget, eco_route_ratio, iri_impact, year))
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

if __name__ == '__main__':
    eco_incentivize_analysis()

