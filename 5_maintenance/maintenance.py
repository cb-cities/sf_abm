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
import logging
import datetime
import warnings
import pandas as pd 
import sf_abm
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

def preprocessing():
    ### Read the edge attributes. 
    edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges.csv'.format(folder, scenario))
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft']]

    ### PCI RELATED EMISSION
    ### Read pavement age on Jan 01, 2017, and degradation model coefficients
    sf_pavement = pd.read_csv(absolute_path+'/input/initial_pavement_data.csv')
    ### Key to merge cnn with igraphid
    sf_cnn_igraphid = pd.read_csv(absolute_path+'/input/3_cnn_igraphid.csv')
    sf_cnn_igraphid = sf_cnn_igraphid[sf_cnn_igraphid['edge_id_igraph']!='None'].reset_index()
    sf_cnn_igraphid['edge_id_igraph'] = sf_cnn_igraphid['edge_id_igraph'].astype('int64')
    ### Get degradation related parameters, incuding the coefficients and initial age
    edges_df = pd.merge(edges_df, sf_cnn_igraphid, on='edge_id_igraph', how='left')
    edges_df = pd.merge(edges_df, sf_pavement[['CNN', 'alpha', 'beta', 'xi', 'uv', 'initial_age']], left_on='cnn', right_on='CNN', how='left')
    ### Keep relevant columns
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft', 'CNN', 'alpha', 'beta', 'xi', 'uv', 'initial_age']]
    ### Remove duplicates
    edges_df = edges_df.drop_duplicates(subset='edge_id_igraph', keep='first').reset_index()
    ### Some igraphids have empty coefficients and age, set to average
    edges_df['initial_age'] = edges_df['initial_age'].fillna(edges_df['initial_age'].mean())
    edges_df['alpha'] = edges_df['alpha'].fillna(edges_df['alpha'].mean())
    edges_df['beta'] = edges_df['beta'].fillna(edges_df['beta'].mean())
    edges_df['xi'] = edges_df['xi'].fillna(0)
    edges_df['uv'] = edges_df['uv'].fillna(0)
    ### Set initial age as the current age
    edges_df['age_current'] = edges_df['initial_age'] ### age in days

    return edges_df

def eco(budget, case):
    ### Read in the edge attribute. 
    edges_df = preprocessing()

    ### SPEED RELATED EMISSION
    day = 4
    random_seed = 0
    probe_ratio = 0.01
    aad_df = edges_df[['edge_id_igraph', 'length']].copy()
    aad_df['aad_vol'] = 0 ### daily volume
    aad_df['aad_vht'] = 0 ### daily vehicle hours travelled
    aad_df['aad_vmt'] = 0 ### vehicle meters traveled
    aad_df['aad_base_emi'] = 0 ### daily emission (in grams) if not considering pavement degradation
    for hour in range(3, 27):
        hour_volume_df = pd.read_csv(absolute_path+'/output/edges_df_singleyear/edges_df_DY{}_HR{}_r{}_p{}.csv'.format(day, hour, random_seed, probe_ratio))
        aad_df = aad_vol_vmt_baseemi(aad_df, hour_volume_df) ### aad_df[['edge_id_igraph', 'length', 'aad_vol', 'aad_vmt', 'aad_base_emi']]

    edges_df = pd.merge(edges_df, aad_df, on='edge_id_igraph', how='left')
    vmlt_total = np.sum(edges_df['aad_vmt'])/1609.34

    ### Fix road sbased on PCI RELATED EMISSION
    for year in range(10):

        ### Calculate the current pci based on the coefficients and current age
        edges_df['pci_current'] = edges_df['alpha']+edges_df['xi'] + (edges_df['beta']+edges_df['uv'])*edges_df['age_current']/365
        ### Adjust emission by considering the impact of pavement degradation
        edges_df['aad_pci_emi'] = edges_df['aad_base_emi']*(1+0.07*0.03*(100-edges_df['pci_current'])) ### daily emission (aad) in gram
        ### Maintenance scheduling
        if case=='normal': ### repair worst roads
            edges_repair = edges_df.nsmallest(budget, 'pci_current')['edge_id_igraph'].tolist()
        if case=='eco': ### repair roads that have the biggest potential on reducing emission
            edges_df['aad_emi_potential'] = edges_df['aad_base_emi']*0.07*0.03 * (edges_df['alpha'] + edges_df['xi'] - edges_df['pci_current'])
            edges_repair = edges_df.nlargest(budget, 'aad_emi_potential')['edge_id_igraph'].tolist()
        ### Repairing
        edges_df['age_current'] = edges_df['age_current']+365
        edges_df.loc[edges_df['edge_id_igraph'].isin(edges_repair), 'age_current'] = 0
        print('average emission pmlpv {}, total {}, vmlt {}'.format(np.sum(edges_df['aad_pci_emi'])/vmlt_total, np.sum(edges_df['aad_pci_emi']), vmlt_total))
        print('total VHT {}'.format(np.sum(edges_df['aad_vht'])))

def eco_incentivize(budget):

    ### Read in the edge attribute. 
    edges_df = preprocessing()

    ### INITIAL GRAPH WEIGHTS: SPEED RELATED EMISSION
    ### Calculate the free flow speed in MPH, as required by the emission-speed model
    edges_df['ffs_mph'] = edges_df['length']/edges_df['fft']*2.23964
    ### FFS_MPH --> speed related emission
    edges_df['base_co2_ffs'] = base_co2(edges_df['ffs_mph']) ### link-level co2 eimission in gram per mile per vehicle

    ### Shape of the network as a sparse matrix
    g_0 = sio.mmread(absolute_path+'/../0_network/data/{}/{}/network_sparse.mtx'.format(folder, scenario))
    g_0_shape = g_0.shape

    for year in range(10):

        ### Calculate the current pci based on the coefficients and current age
        edges_df['pci_current'] = edges_df['alpha']+edges_df['xi'] + (edges_df['beta']+edges_df['uv'])*edges_df['age_current']/365
        ### Adjust emission by considering the impact of pavement degradation
        edges_df['pci_co2_ffs'] = edges_df['base_co2_ffs']*(1+0.07*0.03*(100-edges_df['pci_current'])) ### emission in gram per mile per vehicle
        edges_df['eco_wgh'] = edges_df['pci_co2_ffs']/1609.34*edges_df['length']

        ### Output network graph for ABM simulation
        wgh = edges_df['eco_wgh']
        row = edges_df['start_sp']-1
        col = edges_df['end_sp']-1
        g_coo = scipy.sparse.coo_matrix((wgh, (row, col)), shape=g_0_shape)
        sio.mmwrite(absolute_path+'/output/network/network_sparse_b{}_y{}.mtx'.format(budget, year), g_coo)
        # g_coo = sio.mmread(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder))

        ### Output edge attributes for ABM simulation
        edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft', 'pci_current', 'eco_wgh']].to_csv(absolute_path+'/output/edge_df/edges_b{}_y{}.csv'.format(budget, year), index=False)

        day = 4
        random_seed = 0
        probe_ratio = 0.01
        ### Run ABM
        sf_abm.sta(year, day=day, random_seed=random_seed, probe_ratio=probe_ratio, budget=budget)
        aad_df = edges_df[['edge_id_igraph', 'length', 'pci_current']].copy()
        aad_df['aad_vol'] = 0
        aad_df['aad_vht'] = 0 ### daily vehicle hours travelled
        aad_df['aad_vmt'] = 0
        aad_df['aad_base_emi'] = 0
        for hour in range(3, 27):
            hour_volume_df = pd.read_csv(absolute_path+'/output/edges_df_abm/edges_df_b{}_y{}_DY{}_HR{}_r{}_p{}.csv'.format(budget, year, day, hour, random_seed, probe_ratio))
            aad_df = aad_vol_vmt_baseemi(aad_df, hour_volume_df)

        aad_df = pd.merge(aad_df, edges_df[['edge_id_igraph', 'pci_current', 'alpha', 'xi']], on='edge_id_igraph', how='left')
        vmlt_total = np.sum(aad_df['aad_vmt'])/1609.34
        ### Adjust emission by considering the impact of pavement degradation
        aad_df['aad_pci_emi'] = aad_df['aad_base_emi']*(1+0.07*0.03*(100-aad_df['pci_current'])) ### daily emission (aad) in gram

        ### Maintenance scheduling
        aad_df['aad_emi_potential'] = aad_df['aad_base_emi']*0.07*0.03 * (aad_df['alpha'] + aad_df['xi'] - aad_df['pci_current'])
        edges_repair = aad_df.nlargest(budget, 'aad_emi_potential')['edge_id_igraph'].tolist()
        ### Repair
        edges_df['age_current'] = edges_df['age_current']+365
        edges_df.loc[edges_df['edge_id_igraph'].isin(edges_repair), 'age_current'] = 0

        print('average emission pmlpv {}, total {}, vmlt {}'.format(np.sum(aad_df['aad_pci_emi'])/vmlt_total, np.sum(aad_df['aad_pci_emi']), vmlt_total))
        print('total VHT {}'.format(np.sum(edges_df['aad_vht'])))

if __name__ == '__main__':

    budget = int(os.environ['BUDGET'])
    #eco(budget, 'eco')
    eco_incentivize(budget)
