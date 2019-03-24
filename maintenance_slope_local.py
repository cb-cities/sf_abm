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
import matplotlib.pyplot as plt
import matplotlib.cm as cm 

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))
folder = 'sf_overpass'
scenario = 'original'
outdir = 'output_march19'

highway_type = ['motorway', 'motorway_link', 'trunk', 'trunk_link']

def base_co2(mph_array):
    ### CO2 - speed function constants (Barth and Boriboonsomsin, "Real-World Carbon Dioxide Impacts of Traffic Congestion")
    b0 = 7.362867270508520
    b1 = -0.149814315838651
    b2 = 0.004214810510200
    b3 = -0.000049253951464
    b4 = 0.000000217166574
    return np.exp(b0 + b1*mph_array + b2*mph_array**2 + b3*mph_array**3 + b4*mph_array**4)

def aad_vol_vmt_baseemi(aad_df, hour_volume_df):

    aad_df = pd.merge(aad_df, hour_volume_df, on='edge_id_igraph', how='left')
    aad_df['vht'] = aad_df['true_flow'] * aad_df['t_avg']/3600
    aad_df['v_avg_mph'] = aad_df['length']/aad_df['t_avg'] * 2.23694 ### time step link speed in mph
    aad_df['base_co2'] = base_co2(aad_df['v_avg_mph']) ### link-level co2 eimission in gram per mile per vehicle
    ### correction for slope
    aad_df['base_co2'] = aad_df['base_co2'] * aad_df['slope_factor']
    aad_df['base_emi'] = aad_df['base_co2'] * aad_df['length'] /1609.34 * aad_df['true_flow'] ### speed related CO2 x length x flow. Final results unit is gram.

    aad_df['aad_vol'] += aad_df['true_flow']
    aad_df['aad_vht'] += aad_df['vht']
    aad_df['aad_vmt'] += aad_df['true_flow']*aad_df['length']
    aad_df['aad_base_emi'] += aad_df['base_emi']
    aad_df = aad_df[['edge_id_igraph', 'length', 'type', 'slope_factor', 'aad_vol', 'aad_vht', 'aad_vmt', 'aad_base_emi']]
    return aad_df

def preprocessing():
    ### Read the edge attributes. 
    edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges_elevation.csv'.format(folder, scenario))
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'slope', 'capacity', 'fft', 'type', 'geometry']]
    edges_df['slope_factor'] = np.where(edges_df['slope']<-0.05, 0.2, np.where(edges_df['slope']>0.15, 3.4, 1+0.16*(edges_df['slope']*100)))

    ### PCI RELATED EMISSION
    ### Read pavement age on Jan 01, 2017, and degradation model coefficients
    sf_pavement = pd.read_csv(absolute_path+'/input/r_to_python.csv')
    sf_pavement['initial_age'] *= 365
    ### Key to merge cnn with igraphid
    sf_cnn_igraphid = pd.read_csv(absolute_path+'/input/3_cnn_igraphid.csv')
    sf_cnn_igraphid = sf_cnn_igraphid[sf_cnn_igraphid['edge_id_igraph']!='None'].reset_index()
    sf_cnn_igraphid['edge_id_igraph'] = sf_cnn_igraphid['edge_id_igraph'].astype('int64')
    ### Get degradation related parameters, incuding the coefficients and initial age
    edges_df = pd.merge(edges_df, sf_cnn_igraphid, on='edge_id_igraph', how='left')
    ### Fill cnn na with edge_id_igraph
    edges_df['cnn_expand'] = np.where(pd.isna(edges_df['cnn']), edges_df['edge_id_igraph'], edges_df['cnn'])
    edges_df = pd.merge(edges_df, sf_pavement[['cnn', 'alpha', 'beta', 'xi', 'uv', 'initial_age']], left_on='cnn_expand', right_on='cnn', how='left')
    edges_df['cnn_expand'] = edges_df['cnn_expand'].astype(int).astype(str)

    ### Keep relevant colum_ns
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'slope', 'slope_factor', 'capacity', 'fft', 'cnn_expand', 'alpha', 'beta', 'xi', 'uv', 'initial_age', 'type', 'geometry']]
    ### Remove duplicates
    edges_df = edges_df.drop_duplicates(subset='edge_id_igraph', keep='first').reset_index()

    ### Some igraphids have empty coefficients and age, set to average
    #edges_df['initial_age'] = edges_df['initial_age'].fillna(edges_df['initial_age'].mean())
    edges_df['initial_age'] = edges_df['initial_age'].fillna(0)
    edges_df['alpha'] = edges_df['alpha'].fillna(edges_df['alpha'].mean())
    edges_df['beta'] = edges_df['beta'].fillna(edges_df['beta'].mean())
    edges_df['xi'] = edges_df['xi'].fillna(0)
    edges_df['uv'] = edges_df['uv'].fillna(0)

    ### Not considering highways
    edges_df['initial_age'] = np.where(edges_df['type'].isin(highway_type), 0, edges_df['initial_age'])
    edges_df['alpha'] = np.where(edges_df['type'].isin(highway_type), 85, edges_df['alpha']) ### highway PCI is assumed to be 85 throughout based on Caltrans 2015 state of the pavement report
    edges_df['beta'] = np.where(edges_df['type'].isin(highway_type), 0, edges_df['beta'])
    edges_df['xi'] = np.where(edges_df['type'].isin(highway_type), 0, edges_df['xi'])
    edges_df['uv'] = np.where(edges_df['type'].isin(highway_type), 0, edges_df['uv'])

    ### Set initial age as the current age
    edges_df['age_current'] = edges_df['initial_age'] ### age in days

    return edges_df

def eco_incentivize(budget, eco_route_ratio, iri_impact, case):

    ### Read in the edge attribute. 
    edges_df = preprocessing()

    ### ABM parameters
    day = 2 ### Wednesday
    random_seed = 0
    probe_ratio = 1

    step_results_list = []
    for year in range(11):

        ### Current PCI
        edges_df['pci_current'] = edges_df['alpha']+edges_df['xi'] + (edges_df['beta']+edges_df['uv'])*edges_df['age_current']/365
        edges_df['pci_current'] = np.where(edges_df['pci_current']>100, 100, edges_df['pci_current'])
        edges_df['pci_current'] = np.where(edges_df['pci_current']<0, 0, edges_df['pci_current'])
        
        ### Annual average daily
        aad_df = edges_df[['edge_id_igraph', 'length', 'type', 'slope_factor']].copy()
        aad_df['aad_vol'] = 0
        aad_df['aad_vht'] = 0 ### daily vehicle hours travelled
        aad_df['aad_vmt'] = 0
        aad_df['aad_base_emi'] = 0

        if case in ['normal', 'eco']:
            for hour in range(3, 27):
                hour_volume_df = pd.read_csv(absolute_path+'/{}/edges_df_singleyear/edges_df_DY{}_HR{}_r{}_p{}.csv'.format(outdir, day, hour, random_seed, probe_ratio))
                aad_df = aad_vol_vmt_baseemi(aad_df, hour_volume_df)

        elif case in ['ee', 'er']:
            ### INITIAL GRAPH WEIGHTS: SPEED RELATED EMISSION
            ### Calculate the free flow speed in MPH, as required by the emission-speed model
            edges_df['ffs_mph'] = edges_df['length']/edges_df['fft']*2.23964
            ### FFS_MPH --> speed related emission
            edges_df['base_co2_ffs'] = base_co2(edges_df['ffs_mph']) ### link-level co2 eimission in gram per mile per vehicle
            ### Adjust emission by considering the impact of pavement degradation
            edges_df['pci_co2_ffs'] = edges_df['base_co2_ffs']*(1+0.0714*iri_impact*(100-edges_df['pci_current'])) ### emission in gram per mile per vehicle
            edges_df['eco_wgh'] = edges_df['pci_co2_ffs']/1609.34*edges_df['length']

            ### Output network graph for ABM simulation
            ### Shape of the network as a sparse matrix
            g_time = sio.mmread(absolute_path+'/../0_network/data/{}/{}/network_sparse.mtx'.format(folder, scenario))
            g_time_shape = g_time.shape
            wgh = edges_df['eco_wgh']
            row = edges_df['start_sp']-1
            col = edges_df['end_sp']-1
            g_eco = scipy.sparse.coo_matrix((wgh, (row, col)), shape=g_time_shape)
            sio.mmwrite(absolute_path+'/{}/network/network_sparse_b{}_e{}_i{}_c{}_y{}.mtx'.format(outdir, budget, eco_route_ratio, iri_impact, case, year), g_eco)
            # g_coo = sio.mmread(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder))

            ### Output edge attributes for ABM simulation
            edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'slope_factor', 'length', 'capacity', 'fft', 'pci_current', 'eco_wgh']].to_csv(absolute_path+'/{}/edge_df/edges_b{}_e{}_i{}_c{}_y{}.csv'.format(outdir, budget, eco_route_ratio, iri_impact, case, year), index=False)

            ### Run ABM
            sf_abm.sta(outdir, year, day=day, random_seed=random_seed, probe_ratio=probe_ratio, budget=budget, eco_route_ratio=eco_route_ratio, iri_impact=iri_impact, case=case)

            for hour in range(3, 27):
                hour_volume_df = pd.read_csv(absolute_path+'/{}/edges_df_abm/edges_df_b{}_e{}_i{}_c{}_y{}_HR{}.csv'.format(outdir, budget, eco_route_ratio, iri_impact, case, year, hour))
                aad_df = aad_vol_vmt_baseemi(aad_df, hour_volume_df)

        else:
            print('no such case')

        ### Get pci adjusted emission
        aad_df = pd.merge(aad_df, edges_df[['edge_id_igraph', 'cnn_expand', 'pci_current', 'alpha', 'xi']], on='edge_id_igraph', how='left')

        ### Adjust emission by considering the impact of pavement degradation
        aad_df['aad_pci_emi'] = aad_df['aad_base_emi']*(1+0.0714*iri_impact*(100-aad_df['pci_current'])) ### daily emission (aad) in gram
        aad_df['aad_newroads_emi'] = aad_df['aad_base_emi']*(1+0.0714*iri_impact*(100 -aad_df['alpha'] - aad_df['xi']))
        ### Maintenance scheduling
        aad_df['aad_emi_potential'] = aad_df['aad_pci_emi'] - aad_df['aad_newroads_emi']

        def pci_improvement(df, year, case, budget, eco_route_ratio, iri_impact): ### repair worst roads
            repair_df = df.groupby(['cnn_expand']).agg({'pci_current': np.mean}).reset_index().nsmallest(budget, 'pci_current')
            repair_list = repair_df['cnn_expand'].tolist()
            extract_df = df.loc[df['cnn_expand'].isin(repair_list)]
            extract_df[['edge_id_igraph', 'aad_emi_potential']].to_csv(absolute_path + '/{}/repair_df/repair_df_y{}_c{}_b{}_e{}_i{}.csv'.format(outdir, year, case, budget, eco_route_ratio, iri_impact))
            return repair_list

        def eco_maintenance(df, year, case, budget, eco_route_ratio, iri_impact):
            repair_df = df.groupby(['cnn_expand']).agg({'aad_emi_potential': np.sum}).reset_index().nlargest(budget, 'aad_emi_potential')
            repair_list = repair_df['cnn_expand'].tolist()
            extract_df = df.loc[df['cnn_expand'].isin(repair_list)]
            extract_df[['edge_id_igraph', 'aad_emi_potential']].to_csv(absolute_path+'/{}/repair_df/repair_df_y{}_c{}_b{}_e{}_i{}.csv'.format(outdir, year, case, budget, eco_route_ratio, iri_impact))
            return repair_list

        if case in ['normal', 'er']: 
            repair_list = pci_improvement(aad_df, year, case, budget, eco_route_ratio, iri_impact)
            ### Repair
            edges_df['age_current'] = edges_df['age_current']+365
            edges_df.loc[edges_df['cnn_expand'].isin(repair_list), 'age_current'] = 0
        elif case in ['eco', 'ee']:
            repair_list = eco_maintenance(aad_df, year, case, budget, eco_route_ratio, iri_impact)
            ### Repair
            edges_df['age_current'] = edges_df['age_current']+365
            edges_df.loc[edges_df['cnn_expand'].isin(repair_list), 'age_current'] = 0
            edges_df.loc[edges_df['cnn_expand'].isin(repair_list), 'alpha'] = 100
            edges_df.loc[edges_df['cnn_expand'].isin(repair_list), 'xi'] = 0
        else:
            print('no matching maintenance strategy')

        ### Results
        ### emi
        emi_total = np.sum(aad_df['aad_pci_emi'])/1e6 ### co2 emission in t
        emi_local = np.sum(aad_df[~aad_df['type'].isin(highway_type)]['aad_pci_emi'])/1e6
        emi_highway = np.sum(aad_df[edges_df['type'].isin(highway_type)]['aad_pci_emi'])/1e6
        emi_newlocalroads = np.sum(aad_df[~aad_df['type'].isin(highway_type)]['aad_newroads_emi'])/1e6
        emi_localroads_base = np.sum(aad_df[~aad_df['type'].isin(highway_type)]['aad_base_emi'])/1e6

        ### vht
        vht_total = np.sum(aad_df['aad_vht']) ### vehicle hours travelled
        vht_local = np.sum(aad_df[~edges_df['type'].isin(highway_type)]['aad_vht'])
        vht_highway = np.sum(aad_df[edges_df['type'].isin(highway_type)]['aad_vht'])
        ### vkmt
        vkmt_total = np.sum(aad_df['aad_vmt'])/1000 ### vehicle kilometers travelled
        vkmt_local = np.sum(aad_df[~edges_df['type'].isin(highway_type)]['aad_vmt'])/1000
        vkmt_highway = np.sum(aad_df[edges_df['type'].isin(highway_type)]['aad_vmt'])/1000
        ### pci
        pci_average = np.mean(aad_df['pci_current'])
        pci_local = np.mean(aad_df[~edges_df['type'].isin(highway_type)]['pci_current'])
        pci_highway = np.mean(aad_df[edges_df['type'].isin(highway_type)]['pci_current'])

        step_results_list.append([case, budget, iri_impact, eco_route_ratio, year, emi_total, emi_local, emi_highway, emi_newlocalroads, pci_average, pci_local, pci_highway, vht_total, vht_local, vht_highway, vkmt_total, vkmt_local, vkmt_highway])
    #print(step_results_list[0:10:9])
    return step_results_list

def exploratory_budget():

    plt.rcParams.update({'font.size': 12, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

    preprocessing()
    results_list = []
    for budget in [400, 1000, 1500, 2000, 2500]:
        step_results_list = eco(budget, 0, 'normal')
        results_list += step_results_list
    
    results_df = pd.DataFrame(results_list, columns=['case', 'budget', 'iri_impact', 'year', 'emi_total', 'emi_local', 'emi_highway', 'pci_average', 'pci_local', 'pci_highway', 'vht_total', 'vht_local', 'vht_highway', 'vkmt_total', 'vkmt_local', 'vkmt_highway'])
    
    results_df_grp = results_df.groupby('budget')
    color=iter(cm.magma(np.linspace(0,1,5)))
    fig, ax = plt.subplots()
    for budget, grp in results_df_grp:
        c=next(color)
        ax.plot('year', 'pci_average', data=grp, c=c, label=budget)
    plt.axhline(y=76.7, linestyle=':')
    plt.legend(title='Budget')
    plt.xlabel('Year')
    plt.ylabel('Average PCI on the local road network')
    plt.ylim(50, 100)
    plt.show()

if __name__ == '__main__':

    # preprocessing()
    # sys.exit(0)

    # exploratory_budget()
    # sys.exit(0)

    # eco_incentivize(1500, 0, 0.03, 'normal')
    # sys.exit(0)

    ### Scne 12
    #eco_route_ratio = 0
    #scen12_results_list = []
    #for case in ['normal', 'eco']:
    #    for budget in [400, 1500]:
    #        for iri_impact in [0.01, 0.03]:
    #            print('budget {}, eco_route_ratio {}, iri_impact {}, case {}'.format(budget, eco_route_ratio, iri_impact, case))
    #            step_results_list = eco_incentivize(budget, eco_route_ratio, iri_impact, case)
    #            scen12_results_list += step_results_list

    #results_df = pd.DataFrame(scen12_results_list, columns=['case', 'budget', 'iri_impact', 'eco_route_ratio', 'year', 'emi_total', 'emi_local', 'emi_highway', 'emi_newlocalroads',  'pci_average', 'pci_local', 'pci_highway', 'vht_total', 'vht_local', 'vht_highway', 'vkmt_total', 'vkmt_local', 'vkmt_highway'])
    #results_df.to_csv('{}/results/scen12_results.csv'.format(outdir), index=False)
    #sys.exit(0)

    ### Scen 34
    budget = int(os.environ['BUDGET']) ### 400 or 1500
    eco_route_ratio = float(os.environ['ECO_ROUTE_RATIO']) ### 0.1, 0.5 or 1
    iri_impact = float(os.environ['IRI_IMPACT']) ### 0.01 or 0.03
    case = os.environ['CASE'] ### 'er' for 'routing_only', 'ee' for 'both'
    print('budget {}, eco_route_ratio {}, iri_impact {}, case {}'.format(budget, eco_route_ratio, iri_impact, case))

    step_results_list = eco_incentivize(budget, eco_route_ratio, iri_impact, case)
    results_df = pd.DataFrame(step_results_list, columns=['case', 'budget', 'iri_impact', 'eco_route_ratio', 'year', 'emi_total', 'emi_local', 'emi_highway', 'emi_newlocalroads',  'pci_average', 'pci_local', 'pci_highway', 'vht_total', 'vht_local', 'vht_highway', 'vkmt_total', 'vkmt_local', 'vkmt_highway'])
    results_df.to_csv(absolute_path+'/{}/results/scen34_results_b{}_e{}_i{}_c{}.csv'.format(outdir, budget, eco_route_ratio, iri_impact, case))

