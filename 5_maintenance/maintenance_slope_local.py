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
    aad_df = aad_df[['edge_id_igraph', 'length', 'slope_factor', 'aad_vol', 'aad_vht', 'aad_vmt', 'aad_base_emi']]
    return aad_df

def preprocessing():
    ### Read the edge attributes. 
    edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges_elevation.csv'.format(folder, scenario))
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'slope', 'capacity', 'fft', 'type', 'geometry']]
    edges_df['slope_factor'] = np.where(edges_df['slope']<-0.05, 0.2, np.where(edges_df['slope']>0.15, 3.4, 1+0.16*(edges_df['slope']*100)))
    #edges_df['slope_factor'] = 1

    ### PCI RELATED EMISSION
    ### Read pavement age on Jan 01, 2017, and degradation model coefficients
    sf_pavement = pd.read_csv(absolute_path+'/input/r_to_python.csv')
    ### Key to merge cnn with igraphid
    sf_cnn_igraphid = pd.read_csv(absolute_path+'/input/3_cnn_igraphid.csv')
    sf_cnn_igraphid = sf_cnn_igraphid[sf_cnn_igraphid['edge_id_igraph']!='None'].reset_index()
    sf_cnn_igraphid['edge_id_igraph'] = sf_cnn_igraphid['edge_id_igraph'].astype('int64')
    ### Get degradation related parameters, incuding the coefficients and initial age
    edges_df = pd.merge(edges_df, sf_cnn_igraphid, on='edge_id_igraph', how='left')
    ### Fill cnn na with edge_id_igraph
    edges_df['cnn_expand'] = np.where(pd.isna(edges_df['cnn']), edges_df['edge_id_igraph'], edges_df['cnn'])
    edges_df = pd.merge(edges_df, sf_pavement[['cnn', 'alpha', 'beta', 'xi', 'uv', 'initial_age']], left_on='cnn_expand', right_on='cnn', how='left')
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
    highway_type = ['motorway', 'motorway_link', 'trunk', 'trunk_link']
    edges_df['initial_age'] = np.where(edges_df['type'].isin(highway_type), 0, edges_df['initial_age'])
    edges_df['alpha'] = np.where(edges_df['type'].isin(highway_type), 100, edges_df['alpha'])
    edges_df['beta'] = np.where(edges_df['type'].isin(highway_type), 0, edges_df['beta'])
    edges_df['xi'] = np.where(edges_df['type'].isin(highway_type), 0, edges_df['xi'])
    edges_df['uv'] = np.where(edges_df['type'].isin(highway_type), 0, edges_df['uv'])
    ### Set initial age as the current age
    edges_df['age_current'] = edges_df['initial_age'] ### age in days
    print(len(np.unique(edges_df['cnn_expand'])))
    edges_df.to_csv('output_march/preprocessing.csv', index=False)

    return edges_df

def eco(budget, iri_impact, case):
    ### Read in the edge attribute. 
    edges_df = preprocessing()

    ### SPEED RELATED EMISSION
    day = 2 ### Wednesday
    random_seed = 0
    probe_ratio = 1
    aad_df = edges_df[['edge_id_igraph', 'length', 'slope_factor']].copy()
    aad_df['aad_vol'] = 0 ### daily volume
    aad_df['aad_vht'] = 0 ### daily vehicle hours travelled
    aad_df['aad_vmt'] = 0 ### vehicle meters traveled
    aad_df['aad_base_emi'] = 0 ### daily emission (in grams) if not considering pavement degradation
    for hour in range(3, 5):
        hour_volume_df = pd.read_csv(absolute_path+'/output_march/edges_df_singleyear/edges_df_DY{}_HR{}_r{}_p{}.csv'.format(day, hour, random_seed, probe_ratio))
        aad_df = aad_vol_vmt_baseemi(aad_df, hour_volume_df) ### aad_df[['edge_id_igraph', 'length', 'aad_vol', 'aad_vmt', 'aad_base_emi']]

    edges_df = pd.merge(edges_df, aad_df, on=['edge_id_igraph', 'length', 'slope_factor'], how='left')

    step_results_list = []
    ### Fix road sbased on PCI RELATED EMISSION
    for year in range(10):

        def pci_emi(df, iri_impact):
            ### Calculate the current pci based on the coefficients and current age
            df['pci_current'] = df['alpha']+df['xi'] + (df['beta']+df['uv'])*df['age_current']/365
            df['pci_current'] = np.where(df['pci_current']>100, 100, df['pci_current'])
            df['pci_current'] = np.where(df['pci_current']<0, 0, df['pci_current'])
            ### Adjust emission by considering the impact of pavement degradation
            df['aad_pci_emi'] = df['aad_base_emi']*(1+0.0714*iri_impact*(100-df['pci_current'])) ### daily emission (aad) in gram
            return df.copy()
        edges_df = pci_emi(edges_df, iri_impact)

        ### ploting emission reduction potential
        def filtering(edges_df, iri_impact):
            edges_df['aad_emi_potential'] = edges_df['aad_base_emi']*0.0714*iri_impact * (edges_df['alpha'] + edges_df['xi'] - edges_df['pci_current'])
            #edges_df.to_csv('output/before_filtering.csv', index=False)
            fig, ax = plt.subplots()
            #fig.set_size_inches(8, 6)
            ax.hist(edges_df['aad_emi_potential']/1e3, bins=50)
            plt.xlabel('Potential of CO\u2082 reduction by link (kg)')
            plt.ylabel('Number of road links')
            plt.yscale('log')
            #plt.savefig('aad_emi_potential_hist.png', dpi=300)
            plt.show()
            #sys.exit(0)
        #filtering(edges_df, iri_impact)
        
        ### Maintenance scheduling
        edges_df['aad_emi_potential'] = edges_df['aad_base_emi']*0.0714*iri_impact * (edges_df['alpha'] + edges_df['xi'] - edges_df['pci_current'])
        def pci_improvement(df, year, case, budget, iri_impact): ### repair worst roads
            repair_df = df.groupby(['cnn_expand']).agg({'pci_current': np.mean}).reset_index().nsmallest(budget, 'pci_current')
            repair_list = repair_df['cnn_expand'].tolist()
            extract_df = df.loc[df['cnn_expand'].isin(repair_list)]
            #extract_df[['edge_id_igraph', 'aad_emi_potential']].to_csv('output_march/repair_df/repair_df_y{}_c{}_b{}_i{}.csv'.format(year, case, budget, iri_impact))
            return repair_list

        def eco_maintenance(df, year, case, budget, iri_impact):
            repair_df = df.groupby(['cnn_expand']).agg({'aad_emi_potential': np.sum}).reset_index().nlargest(budget, 'aad_emi_potential')
            repair_list = repair_df['cnn_expand'].tolist()
            extract_df = df.loc[df['cnn_expand'].isin(repair_list)]
            #print(np.unique(extract_df['type']))
            #extract_df[['edge_id_igraph', 'aad_emi_potential']].to_csv('output_march/repair_df/repair_df_y{}_c{}_b{}_i{}.csv'.format(year, case, budget, iri_impact))
            return repair_list

        ### A free year
        # if (case=='normal') and (year==0): 
        #     repair_list = eco_maintenance(edges_df, iri_impact, year, case)
        # elif (case=='normal') and (year>0): 
        #     repair_list = pci_improvement(edges_df, year, case)
        # elif case=='eco':
        #     repair_list = eco_maintenance(edges_df, iri_impact, year, case)
        # else:
        #     print('no matching maintenance strategy')
        ### No free year
        if case=='normal': 
            repair_list = pci_improvement(edges_df, year, case, budget, iri_impact)
        elif case=='eco':
            repair_list = eco_maintenance(edges_df, year, case, budget, iri_impact)
        else:
            print('no matching maintenance strategy')

        ### Repairing
        edges_df['age_current'] = edges_df['age_current']+365
        edges_df.loc[edges_df['cnn_expand'].isin(repair_list), 'age_current'] = 0

        vkmt_total = np.sum(edges_df['aad_vmt'])/1000 ### vehicle kilometers travelled
        vht_total = np.sum(edges_df['aad_vht']) ### vehicle hours travelled
        emi_total = np.sum(edges_df['aad_pci_emi'])/1e6 ### co2 emission in t
        pci_average = np.mean(edges_df['pci_current'])
        step_results_list.append([case, budget, iri_impact, year, emi_total, vkmt_total, vht_total, pci_average])
    print(step_results_list[0])
    print(step_results_list[9])

    return step_results_list

def eco_incentivize(budget, eco_route_ratio, iri_impact, case):

    ### Read in the edge attribute. 
    edges_df = preprocessing()

    ### INITIAL GRAPH WEIGHTS: SPEED RELATED EMISSION
    ### Calculate the free flow speed in MPH, as required by the emission-speed model
    edges_df['ffs_mph'] = edges_df['length']/edges_df['fft']*2.23964
    ### FFS_MPH --> speed related emission
    edges_df['base_co2_ffs'] = base_co2(edges_df['ffs_mph']) ### link-level co2 eimission in gram per mile per vehicle

    ### Shape of the network as a sparse matrix
    g_time = sio.mmread(absolute_path+'/../0_network/data/{}/{}/network_sparse.mtx'.format(folder, scenario))
    g_time_shape = g_time.shape

    for year in range(2):

        ### Calculate the current pci based on the coefficients and current age
        edges_df['pci_current'] = edges_df['alpha']+edges_df['xi'] + (edges_df['beta']+edges_df['uv'])*edges_df['age_current']/365
        ### Adjust emission by considering the impact of pavement degradation
        edges_df['pci_co2_ffs'] = edges_df['base_co2_ffs']*(1+0.0714*iri_impact*(100-edges_df['pci_current'])) ### emission in gram per mile per vehicle
        edges_df['eco_wgh'] = edges_df['pci_co2_ffs']/1609.34*edges_df['length']

        ### Output network graph for ABM simulation
        wgh = edges_df['eco_wgh']
        row = edges_df['start_sp']-1
        col = edges_df['end_sp']-1
        g_eco = scipy.sparse.coo_matrix((wgh, (row, col)), shape=g_time_shape)
        sio.mmwrite(absolute_path+'/output_march/network/network_sparse_b{}_e{}_i{}_c{}_y{}.mtx'.format(budget, eco_route_ratio, iri_impact, case, year), g_eco)
        # g_coo = sio.mmread(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder))

        ### Output edge attributes for ABM simulation
        edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'slope_factor', 'length', 'capacity', 'fft', 'pci_current', 'eco_wgh']].to_csv(absolute_path+'/output_march/edge_df/edges_b{}_e{}_i{}_c{}_y{}.csv'.format(budget, eco_route_ratio, iri_impact, case, year), index=False)

        day = 2
        random_seed = 0
        probe_ratio = 1
        ### Run ABM
        sf_abm.sta(year, day=day, random_seed=random_seed, probe_ratio=probe_ratio, budget=budget, eco_route_ratio=eco_route_ratio, iri_impact=iri_impact, case=case)
        aad_df = edges_df[['edge_id_igraph', 'length', 'slope_factor', 'pci_current']].copy()
        aad_df['aad_vol'] = 0
        aad_df['aad_vht'] = 0 ### daily vehicle hours travelled
        aad_df['aad_vmt'] = 0
        aad_df['aad_base_emi'] = 0
        for hour in range(3, 5):
            hour_volume_df = pd.read_csv(absolute_path+'/output_march/edges_df_abm/edges_df_b{}_e{}_i{}_c{}_y{}_HR{}.csv'.format(budget, eco_route_ratio, iri_impact, case, year, hour))
            aad_df = aad_vol_vmt_baseemi(aad_df, hour_volume_df)

        aad_df = pd.merge(aad_df, edges_df[['edge_id_igraph', 'cnn_expand', 'pci_current', 'alpha', 'xi']], on='edge_id_igraph', how='left')
        vkmt_total = np.sum(aad_df['aad_vmt'])/1000 # vehicle kilometers travelled
        vmlt_total = np.sum(aad_df['aad_vmt'])/1609.34 # vehicle miles travelled

        ### Adjust emission by considering the impact of pavement degradation
        aad_df['aad_pci_emi'] = aad_df['aad_base_emi']*(1+0.0714*iri_impact*(100-aad_df['pci_current'])) ### daily emission (aad) in gram

        ### Maintenance scheduling
        aad_df['aad_emi_potential'] = aad_df['aad_base_emi']*0.0714*iri_impact * (aad_df['alpha'] + aad_df['xi'] - aad_df['pci_current'])

        def pci_improvement(df, year, case, budget, eco_route_ratio, iri_impact): ### repair worst roads
            repair_df = df.groupby(['cnn_expand']).agg({'pci_current': np.mean}).reset_index().nsmallest(budget, 'pci_current')
            repair_list = repair_df['cnn_expand'].tolist()
            extract_df = df.loc[df['cnn_expand'].isin(repair_list)]
            #extract_df[['edge_id_igraph', 'aad_emi_potential']].to_csv('repair_df/repair_df_y{}_c{}_b{}_e{}_i{}.csv'.format(year, case, budget, eco_route_ratio, iri_impact))
            return repair_list

        def eco_maintenance(df, year, case, budget, eco_route_ratio, iri_impact):
            repair_df = df.groupby(['cnn_expand']).agg({'aad_emi_potential': np.sum}).reset_index().nlargest(budget, 'aad_emi_potential')
            repair_list = repair_df['cnn_expand'].tolist()
            extract_df = df.loc[df['cnn_expand'].isin(repair_list)]
            #extract_df[['edge_id_igraph', 'aad_emi_potential']].to_csv('repair_df/repair_df_y{}_c{}_b{}_e{}_i{}.csv'.format(year, case, budget, eco_route_ratio, iri_impact))
            return repair_list

        if case=='er': 
            repair_list = pci_improvement(aad_df, year, case, budget, eco_route_ratio, iri_impact)
        elif case=='ee':
            repair_list = eco_maintenance(aad_df, year, case, budget, eco_route_ratio, iri_impact)
        else:
            print('no matching maintenance strategy')

        ### Repair
        edges_df['age_current'] = edges_df['age_current']+365
        edges_df.loc[edges_df['cnn_expand'].isin(repair_list), 'age_current'] = 0

        print('Year {}'.format(year))
        print('emi pmlpv {}, total CO2 {} t, vkmt {}, vht {}'.format(np.sum(aad_df['aad_pci_emi'])/vmlt_total, np.sum(aad_df['aad_pci_emi'])/1e6, vkmt_total, np.sum(aad_df['aad_vht'])))
        print('average PCI {}'.format(np.mean(edges_df['pci_current'])))

def exploratory_budget():

    plt.rcParams.update({'font.size': 12, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

    results_list = []
    for budget in [400, 800, 1200, 1500, 1800]:
        step_results_list = eco(budget, 0, 'normal')
        results_list += step_results_list
    results_df = pd.DataFrame(results_list, columns=['case', 'budget', 'iri_impact', 'year', 'emi_total', 'vkmt_total', 'vht_total', 'pci_average'])
    print(results_df.head())
    
    results_df_grp = results_df.groupby('budget')
    color=iter(cm.magma(np.linspace(0,1,5)))
    fig, ax = plt.subplots()
    for budget, grp in results_df_grp:
        c=next(color)
        ax.plot('year', 'pci_average', data=grp, c=c, label=budget)
    plt.axhline(y=79.13, linestyle=':')
    plt.legend(title='Budget')
    plt.xlabel('Year')
    plt.ylabel('Network-wide average PCI')
    plt.ylim(20, 100)
    plt.show()

if __name__ == '__main__':

    # preprocessing()
    # sys.exit(0)

    # exploratory_budget()
    # sys.exit(0)

    # eco(1500, 0.03, 'normal')
    # sys.exit(0)

    ### Scne 12
    # scen12_results_list = []
    # for case in ['normal', 'eco']:
    #     for budget in [400, 1500]:
    #         for iri_impact in [0.01, 0.03]:
    #             step_results_list = eco(budget, iri_impact, case)
    #             scen12_results_list += step_results_list

    # results_df = pd.DataFrame(scen12_results_list, columns=['case', 'budget', 'iri_impact', 'year', 'emi_total', 'vkmt_total', 'vht_total', 'pci_average'])
    # results_df['eco_route_ratio'] = 0
    # results_df.to_csv('scen12_results.csv', index=False)
    # sys.exit(0)

    ### Scen 34
    budget = 1500#int(os.environ['BUDGET']) ### 400 or 1500
    eco_route_ratio = 0.1#float(os.environ['ECO_ROUTE_RATIO']) ### 0.1, 0.5 or 1
    iri_impact = 0.03#float(os.environ['IRI_IMPACT']) ### 0.01 or 0.03
    case = 'er'#float(os.environ['CASE']) ### 'er' for 'routing_only', 'ee' for 'both'
    print('budget {}, eco_route_ratio {}, iri_impact {}, case {}'.format(budget, eco_route_ratio, iri_impact, case))

    eco_incentivize(budget, eco_route_ratio, iri_impact, case)

