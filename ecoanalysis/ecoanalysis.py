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
import logging
import datetime
import warnings
import pandas as pd 
import matplotlib.pyplot as plt
import matplotlib.cm as cm 
import gc

absolute_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, absolute_path+'/../')
from residual_demand_sim import sf_residual_demand

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})
pd.set_option('display.max_columns', 10)

folder = 'sf_overpass'
outdir = absolute_path + '/output_LCA2020'

highway_type = ['motorway', 'motorway_link', 'trunk', 'trunk_link']

logging.basicConfig(filename=absolute_path+'/eco_analysis_c{}.log'.format(os.environ['CASE']), level=logging.INFO)
logger = logging.getLogger('start')
logger.info('{}'.format(datetime.datetime.now()))

def base_co2(mph_array):
    ### CO2 - speed function constants (Barth and Boriboonsomsin, "Real-World Carbon Dioxide Impacts of Traffic Congestion")
    b0 = 7.362867270508520
    b1 = -0.149814315838651
    b2 = 0.004214810510200
    b3 = -0.000049253951464
    b4 = 0.000000217166574
    return np.exp(b0 + b1*mph_array + b2*mph_array**2 + b3*mph_array**3 + b4*mph_array**4)

def aad_vol_vmt_baseemi(aad_df, year='', day='', hour='', quarter='', residual=True, case='', random_seed=''):

    quarter_volume_df = pd.read_csv('{}/edges_df/edges_df_YR{}_DY{}_HR{}_qt{}_res{}_c{}_r{}.csv'.format(outdir, year, day, hour, quarter, residual, case, random_seed))
    aad_df = pd.merge(aad_df, quarter_volume_df, on='edge_id_igraph', how='left')
    # print(np.sum(aad_df['aad_vol']))
    aad_df['vht'] = aad_df['true_vol'] * aad_df['t_avg']/3600
    aad_df['v_avg_mph'] = aad_df['length']/aad_df['t_avg'] * 2.23694 ### time step link speed in mph
    aad_df['base_co2'] = base_co2(aad_df['v_avg_mph']) ### link-level co2 eimission in gram per mile per vehicle
    ### correction for slope
    aad_df['base_co2'] = aad_df['base_co2'] * aad_df['slope_factor']
    aad_df['base_emi'] = aad_df['base_co2'] * aad_df['length'] /1609.34 * aad_df['true_vol'] ### speed related CO2 x length x flow. Final results unit is gram.

    aad_df['aad_vol'] += aad_df['true_vol']
    aad_df['aad_vht'] += aad_df['vht']
    aad_df['aad_vmt'] += aad_df['true_vol']*aad_df['length']
    aad_df['aad_base_emi'] += aad_df['base_emi']
    aad_df = aad_df[['edge_id_igraph', 'length', 'type', 'slope_factor', 'aad_vol', 'aad_vht', 'aad_vmt', 'aad_base_emi']]
    return aad_df

def preprocessing(offset=True):
    ### Read the edge attributes. 
    edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/edges_elevation.csv'.format(folder))
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'lanes', 'slope', 'capacity', 'fft', 'type', 'geometry']]
    edges_df['slope_factor'] = np.where(edges_df['slope']<-0.05, 0.2, np.where(edges_df['slope']>0.15, 3.4, 1+0.16*(edges_df['slope']*100)))

    ### PCI RELATED EMISSION
    ### Read pavement age on Jan 01, 2017, and degradation model coefficients
    sf_pavement = pd.read_csv(absolute_path+'/input/r_to_python_20190323.csv')
    sf_pavement['initial_age'] *= 365
    sf_pavement['ispublicworks'] = 1

    ### Key to merge cnn with igraphid
    sf_cnn_igraphid = pd.read_csv(absolute_path+'/input/3_cnn_igraphid.csv')
    sf_cnn_igraphid = sf_cnn_igraphid[sf_cnn_igraphid['edge_id_igraph']!='None'].reset_index()
    sf_cnn_igraphid['edge_id_igraph'] = sf_cnn_igraphid['edge_id_igraph'].astype('int64')
    ### Get degradation related parameters, incuding the coefficients and initial age
    edges_df = pd.merge(edges_df, sf_cnn_igraphid, on='edge_id_igraph', how='left')
    
    ### Fill cnn na with edge_id_igraph
    edges_df['cnn_expand'] = np.where(pd.isna(edges_df['cnn']), edges_df['edge_id_igraph'], edges_df['cnn'])
    edges_df = pd.merge(edges_df, sf_pavement[['cnn', 'ispublicworks', 'stfcbr', 'alpha', 'beta', 'xi', 'uv', 'initial_age']], left_on='cnn_expand', right_on='cnn', how='left')
    edges_df['cnn_expand'] = edges_df['cnn_expand'].astype(int).astype(str)

    ### Keep relevant columns
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'lanes', 'slope', 'slope_factor', 'capacity', 'fft', 'cnn_expand', 'ispublicworks', 'stfcbr', 'alpha', 'beta', 'xi', 'uv', 'initial_age', 'type', 'geometry']]
    ### Remove duplicates
    edges_df = edges_df.drop_duplicates(subset='edge_id_igraph', keep='first').reset_index()
    edges_df['juris'] = np.where(edges_df['ispublicworks']==1, 'DPW',
        np.where(edges_df['type'].isin(highway_type), 'Caltrans', 'no'))

    ### Some igraphids have empty coefficients and age, set to average
    #edges_df['initial_age'] = edges_df['initial_age'].fillna(edges_df['initial_age'].mean())
    edges_df['initial_age'] = edges_df['initial_age'].fillna(0)
    edges_df['alpha'] = edges_df['alpha'].fillna(edges_df['alpha'].mean())
    edges_df['beta'] = edges_df['beta'].fillna(edges_df['beta'].mean())
    edges_df['xi'] = edges_df['xi'].fillna(0)
    edges_df['uv'] = edges_df['uv'].fillna(0)
    edges_df['intercept'] = edges_df['alpha'] + edges_df['xi']
    if offset:
        edges_df['intercept'] -= 5.5 ### to match sf public works record
    edges_df['slope'] = edges_df['beta'] + edges_df['uv']

    ### Not considering non publicwork roads
    edges_df['initial_age'] = np.where(edges_df['juris']=='no', 0, edges_df['initial_age'])
    edges_df['intercept'] = np.where(edges_df['juris']=='no', 100, edges_df['intercept']) ### highway PCI is assumed to be 85 throughout based on Caltrans 2015 state of the pavement report
    edges_df['slope'] = np.where(edges_df['juris']=='no', 0, edges_df['slope'])

    ### Not considering highways
    edges_df['initial_age'] = np.where(edges_df['juris']=='Caltrans', 0, edges_df['initial_age'])
    edges_df['intercept'] = np.where(edges_df['juris']=='Caltrans', 85, edges_df['intercept']) ### highway PCI is assumed to be 85 throughout based on Caltrans 2015 state of the pavement report
    edges_df['slope'] = np.where(edges_df['juris']=='Caltrans', 0, edges_df['slope'])

    ### Set initial age as the current age
    edges_df['age_current'] = edges_df['initial_age'] ### age in days
    ### Current conditions
    edges_df['pci_current'] = edges_df['intercept'] + edges_df['slope'] * edges_df['age_current']/365
    edges_df['pci_current'] = np.where(
            edges_df['pci_current']>100, 100, np.where(
                edges_df['pci_current']<0, 0, edges_df['pci_current']))

    print('total_blocks', len(np.unique(edges_df[edges_df['ispublicworks']==1]['cnn_expand'])))
    print('initial condition: ', np.mean(edges_df[(~edges_df['type'].isin(highway_type))&(edges_df['ispublicworks']==1)]['pci_current']))
    print('edges<63: ', sum(edges_df[(~edges_df['type'].isin(highway_type))&(edges_df['ispublicworks']==1)]['pci_current']<63))
    
    edges_df.to_csv('{}/preprocessing.csv'.format(outdir), index=False)

    return edges_df

def eco_incentivize(random_seed='', budget='', eco_route_ratio='', iri_impact='', case='', traffic_growth='', residual='', day='', total_years='', improv_pct='', closure_list=[], closure_case=''):

    ### Network preprocessing
    edges_df = preprocessing()
    emi_results_list = []
    traf_results_list = []

    for year in range(total_years):
        gc.collect()

        ### Update current PCI
        edges_df['pci_current'] = edges_df['intercept'] + edges_df['slope']*edges_df['age_current']/365
        edges_df['pci_current'] = np.where(
            edges_df['pci_current']>100, 100, np.where(
                edges_df['pci_current']<0, 0, edges_df['pci_current']))
        
        ### Initialize the annual average daily
        aad_df = edges_df[['edge_id_igraph', 'length', 'type', 'slope_factor']].copy()
        aad_df = aad_df.assign(**{'aad_vol': 0, 'aad_vht': 0, 'aad_vmt': 0, 'aad_base_emi': 0})
        ### aad_vht: daily vehicle hours travelled
        ### aad_base_emi: emission not considering pavement degradations

        ### INITIAL GRAPH WEIGHTS: SPEED RELATED EMISSION
        ### Calculate the free flow speed in MPH, as required by the emission-speed model
        ### This is not the same as the maxspeed, as ffs also considers 1.2 delay and intersection delay
        edges_df['ffs_mph'] = edges_df['length']/edges_df['fft']*2.23964
        edges_df['base_co2_ffs'] = base_co2(edges_df['ffs_mph']) ### link-level co2 eimission in gram per mile per vehicle
        ### Adjust emission by considering the impact of pavement degradation
        edges_df['pci_co2_ffs'] = edges_df['base_co2_ffs']*(1+0.0714*iri_impact*(100-edges_df['pci_current'])) ### emission in gram per mile per vehicle. 0.0714 is to convert PCI to IRI
        edges_df['eco_wgh'] = edges_df['pci_co2_ffs']/1609.34*edges_df['length']

        ### Output network graph for ABM simulation
        ### Shape of the network as a sparse matrix
        g_time = sio.mmread(absolute_path+'/../0_network/data/{}/network_sparse.mtx'.format(folder))
        g_time_shape = g_time.shape
        wgh = edges_df['eco_wgh']
        row = edges_df['start_sp']-1
        col = edges_df['end_sp']-1
        g_eco = scipy.sparse.coo_matrix((wgh, (row, col)), shape=g_time_shape)
        sio.mmwrite('{}/network/network_sparse_r{}_b{}_e{}_i{}_c{}_tg{}_y{}.mtx'.format(outdir, random_seed, budget, eco_route_ratio, iri_impact, case, traffic_growth, year), g_eco)

        ### Output edge attributes for ABM simulation
        abm_edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'slope_factor', 'length', 'capacity', 'fft', 'pci_current', 'eco_wgh']].copy()

        ### Run ABM
        traf_stats, y = sf_residual_demand.quasi_sta(abm_edges_df, traffic_only=False, outdir=outdir, year=year, day=day, quarter_counts=4, random_seed=random_seed, residual=residual, budget=budget, eco_route_ratio=eco_route_ratio, iri_impact=iri_impact, case=case, traffic_growth=traffic_growth, closure_list=closure_list, closure_case=closure_case)
        traf_results_list += traf_stats

        for hour in range(3, 27):
            for quarter in range(4):
                aad_df = aad_vol_vmt_baseemi(aad_df, year=year, day=day, hour=hour, quarter=quarter, residual=residual, case=case, random_seed=random_seed)

        # print(np.sum(aad_df['aad_vol']))
        # sys.exit(0)

        ### Get pci adjusted emission
        aad_df = pd.merge(aad_df, edges_df[['edge_id_igraph', 'cnn_expand', 'juris', 'pci_current', 'intercept', 'slope', 'age_current']], on='edge_id_igraph', how='left')

        ### Adjust emission by considering the impact of pavement degradation
        aad_df['aad_pci_emi'] = aad_df['aad_base_emi']*(1+0.0714*iri_impact*(100-aad_df['pci_current'])) ### daily emission (aad) in gram

        aad_df['aad_emi_potential'] = aad_df['aad_base_emi']*(0.0714*iri_impact*(improv_pct * (100 - aad_df['pci_current'])))

        def pci_improvement(df, year, case, budget, eco_route_ratio, iri_impact): ### repair worst roads
            repair_df = df[df['juris']=='DPW'].copy()
            repair_df = repair_df.groupby(['cnn_expand']).agg({'pci_current': np.mean}).reset_index().nsmallest(budget, 'pci_current')
            repair_list = repair_df['cnn_expand'].tolist()
            # extract_df = df.loc[df['cnn_expand'].isin(repair_list)]
            # extract_df[['edge_id_igraph', 'intercept', 'pci_current']].to_csv(absolute_path+'/{}/repair_df/repair_df_y{}_c{}_b{}_e{}_i{}.csv'.format(outdir, year, case, budget, eco_route_ratio, iri_impact), index=False)
            return repair_list

        def eco_maintenance(df, year, case, budget, eco_route_ratio, iri_impact):
            repair_df = df[df['juris']=='DPW'].copy()
            repair_df = repair_df.groupby(['cnn_expand']).agg({'aad_emi_potential': np.sum}).reset_index().nlargest(budget, 'aad_emi_potential')
            repair_list = repair_df['cnn_expand'].tolist()
            # extract_df = df.loc[df['cnn_expand'].isin(repair_list)]
            #extract_df[['edge_id_igraph', 'aad_emi_potential']].to_csv('repair_df/repair_df_y{}_c{}_b{}_e{}_i{}.csv'.format(year, case, budget, eco_route_ratio, iri_impact))
            return repair_list

        if case in ['nr', 'er', 'ps']: 
            repair_list = pci_improvement(aad_df, year, case, budget, eco_route_ratio, iri_impact)
            ### Repair
            edges_df['age_current'] = edges_df['age_current']+365
            edges_df['intercept'] = np.where(edges_df['cnn_expand'].isin(repair_list),
                edges_df['intercept'] + improv_pct*(100-edges_df['pci_current']),
                edges_df['intercept'])

        elif case in ['em', 'ee']:
            repair_list = eco_maintenance(aad_df, year, case, budget, eco_route_ratio, iri_impact)
            ### Repair
            edges_df['age_current'] = edges_df['age_current']+365
            edges_df['intercept'] = np.where(edges_df['cnn_expand'].isin(repair_list),
                edges_df['intercept'] + improv_pct*(100-edges_df['pci_current']),
                edges_df['intercept'])
        else:
            print('no matching maintenance strategy')

        ### Results
        ### emi
        emi_total = np.sum(aad_df['aad_pci_emi'])/1e6 ### co2 emission in t
        emi_local = np.sum(aad_df[aad_df['juris']=='DPW']['aad_pci_emi'])/1e6
        emi_highway = np.sum(aad_df[aad_df['juris']=='Caltrans']['aad_pci_emi'])/1e6
        emi_localroads_base = np.sum(aad_df[aad_df['juris']=='DPW']['aad_base_emi'])/1e6

        ### vht
        vht_total = np.sum(aad_df['aad_vht']) ### vehicle hours travelled
        vht_local = np.sum(aad_df[aad_df['juris']=='DPW']['aad_vht'])
        vht_highway = np.sum(aad_df[aad_df['juris']=='Caltrans']['aad_vht'])
        ### vkmt
        vkmt_total = np.sum(aad_df['aad_vmt'])/1000 ### vehicle kilometers travelled
        vkmt_local = np.sum(aad_df[aad_df['juris']=='DPW']['aad_vmt'])/1000
        vkmt_highway = np.sum(aad_df[aad_df['juris']=='Caltrans']['aad_vmt'])/1000
        ### pci
        pci_average = np.mean(aad_df['pci_current'])
        pci_local = np.mean(aad_df[aad_df['juris']=='DPW']['pci_current'])
        pci_highway = np.mean(aad_df[aad_df['juris']=='Caltrans']['pci_current'])

        emi_results_list.append([random_seed, case, budget, iri_impact, eco_route_ratio, year, emi_total, emi_local, emi_highway, emi_localroads_base, pci_average, pci_local, pci_highway, vht_total, vht_local, vht_highway, vkmt_total, vkmt_local, vkmt_highway])
    #print(step_results_list[0:10:9])
    return traf_results_list, emi_results_list

def scenarios():

    ### Emission analysis parameters
    random_seed = 0#int(os.environ['RANDOM_SEED']) ### 0,1,2,3,4,5,6,7,8,9
    ### Fix random seed
    np.random.seed(random_seed)

    budget = 700#int(os.environ['BUDGET']) ### 200 or 700
    eco_route_ratio = 1.0#float(os.environ['ECO_ROUTE_RATIO']) ### 0.1, 0.5 or 1
    iri_impact = 0.03#float(os.environ['IRI_IMPACT']) ### 0.01 or 0.03

    case = os.environ['CASE'] ### 'nr' no eco-routing or eco-maintenance, 'em' for eco-maintenance, 'er' for 'routing_only', 'ee' for 'both'
    if case in ['nr', 'em', 'ps']:
        eco_route_ratio = 0    
    
    print('random_seed {}, budget {}, eco_route_ratio {}, iri_impact {}, case {}, traffic_growth {}'.format(random_seed, budget, eco_route_ratio, iri_impact, case, traffic_growth))

    ### ABM parameters
    day = 2 ### Wednesday

    ### simulation period
    total_years = 11

    residual = 1
    improv_pct = 1
    traffic_growth = 1

    traf_results_list, emi_results_list = eco_incentivize(random_seed=random_seed, budget=budget, eco_route_ratio=eco_route_ratio, iri_impact=iri_impact, case=case, traffic_growth=traffic_growth, residual=residual, day=day, total_years=total_years, improv_pct=improv_pct)
    traf_results_df = pd.DataFrame(traf_results_list, columns = ['random_seed', 'year', 'day', 'hour', 'quarter', 'quarter_demand', 'inclu_residual_demand', 'prod_residual_demand', 'quarter_avg_min', 'quarter_avg_km', 'avg_max10_vol'])
    emi_results_df = pd.DataFrame(emi_results_list, columns=['random_seed', 'case', 'budget', 'iri_impact', 'eco_route_ratio', 'year', 'emi_total', 'emi_local', 'emi_highway', 'emi_localroads_base',  'pci_average', 'pci_local', 'pci_highway', 'vht_total', 'vht_local', 'vht_highway', 'vkmt_total', 'vkmt_local', 'vkmt_highway'])

    traf_results_df.to_csv('{}/summary/traf_summary_c{}.csv'.format(outdir, case), index=False)
    emi_results_df.to_csv('{}/summary/emi_summary_c{}.csv'.format(outdir, case), index=False)

if __name__ == '__main__':

    ### Running different eco-maintenance and eco-routing scenarios
    scenarios()

