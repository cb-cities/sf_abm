### Based on https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
import json
import sys
import numpy as np
import time 
import os
import logging
import datetime
import warnings
import pandas as pd 

pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))

### CO2 - speed function constants (Barth and Boriboonsomsin, "Real-World Carbon Dioxide Impacts of Traffic Congestion")
b0 = 7.362867270508520
b1 = −0.149814315838651
b2 = 0.004214810510200
b3 = −0.000049253951464
b4 = 0.000000217166574

#def eco():

def eco_incentivize():

    ### Read pavement condition on Jan 01, 2017, and degradation model coefficients
    sf_pavement = pd.read_csv(absolute_path+'/input/initial_pavement_data_fillmean.csv')
    sf_pavement = sf_pavement.drop_duplicates(subset='edge_id', keep='first')

    ### Read in the edge attribute. 
    edges_df = pd.read_csv(absolute_path+'/../0_network/data/{}/{}/edges.csv'.format(folder, scenario))
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft']]

    ### Calculate the free flow speed in MPH, as required by the emission-speed model
    edges_df['ffs_mph'] = edges_df['length']/edges_df['fft']*2.23964
    ### FFS_MPH --> speed related emission
    edges_df['base_co2'] = np.exp(b0 + b1*edges_df['ffs_mph'] + b2*edges_df['ffs_mph']^2 + b3*edges_df['ffs_mph']^3 + b4*edges_df['ffs_mph']^4) ### base_co2 is co2 eimission in gram/mile

    ### Get degradation related parameters, incuding the coefficients and initial age
    edges_df = pd.merge(edges_df, sf_pavement[['edge_id', 'alpha', 'beta', 'xi', 'uv', 'initial_age']], on='edge_id', how='left')
    ### Set initial age as the current age
    edges_df['age_current'] = edges_df['initial_age']

    for year in range(20):

        ### Calculate the current pci based on the coefficients and current age
        edges_df['pci_current'] = edges_df['alpha']+edges_df['xi'] + (edges_df['beta']+edges_df['uv'])*edges_df['age_current']/365
        ### Adjust for speed-pci related emission
        edges_df['pci_co2'] = edges_df['base_co2']*(1+0.07*0.03*(100-edges_df['pci_current'])) ### emission in gram per mile
        edges_df['eco_wgh'] = edge_co2['pci_co2']/1609.34*edges_df['length']

        ### Convert to mtx
        wgh = edges_df['eco_wgh']
        g_coo = sp.coo_matrix((wgh, (row, col)), shape=(g.vcount(), g.vcount()))
        print(g_coo.shape, len(g_coo.data))
        sio.mmwrite(absolute_path+'/output/{}/network_sparse.mtx'.format(year), g_coo)
        # g_coo = sio.mmread(absolute_path+'/../data/{}/network_sparse.mtx'.format(folder))

        edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft', 'pci_current']].to_csv(absolute_path+'/output/{}/edges.csv'.format(year), index=False)

        ### Run ABM
        volume_df = pd.read_csv()
        volume_df['link_tot_co2'] = volume_df['pci_co2']*volume_df['aadt']
        edges_repair = volume_df.nlargest(500, 'edge_co2')['edge_id'].tolist()

        edges_df['age_current'] = edges_df['initial_age']+year+1
        edges_df.loc[edges_df['edge_id'].isin(edges_repair), 'age_current'] = 0

if __name__ == '__main__':
    main()

