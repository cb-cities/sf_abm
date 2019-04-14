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

pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))

def main():
    df = pd.read_csv(absolute_path + '/output_march19/results/scen12_results.csv')
    df = df.loc[df['case']=='eco']
    df = df.loc[df['eco_route_ratio']==0.0]
    df = df.loc[df['iri_impact']==0.03]
    df = df.loc[df['year'].isin([1, 10])]
    df = df[['case','budget','iri_impact','year','emi_total','emi_local','emi_highway','pci_local','vht_total','vht_local','vht_highway','vkmt_total','vkmt_local','vkmt_highway']]
    #print(df.head())
    #df_grp = df.groupby(['year', 'budget', 'iri_impact'])['emi_total', 'emi_local','emi_highway','pci_local','vht_total','vht_local','vht_highway'].apply(lambda df: df.reset_index(drop=True)).unstack()
    df_grp = df.groupby(['year', 'budget', 'iri_impact'])['year', 'emi_total', 'emi_local','emi_highway','vht_total','vht_local','vht_highway','vkmt_total','vkmt_local','vkmt_highway','pci_local'].apply(lambda df: df.reset_index(drop=True)).unstack().transpose().astype(int)
    print(df_grp.to_latex())

if __name__ == '__main__':

    main()



