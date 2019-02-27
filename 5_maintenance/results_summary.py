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
    r12_df = pd.read_csv('scen12_results.csv')
    r34_df = pd.read_csv('scen34_results_test.csv')
    r12_df = r12_df[r34_df.columns.tolist()].copy()
    r1234_df = pd.concat([r12_df, r34_df]).reset_index(drop=True)

    summary_list = []
    for (case, budget, iri_impact, eco_route_ratio) in list(itertools.product(['normal', 'eco', 'er', 'ee'], [400, 1500], [0.01, 0.03], [0.0, 0.1, 0.5, 1.0])):
        slice_df = r1234_df[(r1234_df['case']==case) & (r1234_df['budget']==budget) & (r1234_df['iri_impact']==iri_impact) & (r1234_df['eco_route_ratio']==eco_route_ratio)].copy()
        if slice_df.shape[0]==0: 
            #print(case, budget, iri_impact, eco_route_ratio)
            continue
        CO2_y1 = slice_df.loc[slice_df['year']==0, 'emi_total'].values[0]
        CO2_y10 = slice_df.loc[slice_df['year']==9, 'emi_total'].values[0]
        VKMT = np.average(slice_df['vkmt_total'])/1e7
        VHT = np.average(slice_df['vht_total'])/1e5
        PCI_y1 = slice_df.loc[slice_df['year']==0, 'pci_average'].values[0]
        PCI_y10 = slice_df.loc[slice_df['year']==9, 'pci_average'].values[0]
        summary_list.append([case, budget, iri_impact, eco_route_ratio, CO2_y1, CO2_y10, VKMT, VHT, PCI_y1, PCI_y10])
        ### find base value
        if (case, budget, iri_impact) == ('normal', 400, 0.01):
            base_CO2, base_VKMT, base_VHT, base_PCI= CO2_y1, VKMT, VHT, PCI_y1

    summary_df = pd.DataFrame(summary_list, columns=['case', 'budget', 'iri_impact', 'eco_route_ratio', 'CO2_y1', 'CO2_y10', 'VKMT', 'VHT', 'PCI_y1', 'PCI_y10'])
    print(np.unique(summary_df['eco_route_ratio']))
    summary_abs = summary_df.set_index(['case', 'budget', 'iri_impact', 'eco_route_ratio']).stack().unstack(['budget', 'iri_impact'])
    print(summary_abs)

    relative_df = summary_df.copy()
    relative_df['rel_CO2_y1'] = (relative_df['CO2_y1'] - base_CO2)/base_CO2*100
    relative_df['rel_CO2_y10'] = (relative_df['CO2_y10'] - base_CO2)/base_CO2*100
    relative_df['rel_VKMT'] = (relative_df['VKMT'] - base_VKMT)/base_VKMT*100
    relative_df['rel_VHT'] = (relative_df['VHT'] - base_VHT)/base_VHT*100
    relative_df['abs_PCI_y10'] = (relative_df['PCI_y10'] - base_PCI)
    relative_df = relative_df[['case', 'budget', 'iri_impact', 'eco_route_ratio', 'rel_CO2_y1', 'rel_CO2_y10', 'rel_VKMT', 'rel_VHT', 'abs_PCI_y10']]
    summary_rel = relative_df.set_index(['case', 'budget', 'iri_impact', 'eco_route_ratio']).stack().unstack(['budget', 'iri_impact'])
    print(summary_rel)


if __name__ == '__main__':

    main()



