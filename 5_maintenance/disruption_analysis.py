import pandas as pd
import os
import numpy as np
import sys 

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

def main():
    edges_df = pd.read_csv(absolute_path+'/output/edges_df_closures/preprocessing.csv')
    edges_df['pci_current']=edges_df['alpha']+edges_df['xi'] + (edges_df['beta']+edges_df['uv'])*edges_df['initial_age']/365
    edges_df['aad_vol'] = 0
    edges_df['aad_vht'] = 0
    edges_df['aad_vkmt'] = 0
    edges_df['aad_base_emi'] = 0

    for hour in range(3, 27):
        hour_df = pd.read_csv(absolute_path+'/output/edges_df_closures/edges_df_i0.03_y0_HR{}_cunclosed.csv'.format(hour))
        edges_df = pd.merge(edges_df, hour_df[['edge_id_igraph', 'hour_flow', 'carryover_flow', 't_avg']], on='edge_id_igraph', how='left')
        edges_df['net_vol'] = edges_df['hour_flow'] - edges_df['carryover_flow']
        edges_df['net_vht'] = edges_df['net_vol'] * edges_df['t_avg']/3600
        edges_df['mph_avg'] = edges_df['length']/edges_df['t_avg'] * 2.23694
        edges_df['base_co2'] = base_co2(edges_df['mph_avg'])
        edges_df['base_emi'] = edges_df['base_co2'] * edges_df['length']/1609.34 * edges_df['net_vol']
        
        edges_df['aad_vol'] += edges_df['net_vol']
        edges_df['aad_vht'] += edges_df['net_vht']
        edges_df['aad_vkmt'] += edges_df['net_vol']*edges_df['length']
        edges_df['aad_base_emi'] += edges_df['base_emi']
        edges_df = edges_df[['edge_id_igraph','cnn_expand','length','pci_current', 'aad_vol', 'aad_vht', 'aad_vkmt', 'aad_base_emi', 'type', 'geometry']]

    
    #edges_df = edges_df.sort_values(by='aad_vol', ascending=False).reset_index()
    #print(np.percentile(edges_df['aad_vol'], 99))
    #print(edges_df[~edges_df['type'].isin(['motorway', 'motorway_link', 'trunk'])].head(30))
    #sys.exit(0)
    #cnn_closed = [3284000.0, 11280000.0, 6718000.0, 6717000.0, 13904.0, 26390.0, 8966.0, 7709102.0] #17912, 19465, 17907, 15960, 13904, 26390, 8966, 26392
    #closed_df = edges_df[edges_df['cnn_expand'].isin(cnn_closed)]
    #print(closed_df)
    
    edges_df['aad_pci_emi'] = edges_df['aad_base_emi']*(1+0.0714*0.03*(100-edges_df['pci_current']))

    print('AAD VHT ', np.sum(edges_df['aad_vht']))
    print('AAD VKMT ', np.sum(edges_df['aad_vkmt']))
    print('AAD PCI EMI ', np.sum(edges_df['aad_pci_emi']))
    
    print(edges_df.iloc[0])
    edges_df.to_csv('output/edges_df_closures/cunclosed.csv')


if __name__ == "__main__":
    main()

