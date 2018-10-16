import pandas as pd 
import sys
import json
import os 

absolute_path = os.path.dirname(os.path.abspath(__file__))

for day in [0]:
    for hour in range(3, 27):
            OD = pd.read_csv(absolute_path+'/../1_OD/output_scaled/DY{}/SF_OD_DY{}_HR{}.csv'.format(day, day, hour))
            node_osmid2graphid_dict = json.load(open(absolute_path+'/../0_network/data/sf/node_osmid2graphid.json'))
            OD['graph_O'] = OD.apply(lambda row: node_osmid2graphid_dict[str(row['O'])], axis=1)
            OD['graph_D'] = OD.apply(lambda row: node_osmid2graphid_dict[str(row['D'])], axis=1)
            OD = OD[['graph_O', 'graph_D', 'flow']]
            OD = OD.rename(columns={'graph_O': 'O', 'graph_D': 'D'})
            OD = OD.sample(frac=1).reset_index(drop=True) ### randomly shuffle rows

            OD.to_csv(absolute_path+'/../1_OD/output_scaled_graphid/DY{}/SF_OD_DY{}_HR{}.csv'.format(day, day, hour))