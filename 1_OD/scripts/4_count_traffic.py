import pandas as pd 
import numpy as np 
import os
import sys

absolute_path = os.path.dirname(os.path.abspath(__file__))

def main(year, day):
    OD_df_list = []
    OD_count = 0
    for hour in range(3, 27):
        intraSF_OD_df = pd.read_csv(open(absolute_path+'/../output/OD_tables_growth/intraSF/SF_OD_YR{}_DY{}_HR{}.csv'.format(year, day, hour)))
        #intraSF_OD_df = intraSF_OD_df[['O', 'D', 'flow']]
        OD_df_list.append(intraSF_OD_df)
        try:
            OD_count += np.sum(intraSF_OD_df['flow'])
        except KeyError:
            OD_count += intraSF_OD_df.shape[0]

        intercity_OD_df = pd.read_csv(open(absolute_path+'/../output/OD_tables_growth/intercity/intercity_YR{}_HR{}.csv'.format(year, hour)))
        #intercity_OD_df = intercity_OD_df[['O', 'D', 'flow']]
        OD_df_list.append(intercity_OD_df)
        try:
            OD_count += np.sum(intercity_OD_df['flow'])
        except KeyError:
            OD_count += intercity_OD_df.shape[0]

    print(year, OD_count)

if __name__ == '__main__':
    
    for year in range(11):
        main(year, 2)
