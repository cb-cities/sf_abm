import pandas as pd 
import numpy as np 
import os
import sys

absolute_path = os.path.dirname(os.path.abspath(__file__))

def main(year, day):
    OD_count = 0
    intraSF_OD_count = 0
    intercity_OD_count = 0

    for hour in range(3, 27):
        intraSF_OD_df = pd.read_csv(open(absolute_path+'/../output/OD_tables_growth/intraSF/SF_OD_YR{}_DY{}_HR{}.csv'.format(year, day, hour)))
        try:
            OD_count += np.sum(intraSF_OD_df['flow'])
            intraSF_OD_count += np.sum(intraSF_OD_df['flow'])
        except KeyError:
            OD_count += intraSF_OD_df.shape[0]
            intraSF_OD_count += intraSF_OD_df.shape[0]

        intercity_OD_df = pd.read_csv(open(absolute_path+'/../output/OD_tables_growth/intercity/intercity_YR{}_HR{}.csv'.format(year, hour)))
        try:
            OD_count += np.sum(intercity_OD_df['flow'])
            intercity_OD_count += np.sum(intercity_OD_df['flow'])
        except KeyError:
            OD_count += intercity_OD_df.shape[0]
            intercity_OD_count += intercity_OD_df.shape[0]

    return OD_count, intraSF_OD_count, intercity_OD_count

if __name__ == '__main__':
    
    OD_count_list = []
    for year in range(11):
        OD_count, intraSF_OD_count, intercity_OD_count = main(year, 2)
        OD_count_list.append((year, OD_count, intraSF_OD_count, intercity_OD_count))
    OD_count_df = pd.DataFrame(OD_count_list, columns=['year', 'total', 'intraSF', 'intercity'])
    OD_count_df.to_csv('OD_count.csv', index=False)
