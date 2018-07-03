import json
import pandas as pd 
from pandas.util.testing import assert_frame_equal
import sys 

def main():
    one_process_edge_volume = json.load(open('edge_volume_1p.json'))
    four_processes_edge_volume = json.load(open('edge_volume_4ps.json'))

    one_p_df = pd.DataFrame(list(one_process_edge_volume.items()), columns=['edge_GraphID', 'vol']).sort_values(by='edge_GraphID').reset_index(drop=True)
    four_p_df = pd.DataFrame(list(four_processes_edge_volume.items()), columns=['edge_GraphID', 'vol']).sort_values(by='edge_GraphID').reset_index(drop=True)
    print(one_p_df.head())
    print(four_p_df.head())

    print(assert_frame_equal(one_p_df, four_p_df))

if __name__ == '__main__':
    main()