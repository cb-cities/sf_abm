import os
import sys
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
from matplotlib.lines import Line2D
from pandas.plotting import parallel_coordinates
import gc 

absolute_path = os.path.dirname(os.path.abspath(__file__))
pd.set_option('display.max_columns', 10)
plt.rcParams.update({'font.size': 18, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

folder = 'sf_overpass'
scenario = 'original'

def plot_pcp(case, scale):

    preprocessing = pd.read_csv(absolute_path+'/preprocessing.csv')
    ### Merge results from two directions
    preprocessing['edge_id_igraph_str'] = preprocessing['edge_id_igraph'].astype(str)
    preprocessing['undir_uv_sp'] = pd.DataFrame(np.sort(preprocessing[['start_sp', 'end_sp']].values, axis=1), columns=['small_sp', 'large_sp']).apply(lambda x:'%s_%s' % (x['small_sp'],x['large_sp']),axis=1)
    preprocessing_grp = preprocessing.groupby('undir_uv_sp').agg({
        'type': 'first', 
        'juris': 'first',
        #'edge_id_igraph_str': lambda x: '-'.join(x),
        }).reset_index()

    base_df = pd.read_csv(absolute_path + '/scen3_e0.csv')
    base_df = base_df.rename(columns={'undirected_normal': 'base'})
    er10_df = pd.read_csv(absolute_path + '/daily_undirected_b700_e0.1_i0.03_cer_y10.csv')
    er10_df = er10_df.rename(columns={'undirected_er': 'er10'})
    er50_df = pd.read_csv(absolute_path + '/daily_undirected_b700_e0.5_i0.03_cer_y10.csv')
    er50_df = er50_df.rename(columns={'undirected_er': 'er50'})
    er100_df = pd.read_csv(absolute_path + '/daily_undirected_b700_e1.0_i0.03_cer_y10.csv')
    er100_df = er100_df.rename(columns={'undirected_er': 'er100'})
    ee100_df = pd.read_csv(absolute_path + '/daily_undirected_b700_e1.0_i0.03_cee_y10.csv')
    ee100_df = ee100_df.rename(columns={'undirected_ee': 'ee100'})

    pcp = pd.merge(preprocessing[['undir_uv_sp', 'type', 'juris']], base_df[['undir_uv_sp', 'base']], on='undir_uv_sp')
    pcp = pd.merge(pcp, er10_df[['undir_uv_sp', 'er10']], on='undir_uv_sp')
    pcp = pd.merge(pcp, er50_df[['undir_uv_sp', 'er50']], on='undir_uv_sp')
    pcp = pd.merge(pcp, er100_df[['undir_uv_sp', 'er100']], on='undir_uv_sp')
    pcp = pd.merge(pcp, ee100_df[['undir_uv_sp', 'ee100']], on='undir_uv_sp')

    if case == 'highway':
        pcp = pcp.loc[pcp['juris'] == 'Caltrans']
        color = 'black'
    else:
        pcp = pcp.loc[pcp['juris'] == 'DPW']
        color = 'g'
    pcp = pcp.iloc[0:100]

    fig, ax = plt.subplots()
    fig.set_size_inches(18,7)
    var_cols = ['juris', 'base', 'er10', 'er50', 'er100', 'ee100']
    parallel_coordinates(pcp.loc[:,var_cols], 'juris', color=color, alpha=0.1)

    ### Shrink current axis's height
    box = ax.get_position()
    ax.set_position([box.x0-box.width*0.05, box.y0, box.width*1.1, box.height])

    plt.xlabel("scenario")
    ax.set_xticklabels(['Base', '3d', '3h', '3l', '4l'])
    if scale=='log':
        plt.ylabel("AAD link volume (log)")
        plt.yscale('log')
    else:
        plt.ylabel("AAD link volume")
    plt.title("Change in {} usage under different emission mitigation scenarios".format(case))

    # remove the pandas legend
    plt.gca().legend_.remove()
    #plt.show()
    plt.savefig(absolute_path+'/pcp_{}_{}.png'.format(case, scale))


if __name__ == '__main__':
    plot_pcp('highway', 'log') ### max flow hour is 18