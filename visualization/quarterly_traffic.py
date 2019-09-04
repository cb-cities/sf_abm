import scipy.io as sio
import json
import sys
import numpy as np 
import pandas as pd 
import geopandas as gpd
import os 
import matplotlib as mpl 
import matplotlib.pyplot as plt 
import matplotlib.colors as pltcolors
import descartes 
import shapely.wkt 
import gc 

absolute_path = os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.size': 10, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

folder = 'sf_overpass'

def make_img(day, hour, quarter, residual, random_seed, quarter_counts=4):

    ### Get quarterly flow of a particular snapshot of the day
    edge_flow_df = pd.read_csv(absolute_path+'/../residual_demand_sim/output/edges_df/edges_df_hpc/edges_df_YR0_DY{}_HR{}_qt{}_res{}_r{}.csv'.format(day, hour, quarter, residual, random_seed))

    ### Get attributes and geometry of each edge
    network_attr_df = pd.read_csv(absolute_path+'/../0_network/data/{}/edges_elevation.csv'.format(folder), index_col=0)
    edge_flow_df = pd.merge(edge_flow_df, network_attr_df, on = ['edge_id_igraph'])
    edge_flow_df['edge_id_igraph_str'] = edge_flow_df['edge_id_igraph'].astype(str)
    edge_flow_df['voc'] = edge_flow_df['true_vol']*quarter_counts/edge_flow_df['capacity']

    ### Merge results from two directions
    edge_flow_df['undir_uv_igraph'] = pd.DataFrame(np.sort(edge_flow_df[['start_igraph', 'end_igraph']].values, axis=1), columns=['small_igraph', 'large_igraph']).apply(lambda x:'%s_%s' % (x['small_igraph'],x['large_igraph']),axis=1)
    edge_flow_df_grp = edge_flow_df.groupby('undir_uv_igraph').agg({
            'true_vol': np.sum, 
            'voc': np.max,
            'edge_id_igraph_str': lambda x: '-'.join(x),
            'geometry': 'first'}).reset_index()
    edge_flow_df_grp = edge_flow_df_grp.rename(columns={'true_vol': 'undirected_quart_vol', 'voc': 'larger_voc'})
    #print(hour, quarter, np.max(edge_flow_df_grp['undirected_quart_vol']))
    print(edge_flow_df_grp.nlargest(1, 'larger_voc').iloc[0]['undir_uv_igraph'])
    return
    
    edge_flow_gdf = gpd.GeoDataFrame(edge_flow_df_grp, 
        crs={'init': 'epsg:4326'}, 
        geometry=edge_flow_df_grp['geometry'].map(shapely.wkt.loads))
    edge_flow_gdf = edge_flow_gdf.to_crs(epsg='7131')
    edge_flow_gdf = edge_flow_gdf.append([edge_flow_gdf.iloc[-1]],ignore_index=True)
    edge_flow_gdf.at[edge_flow_gdf.shape[0]-1, 'undirected_quart_vol'] = 10000
    edge_flow_gdf.at[edge_flow_gdf.shape[0]-1, 'geometry'] = 'LINESTRING ()'
    # print(edge_flow_gdf.tail())
    # sys.exit(0)
    # edge_flow_gdf['norm_undir_quart_vol'] = edge_flow_gdf['undirected_quart_vol']/np.max(edge_flow_gdf['undirected_quart_vol'])*10000

    fig = plt.figure(figsize=(5, 6))
    ax1 = fig.add_axes([0.05, 0.15, 0.9, 0.9])
    # ax1.set_yticklabels([])
    # ax1.set_xticklabels([])
    ax1.tick_params(axis='both', which='both', bottom=False, top=False, labelbottom=False, right=False, left=False, labelleft=False)
    ax2 = fig.add_axes([0.05, 0.1, 0.9, 0.05])
    bounds = np.array([0, 10, 50, 100, 200, 250, 500, 1000, 5000, 10000])
    cmap = mpl.cm.get_cmap('inferno_r')
    norm = pltcolors.BoundaryNorm(boundaries=bounds, ncolors=cmap.N)
    map_plot = edge_flow_gdf.plot(ax=ax1, column='undirected_quart_vol', cmap=cmap, norm=norm, lw=0.5)
    # fig.colorbar(map_plot, extend='both', orientation='vertical')
    cb = mpl.colorbar.ColorbarBase(ax2, cmap=cmap, norm=norm, orientation='horizontal')
    ax2.set_title('HR {} QT {} \n 15 min traffic volume in both directions'.format(hour, quarter))
    #, spacing='proportional')
    #plt.show()
    plt.savefig(absolute_path+'/traffic_map/traffic_DY{}_HR{}_res{}_qt{}_r{}.png'.format(day, hour, residual, quarter, random_seed))

    #edge_flow_df_grp.to_csv(absolute_path+'/quarterly_traffic/edges_df_DY{}_HR{}_res{}_qt{}_r{}.csv'.format(day, hour, residual, quarter, random_seed), index=False)

def make_gif():

    import imageio

    images = []
    for day in [2]:
        for hour in range(3, 27):
            for quarter in [0,1,2,3]:
                for residual in [1]:
                    for random_seed in [0]:
                        make_img(day, hour, quarter, residual, random_seed)
                        images.append(imageio.imread(absolute_path+'/traffic_map/traffic_DY{}_HR{}_res{}_qt{}_r{}.png'.format(day, hour, residual, quarter, random_seed)))
    imageio.mimsave(absolute_path+'/traffic_map/traffic_map.gif', images, fps=2)


if __name__ == '__main__':
                        
    make_gif()



