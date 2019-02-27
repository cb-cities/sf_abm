import pandas as pd 
import numpy as np 
from scipy.sparse import coo_matrix
import os 
import sys 
import matplotlib.pyplot as plt 
from matplotlib.colors import LogNorm
from mpl_toolkits.mplot3d import Axes3D 
import geopandas as gpd 
import shapely.wkt

plt.rcParams.update({'font.size': 15, 'font.weight': "normal", 'font.family':'serif', 'axes.linewidth': 0.1})

absolute_path = os.path.dirname(os.path.abspath(__file__))
folder = 'sf_overpass'
scenario = 'original'

def get_grid(budget, iri_impact, case):
    all_repair_df = pd.read_csv('repair_df_y0_c{}_b{}_i{}.csv'.format(case, budget, iri_impact))
    all_repair_df['year'] = 0

    for year in range(1, 10):
        repair_df = pd.read_csv('repair_df_y{}_c{}_b{}_i{}.csv'.format(year, case, budget, iri_impact))
        repair_df['year'] = year
        all_repair_df = pd.concat([all_repair_df, repair_df])
    
    return all_repair_df

def maintenance_spatial_plot():

    ### add spatial
    edges_df = pd.read_csv(absolute_path+'/../../0_network/data/{}/{}/edges.csv'.format(folder, scenario))
    edges_df = edges_df[['edge_id_igraph', 'start_sp', 'end_sp', 'length', 'capacity', 'fft', 'type', 'geometry']]
    edges_gdf = gpd.GeoDataFrame(edges_df, crs={'init': 'epsg:4326'}, geometry=edges_df['geometry'].map(shapely.wkt.loads))

    edges_gdf['x'] = edges_gdf['geometry'].centroid.x
    edges_gdf['y'] = edges_gdf['geometry'].centroid.y

    all_repair_df = get_grid(1500, 0.03, 'eco')
    all_repair_df = pd.merge(all_repair_df, edges_gdf[['edge_id_igraph', 'x', 'y', 'geometry']], on='edge_id_igraph', how='left')
    # sf_pci_maintenance['date_int'] = (sf_pci_maintenance['date'] - min(sf_pci_maintenance['date'])).dt.days
    # ### arange by date
    # #sf_pci_maintenance = sf_pci_maintenance.sort_values(by='date').reset_index()
    # #print(sf_pci_maintenance[['CNN', 'date']].head())

    # ### 3D scatter plot
    # maintenance_by_date = sf_pci_maintenance.groupby(sf_pci_maintenance['date']).size().reset_index(name='counts')
    # spatial_plot_df = sf_pci_maintenance[sf_pci_maintenance['date'].isin(maintenance_by_date[maintenance_by_date['counts']>100]['date'])]
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(all_repair_df['x'], 
        all_repair_df['y'], 
        all_repair_df['year'], 
        c = all_repair_df['aad_emi_potential'], cmap='rainbow', s=3, alpha=0.3, edgecolors='none')
    ax.set(xlabel='longitude', ylabel='latitude', zlabel='years')
    #ax.grid(False)
    plt.title('Spatio-temporal occurances of maintenances')
    plt.show()
    #plt.savefig('spatiotemporal_maintenances.png')

def grid_plot():
    df_eco = get_grid(1500, 0.03, 'eco')
    m_eco = coo_matrix((df_eco['aad_emi_potential'], (df_eco['year'], df_eco['edge_id_igraph'])), shape=(10, 26893))
    a_eco = m_eco.toarray()

    df_normal = get_grid(1500, 0.03, 'normal')
    m_normal = coo_matrix((df_normal['aad_emi_potential'], (df_normal['year'], df_normal['edge_id_igraph'])), shape=(10, 26893))
    a_normal = m_normal.toarray()

    vmin = min(a_eco.min(), a_normal.min())
    vmax = min(a_eco.max(), a_normal.max())
    df_eco['c'] = np.log(df_eco['aad_emi_potential'])

    fig, ax = plt.subplots()
    fig.set_size_inches(10, 5)
    # #ax.imshow(a_normal[:, 0:100], cmap='hot', interpolation='nearest')
    # data = df_eco[df_eco['edge_id_igraph']<1000]
    # ax.scatter(data['edge_id_igraph'], data['year'], c=data['aad_emi_potential'], cmap='gray')
    # #plt.colorbar()
    # plt.show()

    plt.subplot(211)
    plt.imshow(a_normal[:, 0:200], cmap=plt.cm.gnuplot_r)
    plt.gca().set_title("(a) Scenario 1: Condition-based maintenance", fontsize=15)
    plt.gca().set_xlabel("Road ID", fontsize=15)
    plt.gca().set_ylabel("Year", fontsize=15)
    plt.subplot(212)
    plt.imshow(a_eco[:, 0:200], cmap=plt.cm.gnuplot_r)
    plt.gca().set_title("(b) Scenario 2: Eco-maintenance", fontsize=15)
    plt.gca().set_xlabel("Road ID", fontsize=15)
    plt.gca().set_ylabel("Year", fontsize=15)

    plt.subplots_adjust(bottom=0.2, right=0.8, left=0.05, top=0.75)
    cax = plt.axes([0.85, 0.2, 0.035, 0.55])
    cbar = plt.colorbar(cax=cax)
    cbar.ax.tick_params(labelsize=12) 
    ax = cbar.ax
    ax.text(-0.7,0.95,'AAD-CO\u2082 reduction on link', rotation=90)
    #plt.show()
    plt.savefig('maintenance_pattern_imshow.png', dpi=300, transparent=True)

if __name__ == '__main__':
    #maintenance_spatial_plot()
    grid_plot()