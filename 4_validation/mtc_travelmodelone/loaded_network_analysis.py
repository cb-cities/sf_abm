### Data dictionary: https://github.com/BayAreaMetro/modeling-website/wiki/LoadedHighway
import sys 
import pandas as pd 
import geopandas as gpd 
import shapely.wkt 
import numpy as np 
import haversine
import matplotlib.pyplot as plt 
import matplotlib 

def main():
    ### Read data
    loaded_network = gpd.read_file('avgload5period/avgload5period.shp')
    loaded_network.crs = {'init': 'epsg:3717'}
    loaded_network = loaded_network.to_crs({'init': 'epsg:4326'})
    print(loaded_network.shape)
    osm_network = pd.read_csv('../../3_visualization/weekly_traffic.csv')
    osm_network = gpd.GeoDataFrame(osm_network,
                                    crs={'init': 'epsg:4326'},
                                    geometry = osm_network['geometry'].apply(shapely.wkt.loads))
    ### Clip by bbox
    osm_xmin, osm_ymin, osm_xmax, osm_ymax = osm_network.total_bounds
    sf_loaded_network = loaded_network.cx[osm_xmin:osm_xmax, osm_ymin:osm_ymax].reset_index()
    ### Clip by convex hull
    osm_convex_hull = osm_network.unary_union.convex_hull
    sf_loaded_network['geometry'] = sf_loaded_network.apply(lambda row: row['geometry'].intersection(osm_convex_hull), axis=1)
    sf_loaded_network = sf_loaded_network[sf_loaded_network['geometry'].notnull()].reset_index()

    ### Vehicle class https://github.com/BayAreaMetro/modeling-website/wiki/LoadedHighway
    vehicle_class = ['DA', 'DAT', 'S2', 'S2T', 'S3', 'S3T', 'SM', 'SMT', 'HV', 'HVT']

    ### Keep two overlapping directions as it is
    sf_loaded_network['daily_traffic'] = 0
    print(sf_loaded_network.shape)
    for vc in vehicle_class:
        sf_loaded_network['daily_traffic'] += sf_loaded_network['VOL24HR_{}'.format(vc)]
    sf_loaded_network['fiveday_traffic'] = sf_loaded_network['daily_traffic']*5
    print(sf_loaded_network.shape)
    #sf_loaded_network[['A', 'B', 'CAP', 'fiveday_traffic', 'geometry']].to_csv('directed_mtcone_5day_traffic.csv', index=False)

    ### Merge results from two directions
    sf_loaded_network['undir_AB'] = pd.DataFrame(np.sort(sf_loaded_network[['A', 'B']].values, axis=1), columns=['small_nodeid', 'large_nodeid']).apply(lambda x:'%s_%s' % (x['small_nodeid'],x['large_nodeid']),axis=1)
    sf_loaded_network_grp = sf_loaded_network.groupby('undir_AB').agg({
            'fiveday_traffic': np.sum, 
            'CAP': np.sum,
            'geometry': 'first'}).reset_index()
    tmp = sf_loaded_network.groupby('undir_AB').size()
    sf_loaded_network_grp = sf_loaded_network_grp.rename(columns={'fiveday_traffic': 'undirected_fiveday_traffic'})

    sf_loaded_network_grp[['undir_AB', 'CAP', 'undirected_fiveday_traffic', 'geometry']].to_csv('undirected_mtcone_5day_traffic.csv', index=False)

def plot_validation():
    mtc_df = pd.read_csv('directed_mtcone_5day_traffic_20190102.csv')
    mtc_gdf = gpd.GeoDataFrame(mtc_df, 
        crs={'init': 'epsg:4326'},
        geometry = mtc_df['geometry'].apply(shapely.wkt.loads))
    mtc_gdf['haversine_length'] = mtc_gdf.apply(lambda x: 
        sum([haversine.haversine(y0, x0, y1, x1) 
            for ((x0, y0), (x1, y1)) in zip(x['geometry'].coords, x['geometry'].coords[1:])]), 
        axis=1)
    mtc_gdf['haversine_length'] = mtc_gdf['haversine_length']/1000 # km to m
    print(sum(mtc_gdf['haversine_length']))
    mtc_gdf['length_traffic'] = mtc_gdf['haversine_length'] * mtc_gdf['fiveday_traffic']
    mtc_gdf = mtc_gdf.sort_values(by='fiveday_traffic', ascending=False)
    mtc_gdf['cumsum_length'] = mtc_gdf['haversine_length'].cumsum()
    mtc_gdf['cumsum_length_traffic'] = mtc_gdf['length_traffic'].cumsum()
    ### road length
    mtc_gdf['scaled_length'] = mtc_gdf['haversine_length']*300000
    mtc_gdf['length_cumsum_length'] = mtc_gdf.sort_values(by='scaled_length', ascending=False)['haversine_length'].cumsum()

    osm_df = pd.read_csv('../../3_visualization/directed_weekly_traffic_20190227.csv')
    osm_gdf = gpd.GeoDataFrame(osm_df, 
        crs={'init': 'epsg:4326'},
        geometry = osm_df['geometry'].apply(shapely.wkt.loads))
    osm_gdf['length'] = osm_gdf['length']/1000 # km to m
    print(sum(osm_gdf['length']))
    osm_gdf['length_traffic'] = osm_gdf['length'] * osm_gdf['fiveday_traffic']
    osm_gdf = osm_gdf.sort_values(by='fiveday_traffic', ascending=False)
    osm_gdf['cumsum_length'] = osm_gdf['length'].cumsum()
    osm_gdf['cumsum_length_traffic'] = osm_gdf['length_traffic'].cumsum()
    ### A random variable
    osm_gdf['rand'] = np.random.rand(osm_gdf.shape[0])*800000
    osm_gdf['rand_cumsum_length'] = osm_gdf.sort_values(by='rand', ascending=False)['length'].cumsum()
    ### road length
    osm_gdf['scaled_length'] = osm_gdf['length']*300000
    osm_gdf['length_cumsum_length'] = osm_gdf.sort_values(by='scaled_length', ascending=False)['length'].cumsum()

    ### Road length distribution
    # plt.hist(mtc_gdf['haversine_length'], bins=500, label='MTC')
    # plt.hist(osm_gdf['length'], bins=500, alpha=0.2, label='OSM')
    # plt.xlim(0, 0.8)
    # plt.xlabel('link length (km)')
    # plt.legend()
    # plt.show()
    # sys.exit(0)

    matplotlib.rcParams.update({'font.size': 20})
    osm_gdf['fiveday_traffic_thousand'] = osm_gdf['fiveday_traffic']/1000
    mtc_gdf['fiveday_traffic_thousand'] = mtc_gdf['fiveday_traffic']/1000
    plt.plot('cumsum_length', 'fiveday_traffic_thousand', 'g^', ms=10, data=osm_gdf, label='SF ABM')
    plt.plot('cumsum_length', 'fiveday_traffic_thousand', 'r+', ms=10, data=mtc_gdf, label='MTC Travel Model One')
    #plt.plot('length_cumsum_length', 'scaled_length', 'b.', ms=0.1, data=osm_gdf, label='SF ABM road length')
    #plt.plot('length_cumsum_length', 'scaled_length', 'y+', ms=0.1, data=mtc_gdf, label='MTC road length')
    plt.xlabel('Cumulative road length "mileage" (km)')
    plt.ylabel('Weekday traffic (in 1000)')
    #plt.xlim([0, 200])
    plt.legend()
    plt.xscale('log')
    plt.show()
    sys.exit(0)

    ###
    matplotlib.rcParams.update({'font.size': 20})
    plt.plot('cumsum_length', 'cumsum_length_traffic', 'r.', data=mtc_gdf, label='MTC Travel Model One')
    plt.plot('cumsum_length', 'cumsum_length_traffic', 'g.', data=osm_gdf, label='SF ABM')
    plt.xlabel('Cumulative road length "mileage" (km)')
    plt.ylabel('Cumulative vehicle kilometers in a typical Mon-Fri (veh*km)')
    plt.xscale('log')
    plt.legend()
    plt.show()

if __name__ == '__main__':
    #main()
    plot_validation()
    print('end')