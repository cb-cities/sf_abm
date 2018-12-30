### Data dictionary: https://github.com/BayAreaMetro/modeling-website/wiki/LoadedHighway
import sys 
import pandas as pd 
import geopandas as gpd 
import shapely.wkt 
import numpy as np 

def main():
    ### Read data
    loaded_network = gpd.read_file('avgload5period/avgload5period.shp')
    loaded_network.crs = {'init': 'epsg:3717'}
    loaded_network = loaded_network.to_crs({'init': 'epsg:4326'})
    print(loaded_network.dtypes)
    osm_network = pd.read_csv('../../3_visualization/weekly_traffic.csv')
    osm_network = gpd.GeoDataFrame(osm_network,
                                    crs={'init': 'epsg:4326'},
                                    geometry = osm_network['geometry'].apply(shapely.wkt.loads))
    osm_xmin, osm_ymin, osm_xmax, osm_ymax = osm_network.total_bounds
    print(osm_network.total_bounds)
    print(loaded_network.total_bounds)
    sf_loaded_network = loaded_network.cx[osm_xmin:osm_xmax, osm_ymin:osm_ymax].reset_index()
    #sys.exit(0)

    ### Vehicle class https://github.com/BayAreaMetro/modeling-website/wiki/LoadedHighway
    vehicle_class = ['DA', 'DAT', 'S2', 'S2T', 'S3', 'S3T', 'SM', 'SMT', 'HV', 'HVT']
    ### Time period https://github.com/BayAreaMetro/modeling-website/wiki/TimePeriods
    time_period = ['EA', 'AM', 'MD', 'PM', 'EV']

    ### Check the sum of all time periods and VOL24HR
    # loaded_network['VOL_sum_DA'] = 0
    # for tp in time_period:
    #     loaded_network['VOL_sum_DA'] += loaded_network['VOL{}_DA'.format(tp)]
    # print(sum(loaded_network['VOL_sum_DA']-loaded_network['VOL24HR_DA']), sum(loaded_network['VOL_sum_DA'])) # 0.05794999974178225 224231190.02852812

    sf_loaded_network['daily_traffic'] = 0
    for vc in vehicle_class:
        sf_loaded_network['daily_traffic'] += sf_loaded_network['VOL24HR_{}'.format(vc)]
    sf_loaded_network['fiveday_traffic'] = sf_loaded_network['daily_traffic']*5

    ### Merge results from two directions
    sf_loaded_network['undir_AB'] = pd.DataFrame(np.sort(sf_loaded_network[['A', 'B']].values, axis=1), columns=['small_nodeid', 'large_nodeid']).apply(lambda x:'%s_%s' % (x['small_nodeid'],x['large_nodeid']),axis=1)
    sf_loaded_network_grp = sf_loaded_network.groupby('undir_AB').agg({
            'fiveday_traffic': np.sum, 
            'CAP': np.sum,
            'geometry': 'first'}).reset_index()
    sf_loaded_network_grp = sf_loaded_network_grp.rename(columns={'fiveday_traffic': 'undirected_fiveday_traffic'})

    sf_loaded_network_grp[['undir_AB', 'CAP', 'undirected_fiveday_traffic', 'geometry']].to_csv('mtcone_5day_traffic.csv', index=False)

if __name__ == '__main__':
    main()
    print('end')