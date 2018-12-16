### Data dictionary: https://github.com/BayAreaMetro/modeling-website/wiki/LoadedHighway
import sys 
import pandas as pd 
import geopandas as gpd 
import shapely.wkt 

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

    sf_loaded_network['VOL24HR_total'] = 0
    for vc in vehicle_class:
        sf_loaded_network['VOL24HR_total'] += sf_loaded_network['VOL24HR_{}'.format(vc)]

    sf_loaded_network['VOL_weekly'] = sf_loaded_network['VOL24HR_total']*7
    sf_loaded_network[['A', 'B', 'CAP', 'VOL_weekly', 'geometry']].to_csv('avgload5period_24hrtotal.csv', index=False)

if __name__ == '__main__':
    main()
    print('end')