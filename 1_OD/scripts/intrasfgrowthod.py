### Check growth rates: 
### https://www.sfmta.com/sites/default/files/reports-and-documents/2018/01/san_francisco_transportation_trends_2.3.15.pdf
### https://www.sfmta.com/sites/default/files/reports-and-documents/2019/01/sfmta_mobility_trends_report_2018.pdf
import os
import sys
import time
import numpy as np 
import pandas as pd 
import geopandas as gpd 
from collections import Counter
import itertools 
import json
import random
import intrasfod

pd.set_option('display.max_columns', 10)

absolute_path = os.path.dirname(os.path.abspath(__file__))
np.random.seed(0)
random.seed(0)

def district_growth_rate():
    ### ConnectSF trip pattern 2015 & 2050

    ### Read data
    district_data = pd.read_csv(absolute_path + '/../input/planbayarea2050/trippattern/data_trippattern.csv')
    ### Keep only driving mode and total counts
    district_data = district_data[
        district_data['group_mode'].isin(['drive', 'uber/lyft']) & 
        #district_data['group_mode'].isin(['walk/bike']) & 
        district_data['group_purpose'].isin(['total'])]

    ### Change origin name in accordance with destination name (column headers)
    district_data['odistrict_2'] = district_data['odistrict'].apply(
        lambda x: x.replace('Marina/N. Heights', 'marina_and_n_heights')
        .replace('N. Beach/Chinatown', 'n_beach_and_chinatown')
        .replace('/', '_and_').replace(' ', '_').lower())
    district_names = np.unique(np.sort(district_data['odistrict_2']))

    growth_rate_dict = {}
    within_SF_districts = ['bayshore', 'downtown', 'hill_districts', 'marina_and_n_heights', 'mission_and_potrero', 'n_beach_and_chinatown', 'noe_and_glen_and_bernal', 'outer_mission', 'richmond', 'soma', 'sunset', 'western_market']
    for district in within_SF_districts:
        pickups_2015 = district_data.loc[(district_data['year']==2015) & (district_data['odistrict_2']==district)][within_SF_districts].sum().sum()
        pickups_2050 = district_data.loc[(district_data['year']==2050) & (district_data['odistrict_2']==district)][within_SF_districts].sum().sum()
        dropoffs_2015 = district_data.loc[(district_data['year']==2015) & (district_data['odistrict_2'].isin(within_SF_districts)), district].sum()
        dropoffs_2050 = district_data.loc[(district_data['year']==2050) & (district_data['odistrict_2'].isin(within_SF_districts)), district].sum()
        growth_rate_dict[district] = [pickups_2015, pickups_2050, dropoffs_2015, dropoffs_2050]

    outside_SF_districts = ['east_bay', 'north_bay', 'south_bay']
    for district in outside_SF_districts:
        destinations = [d for d in outside_SF_districts if d != district] + within_SF_districts
        enterSF_2015 = district_data.loc[(district_data['year']==2015) & (district_data['odistrict_2']==district)][destinations].sum().sum()
        enterSF_2050 = district_data.loc[(district_data['year']==2050) & (district_data['odistrict_2']==district)][destinations].sum().sum()
        leaveSF_2015 = district_data.loc[(district_data['year']==2015) & (district_data['odistrict_2'].isin(destinations)), district].sum()
        leaveSF_2050 = district_data.loc[(district_data['year']==2050) & (district_data['odistrict_2'].isin(destinations)), district].sum()
        growth_rate_dict[district] = [enterSF_2015, enterSF_2050, leaveSF_2015, leaveSF_2050]

    district_growth = pd.DataFrame.from_dict(growth_rate_dict, orient='index', columns=['pickups_2015', 'pickups_2050', 'dropoffs_2015', 'dropoffs_2050']).reset_index().rename(columns={'index': 'district'})   
    district_growth['pickups_year_growth'] = np.power(district_growth['pickups_2050']/district_growth['pickups_2015'], 1/(50-15))
    district_growth['dropoffs_year_growth'] = np.power(district_growth['dropoffs_2050']/district_growth['dropoffs_2015'], 1/(50-15))

    print(district_growth[['district', 'pickups_year_growth', 'dropoffs_year_growth']])
    
    # district_growth.to_csv('connectsf_district_growth_2.csv', index=False)
    # sys.exit(0)

    return district_growth

def find_TAZ_district():
    ### Each TAZ is contained in which district.

    ### Input 1: TAZ polyline
    taz_gdf = gpd.read_file(absolute_path+'/../input/TAZ981/TAZ981.shp')
    taz_gdf = taz_gdf.to_crs({'init': 'epsg:4326'})
    taz_gdf['centroid'] = taz_gdf['geometry'].centroid
    taz_spatial_index = taz_gdf[['TAZ', 'centroid']].set_geometry('centroid').sindex
    taz_gdf['district'] = 0

    ### Input 2: District polyline
    time_0 = time.time()
    district_gdf = gpd.read_file(absolute_path+'/../input/planbayarea2050/trippattern/DIST15_wgs84.shp')
    district_gdf = district_gdf.to_crs({'init': 'epsg:4326'})

    district_gdf['district'] = district_gdf['DIST15NAME'].apply(
        lambda x: x.replace('Marina/N. Heights', 'marina_and_n_heights')
        .replace('N. Beach/Chinatown', 'n_beach_and_chinatown')
        .replace('/', '_and_').replace(' ', '_').lower())
    district_names = np.unique(np.sort(district_gdf['district']))

    ### Method 1: spatial index
    ### Method 2: deleted. Matplotlib.path cannot deal with multipolygon
    for district in district_gdf.itertuples():
        district_polygon = getattr(district, 'geometry')
        possible_matches_index = list(taz_spatial_index.intersection(district_polygon.bounds))
        possible_matches = taz_gdf.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.intersects(district_polygon)]
        taz_gdf['district'] = np.where(
            taz_gdf['TAZ'].isin(precise_matches['TAZ']), getattr(district, 'district'),
            taz_gdf['district'])

    return taz_gdf


def TAZ_nodes_OD(district_growth, TAZ_district, year=0, day=2, hour=3):

    ### 0.b Get district level annual traffic growth rate
    taz_growth_df = pd.merge(TAZ_district[['TAZ', 'district']], district_growth[['district', 'pickups_year_growth', 'dropoffs_year_growth']], on='district', how='left')

    hour_demand = intrasfod.TAZ_nodes_OD(year=year, day=day, hour=hour, taz_growth_df = taz_growth_df)
    if hour == 18: 
        print('============ Year {} Day {} Hour {} ============'.format(year, day, hour))
        print(hour_demand)


def main():

    ### Traffig growth based on ConnectSF data: https://connectsf-trippatterns.sfcta.org
    ### Find with ConnectSF each TAZ belongs to
    TAZ_district = find_TAZ_district()

    ### Calculate the growth rate of each district
    district_growth = district_growth_rate()
    district_growth = district_growth.rename(columns={'odistrict_2': 'district'})
    
    for year in range(1, 11):
        ### year 0 should be copied
        for hour in range(3, 27):
        #for hour in range(7, 8):
            TAZ_nodes_OD(district_growth, TAZ_district, year=year, day=2, hour=hour)

if __name__ == '__main__':
    main()
