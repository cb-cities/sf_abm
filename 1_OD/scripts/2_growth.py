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

    # print(district_growth[district_growth['district'].isin(outside_SF_districts)][['pickups_year_growth', 'dropoffs_year_growth']])
    
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
        lambda x: x[1: -1].replace('Marina/N. Heights', 'marina_and_n_heights')
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


def TAZ_nodes_OD(district_growth, TAZ_district, hour=5, day=3, year=0):

    print('============ Year {} Day {} Hour {} ============'.format(year, day, hour))

    ### 0.a READING
    taz_travel_df = pd.read_csv(absolute_path+'/../input/TNC_pickups_dropoffs.csv') ### TNC ODs from each TAZ
    taz_scale_df = pd.read_csv(absolute_path+'/../input/TAZ_supervisorial.csv') ### Scaling factors for each TAZ. Figure 17-19 in the SFCTA TNCs Today report
    taz_pair_dist_df = pd.read_csv(absolute_path+'/../output/sf_overpass/original/TAZ_pair_distance.csv') ### Centroid coordinates of each TAZ

    ### 0.b Get district level annual traffic growth rate
    taz_growth_df = pd.merge(TAZ_district[['TAZ', 'district']], district_growth[['district', 'pickups_year_growth', 'dropoffs_year_growth']], on='district', how='left')

    ### 1. FILTERING to time of interest
    ### OD_df: pickups and dropoffs by TAZ from TNC study
    ### [{'taz':1, 'day':1, 'hour':10, 'pickups': 80, 'dropoffs': 100}, ...]
    ### Monday is 0 -- Sunday is 6. Hour is from 3am-26am(2am next day)
    hour_taz_travel_df = taz_travel_df[(taz_travel_df['day_of_week']==day) & (taz_travel_df['hour']==hour)].reset_index()

    ### 2. SCALING
    ### Scale the TNC ODs to the total TAZ ODs using scaling factors that vary spatially (by superdistricts) and temporally (by AM, PM and off-peak)
    if hour in [7, 8]: share_col = 'AMshare' ### 7-9am according to https://sfgov.org/scorecards/transportation/congestion
    elif hour in [17, 18]: share_col = 'PMshare' ### 4.30-6.30pm
    else: share_col = 'OPshare'
    if day in [5, 6]: share_col = 'OPshare' ### Weekend is always off-peak

    hour_taz_travel_df = pd.merge(hour_taz_travel_df[['taz', 'pickups', 'dropoffs']], taz_scale_df, how='left', left_on='taz', right_on='TAZ')
    hour_taz_travel_df['veh_pickups'] = hour_taz_travel_df.apply(lambda row: row['pickups']/row[share_col], axis=1)
    hour_taz_travel_df['veh_dropoffs'] = hour_taz_travel_df.apply(lambda row: row['dropoffs']/row[share_col], axis=1)
    hour_taz_travel_df = hour_taz_travel_df.dropna(subset=['veh_pickups', 'veh_dropoffs']) ### drop the rows that have NaN total_pickups and total_dropoffs

    ### 2.b SCALING by yearly growth rate
    hour_taz_travel_df = pd.merge(hour_taz_travel_df[['taz', 'veh_pickups', 'veh_dropoffs']], taz_growth_df, how='left', left_on='taz', right_on='TAZ')
    hour_taz_travel_df['veh_pickups_proj'] = hour_taz_travel_df['veh_pickups']*hour_taz_travel_df['pickups_year_growth']**year
    hour_taz_travel_df['veh_dropoffs_proj'] = hour_taz_travel_df['veh_dropoffs']*hour_taz_travel_df['dropoffs_year_growth']**year

    O_counts = np.sum(hour_taz_travel_df['veh_pickups_proj'])
    D_counts = np.sum(hour_taz_travel_df['veh_dropoffs_proj'])
    print(' base year total pickups {}, dropoffs {}. They may not be equal. \n projected year total pickups {}, dropoffs {}'.format(
        np.sum(hour_taz_travel_df['veh_pickups']),
        np.sum(hour_taz_travel_df['veh_dropoffs']),
        O_counts, D_counts))

    ### 3. TAZ-level OD pairs
    ### The probability of trips from each origin (to each destination) TAZ is proporional to the # trip origins (destinations) of that TAZ devided by the total origins (destinations) in all TAZs.
    hour_demand = int(0.5 * (O_counts + D_counts))
    print('DY{}, HR{}, total number of OD pairs: {}'.format(day, hour, hour_demand))
    O_prob = hour_taz_travel_df['veh_pickups_proj']/O_counts
    D_prob = hour_taz_travel_df['veh_dropoffs_proj']/D_counts
    
    OD_list = [] ### A list holding all the TAZ level OD pairs
    step = 0
    while (len(OD_list) < hour_demand):### While not having generated enough OD pairs
        step_demand = max(10, int(hour_demand*(0.3**step))) ### step 0: step_demand = total_demand; step 1: step_demand = 0.3*total_demand ...
        O_list = np.random.choice(hour_taz_travel_df['TAZ'], step_demand, replace=True, p=O_prob)
        D_list = np.random.choice(hour_taz_travel_df['TAZ'], step_demand, replace=True, p=D_prob)
        step_OD_df = pd.DataFrame({'O': O_list, 'D': D_list})
        ### I did not sort OD by small or big, so merge twice.
        step_OD_df = pd.merge(step_OD_df, taz_pair_dist_df, how='left', left_on=['O', 'D'], right_on=['TAZ_small', 'TAZ_big'])
        step_OD_df = pd.merge(step_OD_df, taz_pair_dist_df, how='left', left_on=['D', 'O'], right_on=['TAZ_small', 'TAZ_big'], suffixes=['_1', '_2'])
        step_OD_df = step_OD_df.fillna(value={'distance_1': 0, 'distance_2':0})
        step_OD_df = step_OD_df.loc[step_OD_df['distance_1'] + step_OD_df['distance_2']>2500].reset_index() ### half an hour if walking at 5km/h
        OD_list += list(zip(step_OD_df['O'], step_OD_df['D']))
        # print('step demand {}, length of OD at step {}: {}'.format(step_demand, step, len(OD_list)))
        step += 1
    #sys.exit(0)

    OD_list = OD_list[0:hour_demand]
    OD_counter = Counter(OD_list) ### get dictionary of list item count

    ### 4. Nodal-level OD pairs
    ### Now sample the nodes for each TAZ level OD pair
    taz_nodes_dict = json.load(open(absolute_path+'/../output/sf_overpass/original/taz_nodes.json'))
    #node_osmid2graphid_dict = json.load(open(absolute_path+'/../0_network/data/sf/node_osmid2graphid.json'))
    nodal_OD = []
    for k, v in OD_counter.items():
        taz_O = k[0]
        taz_D = k[1]
        
        ### use the nodes from the next TAZ if there is no nodes in the current TAZ
        ### Scenario 1: TAZ=741. It is in downtown, with many pickups and dropoffs. However, all the nodes are on the boundary (no node in TAZ=741). Then we use the nodes from nearby TAZs.
        while len(taz_nodes_dict[str(taz_O)])==0: taz_O += 1
        while len(taz_nodes_dict[str(taz_D)])==0: taz_D += 1

        nodal_OD_pairs = random.choices(list(itertools.product(taz_nodes_dict[str(taz_O)], taz_nodes_dict[str(taz_D)])), k=v)
            ### random.choices generate k elements from the population with replacement
        nodal_OD += nodal_OD_pairs

    nodal_OD_df = pd.DataFrame(nodal_OD, columns=['O', 'D'])
    print('final nodal OD {}'.format(nodal_OD_df.shape[0]))

    nodal_OD_df.to_csv(absolute_path+'/../output/sf_overpass/original/growth/intraSF/SF_OD_YR{}_DY{}_HR{}.csv'.format(year, day, hour), index=False)

def main():

    ### Traffig growth based on ConnectSF data: https://connectsf-trippatterns.sfcta.org
    ### Find with ConnectSF each TAZ belongs to
    TAZ_district = find_TAZ_district()
    ### Calculate the growth rate of each district
    district_growth = district_growth_rate()
    district_growth = district_growth.rename(columns={'odistrict_2': 'district'})
    
    for year in range(1, 11):
        ### year 0 should be copied
        #for hour in range(3, 27):
        for hour in range(7, 8):
            TAZ_nodes_OD(district_growth, TAZ_district, hour=hour, day=2, year=year)

def process_year_0(day = 2):
    ### Because when the OD for year 0 was generated, the random seed was not fixed. So this step copies the original year 0 tables to the desired location, removes the "flow" column and breaks down the multiple agent OD into individual ODs.

    for hour in range(3, 27):
        year_0_OD = pd.read_csv(absolute_path+'/../output/OD_tables_no_growth/intraSF/DY{}/SF_OD_DY{}_HR{}.csv'.format(day, day, hour))

        duplicated_year_0_OD_list = []
        duplicated_year_0_OD = year_0_OD.loc[year_0_OD['flow']>1]
        for index, row in duplicated_year_0_OD.iterrows():
            duplicated_year_0_OD_list += [(getattr(row, 'O'), getattr(row, 'D'))]*getattr(row, 'flow')
        duplicated_year_0_OD_df = pd.DataFrame(duplicated_year_0_OD_list, columns=['O', 'D'])
        
        processed_year_0_OD_df = year_0_OD.loc[year_0_OD['flow']==1][['O', 'D']].reset_index(drop=True)
        processed_year_0_OD_df = pd.concat([processed_year_0_OD_df, duplicated_year_0_OD_df])
        
        print('DY{}, HR {}, OD_df shape with flow column {}, duplicate agents {}, in {} rows, OD_df shape without flow column {}'.format(day, hour, year_0_OD.shape, duplicated_year_0_OD['flow'].sum(), duplicated_year_0_OD.shape[0], processed_year_0_OD_df.shape))

        if day ==2: ### Only for Wednesday do we consider the traffic increase. As Wednesday was used for the 10 year emission analysis
            processed_year_0_OD_df.to_csv(absolute_path+'/../output/OD_tables_growth/intraSF/SF_OD_YR0_DY{}_HR{}.csv'.format(day, hour), index=False)
        processed_year_0_OD_df.to_csv(absolute_path+'/../output/OD_tables_no_growth/intraSF/SF_OD_YR0_DY{}_HR{}.csv'.format(day, hour), index=False)

if __name__ == '__main__':
    #main()
    process_year_0(day = 6)
