import os
import sys
import time
import numpy as np 
import pandas as pd 
import geopandas as gpd 
import matplotlib.path as mpltPath

absolute_path = os.path.dirname(os.path.abspath(__file__))

def district_growth_rate():
    ### ConnectSF trip pattern 2015 & 2050

    ### Read data
    district_data = pd.read_csv(absolute_path + '/../input/planbayarea2050/trippattern/data_trippattern.csv')
    ### Keep only driving mode and total counts
    district_data = district_data[
        district_data['group_mode'].isin(['drive', 'uber/lyft']) & 
        district_data['group_purpose'].isin(['total'])]
    print(np.unique(district_data['group_mode']))
    print(np.unique(district_data['group_purpose'])) ### madatory, discretionary

    ### Change origin name in accordance with destination name (column headers)
    district_data['odistrict_2'] = district_data['odistrict'].apply(
        lambda x: x.replace('Marina/N. Heights', 'marina_and_n_heights')
        .replace('N. Beach/Chinatown', 'n_beach_and_chinatown')
        .replace('/', '_and_').replace(' ', '_').lower())
    district_names = np.unique(np.sort(district_data['odistrict_2']))
    print(district_names)

    ### Origin counts
    district_data['pickups'] = district_data[district_names].sum(axis=1)
    pickups_2015 = district_data.loc[district_data['year']==2015].groupby('odistrict_2').agg({'pickups': np.sum}).reset_index().rename(columns={'pickups': 'pickups_2015'})
    pickups_2050 = district_data.loc[district_data['year']==2050].groupby('odistrict_2').agg({'pickups': np.sum}).reset_index().rename(columns={'pickups': 'pickups_2050'})

    ### Destination counts
    dropoffs_2015 = district_data.loc[district_data['year']==2015, district_names].sum(axis=0).reset_index(name='dropoffs_2015').rename(columns={'index': 'odistrict_2'})
    dropoffs_2050 = district_data.loc[district_data['year']==2050, district_names].sum(axis=0).reset_index(name='dropoffs_2050').rename(columns={'index': 'odistrict_2'})
    print(pickups_2015.shape, pickups_2050.shape, dropoffs_2015.shape, dropoffs_2050.shape)
    
    ### District, pickups_2015, pickups_2050, dropoffs_2015, dropoffs_2050
    district_growth = pd.merge(pickups_2015, pickups_2050, on='odistrict_2')
    district_growth = pd.merge(district_growth, dropoffs_2015, on='odistrict_2')
    district_growth = pd.merge(district_growth, dropoffs_2050, on='odistrict_2')
    
    district_growth['pickups_year_growth'] = np.power(district_growth['pickups_2050']/district_growth['pickups_2015'], 1/(50-15))
    district_growth['dropoffs_year_growth'] = np.power(district_growth['dropoffs_2050']/district_growth['dropoffs_2015'], 1/(50-15))
    #print(district_growth.sort_values(by='pickups_year_growth', ascending=False).tail())

    return district_growth

def find_in_nodes(row, points, nodes_df):
    ### return the indices of points in nodes_df that are contained in row['geometry']
    ### this function is called by TAZ_nodes()
    district_polygon = getattr(row, 'geometry')
    if district_polygon.type == 'MultiPolygon':
        return []
    else:
        path = mpltPath.Path(list(zip(*district_polygon.exterior.coords.xy)))
        in_index = path.contains_points(points)
        return nodes_df['TAZ'].loc[in_index].tolist()

def find_TAZ_district():
    ### Each TAZ is contained in which district.

    ### Input 1: TAZ polyline
    taz_gdf = gpd.read_file(absolute_path+'/../input/TAZ981/TAZ981.shp')
    taz_gdf = taz_gdf.to_crs({'init': 'epsg:4326'})
    taz_gdf['centroid'] = taz_gdf['geometry'].centroid
    taz_gdf['centroid_lon'] = taz_gdf['geometry'].centroid.x
    taz_gdf['centroid_lat'] = taz_gdf['geometry'].centroid.y
    taz_centroids = taz_gdf[['centroid_lon', 'centroid_lat']].values
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
    print(district_names)

    ### Method 1: spatial index
    # for district in district_gdf.itertuples():
    #     district_polygon = getattr(district, 'geometry')
    #     possible_matches_index = list(taz_spatial_index.intersection(district_polygon.bounds))
    #     possible_matches = taz_gdf.iloc[possible_matches_index]
    #     precise_matches = possible_matches[possible_matches.intersects(district_polygon)]
    #     print(precise_matches.shape)
    #     taz_gdf['district'] = np.where(
    #         taz_gdf['TAZ'].isin(precise_matches['TAZ']), getattr(district, 'DIST15NAME'),
    #         taz_gdf['district'])

    ### Method 2: matplotlib path
    for district in district_gdf.itertuples():
        precise_matches = find_in_nodes(district, taz_centroids, taz_gdf)
        taz_gdf['district'] = np.where(
            taz_gdf['TAZ'].isin(precise_matches), getattr(district, 'district'),
            taz_gdf['district'])
    time_1 = time.time()
    # print(time_1 - time_0)
    # print(taz_gdf.head())

    return taz_gdf


def TAZ_nodes_OD(day, hour, count=50000):

    ### 0.a READING
    taz_travel_df = pd.read_csv(absolute_path+'/../input/TNC_pickups_dropoffs.csv') ### TNC ODs from each TAZ
    taz_scale_df = pd.read_csv(absolute_path+'/../input/TAZ_supervisorial.csv') ### Scaling factors for each TAZ. Figure 17-19 in the SFCTA TNCs Today report
    taz_pair_dist_df = pd.read_csv(absolute_path+'/../output/sf_overpass/original/TAZ_pair_distance.csv') ### Centroid coordinates of each TAZ

    ### 0.b Get district level annual traffic growth rate
    district_growth = district_growth_rate()
    district_growth = district_growth.rename(columns={'odistrict_2': 'district'})
    TAZ_district = find_TAZ_district()
    taz_growth = pd.merge(TAZ_district[['TAZ', 'district']], district_growth[['district', 'pickups_year_growth', 'dropoffs_year_growth']], on='district', how='left')
    print(taz_growth.shape)
    print(taz_growth.head())
    sys.exit(0)

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

    O_counts = np.sum(hour_taz_travel_df['veh_pickups'])
    D_counts = np.sum(hour_taz_travel_df['veh_dropoffs'])
    logger.debug('sum total_pickups {}; sum total_dropoffs {}. They may not be equal'.format(O_counts, D_counts))

def main():
    TAZ_nodes_OD(5, 3)

if __name__ == '__main__':
    main()