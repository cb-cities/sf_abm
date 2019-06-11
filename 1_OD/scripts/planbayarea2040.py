import os 
import pandas as pd 
import geopandas as gpd 
import numpy as np 
import matplotlib.pyplot as plt 
from math import radians

absolute_path = os.path.dirname(os.path.abspath(__file__))

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    From: https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
    """

    ### Convert lat lon to radian before calculation

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000

def main():
    tazData = pd.read_csv(absolute_path + '/../input/planbayarea2040/tazData.csv')
    tazSF = np.unique(tazData[tazData['COUNTY']==1]['ZONE'])

    taz_gdf = gpd.read_file(absolute_path + '/../input/planbayarea2040/TAZ1454/Travel Analysis Zones.shp')
    SF_taz_gdf = taz_gdf[taz_gdf['TAZ1454'].isin(tazSF)].reset_index(drop=True)
    SF_taz_gdf = SF_taz_gdf.to_crs({'init': 'epsg:4326'})
    SF_taz_gdf['x'] = SF_taz_gdf['geometry'].centroid.x
    SF_taz_gdf['y'] = SF_taz_gdf['geometry'].centroid.y
    SF_taz_gdf['lon'] = SF_taz_gdf['x'].map(radians)
    SF_taz_gdf['lat'] = SF_taz_gdf['y'].map(radians)

    joint_trip_2015 = pd.read_csv(absolute_path + '/../input/planbayarea2040/jointTripData_2015.csv')
    SF_joint_trip_2015 = joint_trip_2015[joint_trip_2015['orig_taz'].isin(tazSF) & joint_trip_2015['dest_taz'].isin(tazSF)].reset_index(drop=True)

    indiv_trip_2015 = pd.read_csv(absolute_path + '/../input/planbayarea2040/indivTripData_2015.csv')
    SF_indiv_trip_2015 = indiv_trip_2015[indiv_trip_2015['orig_taz'].isin(tazSF) & indiv_trip_2015['dest_taz'].isin(tazSF)].reset_index(drop=True)

    SF_trip_2015 = pd.concat([
        SF_joint_trip_2015[['hh_id', 'orig_taz', 'dest_taz', 'trip_mode']], 
        SF_indiv_trip_2015[['hh_id', 'orig_taz', 'dest_taz', 'trip_mode']]], sort=False)

    SF_trip_2015 = pd.merge(SF_trip_2015, SF_taz_gdf[['TAZ1454', 'lon', 'lat']], left_on='orig_taz', right_on='TAZ1454', how='left')
    SF_trip_2015 = pd.merge(SF_trip_2015, SF_taz_gdf[['TAZ1454', 'lon', 'lat']], left_on='dest_taz', right_on='TAZ1454', how='left', suffixes=['_orig', '_dest'])
    SF_trip_2015['taz_dist'] = haversine(
        SF_trip_2015['lat_orig'], 
        SF_trip_2015['lon_orig'], 
        SF_trip_2015['lat_dest'], 
        SF_trip_2015['lon_dest'])

    SF_trip_2015['mode'] = np.where(
        SF_trip_2015['trip_mode'].isin([1,2,3,4,5,6, 19, 20, 21]), 'drive', np.where(
            SF_trip_2015['trip_mode'].isin([7]), 'walk', np.where(
                SF_trip_2015['trip_mode'].isin([8]), 'bike', np.where(
                    SF_trip_2015['trip_mode'].isin([9, 10, 11, 12, 13, 14, 15, 16, 17, 18]), 'transit', 'unknwon'))))

    print(SF_trip_2015.groupby('mode').size())
    fig, ax = plt.subplots()
    for name, grp in SF_trip_2015.groupby('mode'):
        print(name)
        ax.hist(grp['taz_dist'], bins=np.linspace(0, 15000, 50), histtype=u'step', label=name)
    plt.legend()
    plt.show()

if __name__ == '__main__':
    main()