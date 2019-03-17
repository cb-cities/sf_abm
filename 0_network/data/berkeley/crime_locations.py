import pandas as pd 
import certifi
import ssl
import geopy.geocoders 
from geopy.geocoders import Nominatim 
import sys 

ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx
geolocator = Nominatim(user_agent="bz247", scheme='http')

crime_map = pd.read_csv(open('crimemapping.csv'))
crime_map = crime_map.loc[crime_map['Location'].notnull()]
crime_map_lon = []
crime_map_lat = []

for row in crime_map.itertuples():
    location_str = getattr(row, 'Location')
    location_str = location_str.replace('BLOCK ', '')
    location_str = location_str + ', Berkeley'
    
    location = geolocator.geocode(location_str)
    try:
        crime_map_lon.append(location.longitude)
    except AttributeError:
        crime_map_lon.append(None)
    try:
        crime_map_lat.append(location.latitude)
    except AttributeError:
        crime_map_lat.append(None)

crime_map['lon'] = crime_map_lon
crime_map['lat'] = crime_map_lat
crime_map = crime_map.loc[crime_map['lon'].notnull()]
crime_map[['Description', 'Incident #', 'Location', 'Agency', 'Date', 'lon', 'lat']].to_csv('crime_locations.csv', index=False)