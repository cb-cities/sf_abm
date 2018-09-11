# OD_pairs
* Constructing hourly OD matrices for the bay area from the [California Household Travel Survey (CHTS)](https://www.nrel.gov/transportation/secure-transportation-data/tsdc-california-travel-survey.html).

This folder provides the scripts to generate an arbitrary number OD pairs from the CHTS place table.

### Required data
1. Required `surve_place.csv`
  * Download the full survey data from the [CHTS website](https://www.nrel.gov/transportation/secure-transportation-data/tsdc-california-travel-survey.html);
  * Copy the `survey_place.csv` table to this folder.
2. Required `census_tract.geojson`
  * Download the 2010 California census tract shapefile from the [US Census Bureau website](https://www.census.gov/geo/maps-data/data/cbf/cbf_tracts.html).
  * Convert the downloaded shapefile to geojson format, e.g., using [QGIS](https://www.qgis.org/en/site/).

### Generating OD pairs
Modify the last line and run the [CHTS_place2OD.py](CHTS_place2OD.py) to generate the desired number of OD pairs for specified days and hours, for example, `4, '09:00:00', 1000`.