# OD demand
* Constructing hourly OD matrices for San Francisco based on SFCTA's [TNC study](http://tncstoday.sfcta.org). The TNC data is believed to reflect Uber/Lyft pick-ups and drop-offs in the city by Traffic Analysis Zone (TAZ).
* Generate any number of OD pairs based on the TAZ-level travel demand.

This folder provides the scripts to generate OD pairs, or travel demand, for San Francisco specifically. You can easily supply your own OD and skip this folder altogether as long as it is compatible to the [required format](#required-format).

### Required format
1. `CityName_DYx_HRy_OD_SomeNumbers.csv`
In contrary to the commonly used OD matrix, we use OD table. This is because for our ABM simulation, there will be hundreds of thousands of origins and destinations. But unlike the conventional dense OD matrix, our OD is sparse and can be conveniently expressed in the following format:
```
id, O, D, flow
1, nodeID_1_o, nodeID_1_d, flow_1
2, nodeID_2_o, nodeID_2_d, flow_2
...
```
The file name is based on the name of the city, day of week as well as the hour of travel, as currently we are mainly working on hourly OD. So for 7 days per week, 24 hours per day, there should be 24x7 OD files. Or you can just supply a few to test. The OD files should be placed under [output/](output/).

### Generating OD pairs
If you don't have your own information on OD pairs, we provide the scripts to generate such files based on Uber/Lyft pick-ups and drop-offs in San Francisco.

1. Run [OD2csv.py](OD2csv.py) to generate the desired number of OD pairs for specified days and hours:
  * You can leave anything to default, which will generate 50k OD pairs for Tuesday 9am and Tuesday 10am. 50k would hardly be the hourldy demand for a weekday morning, but it is a decent size for testing.
  * Optionally, at the end of the file, 
  	* change `for day_of_week in [1]` to obtain results for different days of week. Monday is 0, Tuesday is 1, ..., Sunday is 6.
  	* change `for hour in range(9,11)` to generate OD pairs for different hours of the day.
  	* change `50000` in `TAZ_nodes_OD(day_of_week, hour, 50000)` to generate different numbers of OD pairs.
  * Check you have outputs, e.g., `SF_graph_DY1_HR9_OD_50000.csv`, in [output/](output/).