# network
* Instructions for downloading the steet network from OpenStreetMap (OSM);
* Cleaning the OSM data by removing "curve" nodes that do not define edge intersections;
* Converting the original OSM data and the simplified OSM data to GeoJSON format for easy visualisation;
* Preparing the data as a graph object (for python-igraph) or sparse matrix (for [sp](https://github.com/cb-cities/sp)).

This folder provides the scripts to prepare the graph network for the Agent Based Modelling (ABM) simulation. Of course you can use your own road network compatible with the [required format](#required-format), but this is not necessary as we have prepared this guidance for you to download the required data directly from OSM!

### Required format
1. Required `nodes.json`
Nodes are road crossings where agents can make a route choice decision. Points defining the shape (e.g., curves or bents) of a road are not considered as nodes here. The `nodeID_n` refers to the unique identifier for the n-th node. `lat_n` and `lon_n` are the latitude and longitude coordinates of the n-th node.
```
{
  "nodeID_1": [
    lat_1,
    lon_1
  ],
  "nodeID_2": [
    lat_2,
    lon_2
  ],
  "nodeID_3": [
    lat_3,
    lon_3
  ],
  ...
}
```
2. Required `ways.json`
A road way may contain multiple nodes. The nodes here should be corresponding to the nodes in `nodes.json` file, with the same indexing, `nodeID_n`. For two-way roads, it should be represented as two separate elements.
```
[
  {
    "nodes": [
      nodeID_a,
      nodeID_b,
      nodeID_c
      ...
    ],
    "length": [
      integer_section_length_1,
      integer_section_length_2,
      ...
    ],
    "osmid": integer_unique_wayID,
    "lane": integer_lane_count_this_direction,
    "maxmph": integer_speed_limit_mph,
    "capacity": integer_capacity_vehicle_per_hour
  },
  ...
]
```
3. `nodes.json` and `ways.json` should be placed under the folder called [data/sf/](data/sf/). You can rename `sf` to any city you are studying, but just remember to change the file path in other scripts.

### Downloading and preparing the OSM street network data
If you don't have existing data and plan to download it from the OSM, you can run the following scripts.

1. Downloading the OSM street data: 
  * In terminal, run the following commands to download the road network data for San Francisco from the OSM:     
    * `echo "data=[out:json][bbox:37.6040,-122.5287,37.8171,-122.3549];way[highway];(._;>;);out;" > query.osm`   
    * `curl -o target.osm -X POST -d @query.osm "http://overpass-api.de/api/interpreter"`
    * Change the values in `bbox` if you want to download data for another city.
    * The above commands are based on http://overpass-api.de/command_line.html  
  * Store the downloaded file `target.osm` under [data/sf/](data/sf/).
2. Filter and split the OSM data by running [scripts/1_osm2json.py](scripts/1_osm2json.py):
  * Only keeping the drivable roads and road crossing nodes in the OSM data. Getting rid of the small paths and points defining the geometry of the road. Spliting the dataset into a `nodes.json` file and a `ways.json` file.
  * Optionally, you can convert the `target.osm` to GeoJSON format for visualisation (e.g., in QGIS) by un-commenting `osm_to_geojson()`.
  * Optionally, you can also choose to output in GeoJSON format by setting `output_geojson=True`.

### Preparing the graph object of the road network
With `nodes.json` and `ways.json`, we can create the graph object for agents to navigate on.

1. Check that `nodes.json` and `ways.json` are in the right place, i.e., under [data/sf/](data/sf/).
2. Run [scripts/2_json2graph.py](scripts/2_json2graph.py), which will combine the `nodes.json` and `ways.json` into a pickeld python-igraph object `network_graph.pkl`.
  * The summary of the graph size, vertice and edge attributes will be printed on screen.
  * This script will also output `node_osmid2graphid.json`, which is useful in the later stage to map node ID to its ID on the graph.
3. If you want to use the shortest path algorithm [sp](https://github.com/cb-cities/sp) that we developed (it is much faster!), then run [scripts/2_json2graph.py](scripts/2_json2graph.py) to convert `network_graph.pkl` to a sparse matrix `network_sparse.mtx`. This part is currently under development.