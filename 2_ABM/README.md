# ABM_shortest_path
* Finding the shortest path for each OD pair;
* Calculation accelerated by Python multiprocessing on HPC.


### Required data
1. Required `OD_DY{}_HR{}_50000.csv`
  * OD data generated from the previous step or information organised in the following format. The csv file contains of three columns, named `O`, `D` and `flow`. Each row is the origin ID, destination ID and flow for each OD pair. The location of the file is under the folder `1_OD/`.
  ```
	O,D,flow
	OriginID_1,DestinationID_1,Flow_1
	OritionID_2,DestinationID_2,Flow_2
	...
  ```
2. Required `network_graph.graphmlz`
  * The network data stored as graphmlz format from `0_network`. The location of the file in under `0_network/data/`.

### Running the ABM
  * The actual multiprocessing shortest path finding part is in `abm_mp.py`. However, `abm_mp_profile.py` should be run or submitted to the HPC to get profile results.
  * The number of processes are set by `process_count`.
  * The day of week, start and end hour of the simulation are set by `day_of_week`, `start_hour`, `end_hour`.
  * Resulting graph for each simulation time step (one hour time step currently) can be sent to AWS S3 if the function `write_geojson` is enabled.
