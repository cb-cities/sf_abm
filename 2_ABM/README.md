# ABM_shortest_path
* Finding the shortest path for each OD pair with [python-igraph](http://igraph.org/python/) or [sp](https://github.com/cb-cities/sp);
* Parallel for each agents with Python multiprocessing on HPC.

This folder contains the actual ABM model part. We have been using `python-igraph` for shortest path route finding for some time, but now are changing to our own shortest path implementation [sp](https://github.com/cb-cities/sp) (priority queue Dijkstra). In our test, we found speed-wise `sp` >> `python-igraph` >> `networkx`. In this folder, we provide the scripts based on both `python-igraph` and `sp`, both with the possibility of running on multiple processes or on HPC. However, at this moment, the code for `sp`-based ABM has not been completely finished yet.

### Required data
1. Network file:
  * If you are using `python-igraph`, you need to have `network_graph.pkl` in [sf_abm/0_network/data/sf/](../0_network/data/sf/)
  * If you are using `sp`, you need to have `network_sparse.mtx` in [sf_abm/0_network/data/sf/](../0_network/data/sf/).
2. OD tables:
  * You need to have at least one OD table, e.g., `SF_graph_DY1_HR9_OD_50000.csv`, in in [sf_abm/1_OD/output/](../1_OD/output/).

### Running the ABM

  * Run locally:
    * There are two versions of the abm, `sf_abm_mp_igraph.py` if you have decided to use `python-igraph`, or `sf_abm_mp_qdijkstra.py` if you decide to use our shortest path library `sp`. At this stage, maybe `python-igraph` will run smoother. Open the file of your choice:
      * Set `process_count` to 1 if you want to run single process or a higher number for multiprocessing. Usually PCs have about 4-8 cores.
      * Set `unique_origin` to a number of shortest paths you want to run. Set it to 200 if you are merely testing, or `OD.shape[0]` (the total number of OD pairs in your OD table) to get the full results.
      * Change `for day in [1]` and `for hour in range(9,10)` to the corresponding days of week and hours of analysis. You need to have OD tables for all these time slices. 
      * Optionally, uncomment `write_geojson()` if you want to output the loaded network or save results to AWS S3.

  * Run on HPC:
    * Login to the HPC system and clone the [sf_abm Github repo](https://github.com/cb-cities/sf_abm). Go through the network generation and OD generation steps. Modify the ABM script as described above.
    * If you are running on the HPC, it will be good to profile the performance of the code. To do so, open `sf_abm_mp_profile.py` and make sure to import the correct ABM script (`import sf_abm_mp_igraph` or `import sf_abm_mp_qdijkstra`)
    * Check if there is a `run.sh` included in the repo. In the linux HPC terminal, do `chmod +x run.sh` to make the python scripts part of an executable.
    * Modify the example submit script. This is highly dependent on your HPC system, but the general idea is to request enough nodes, cores (same as the `process_count` in the ABM script), time, etc., as well as to provide the correct path to the executable `run.sh`. Then you can submit the submission script to the computational nodes.
