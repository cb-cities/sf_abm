# bay_area_abm
* Creating the street network graph for the San Francisco Bay Area from [OpenStreetMap (OSM)](openstreetmap.org);
* Generating arbitrary numbers of Origin-Destination (OD) pairs from the [California Household Travel Survey (CHTS) 2010](https://www.nrel.gov/transportation/secure-transportation-data/tsdc-california-travel-survey.html);
* Simulating the traffic in the Bay Area based on an Agent Based Modelling (ABM) approach that runs in parallel using python multiprocessing and HPC.

![Traffic flow with one million agents](figures/bay_area_abm_1m.png)

### Code structure
To run the ABM simulation, you will need to run through the code in each of the following folder:
  * [`0_network`](0_network): downloading the OSM street network for the Bay Area and constructing a graph (nodes and links) for the area;
  * [`1_OD`](1_OD): generating the hourly OD matrices based on data from CHTS. Origins and destinations are nodes in the graph;
  * [`2_ABM`](2_ABM): finding the shortest path for each OD pair using [`python-igraph`](http://igraph.org/python/) and [`python-multiprocessing`](https://docs.python.org/3.4/library/multiprocessing.html?highlight=process). The code can run on multi-core HPC.

### Performance
  * The most time consuming part of the ABM is finding the shortest path for each agent. In order to speed up the shortest path computation for thousands or even millions of agents, we use the multiprocessing functionality in python to compute the shortest paths in 32 threads.
  * On the Bay Area network (370,000 vertices, 700,000 edges), running on a 32-CPU node (6GB per CPU), it takes 47 minutes in total to run the ABM with 1 million agents.
  * On the same graph and computing hardware, it takes less than 2 minutes to run the ABM for 40,000 agents, roughly 17 times faster than running on single thread.
  ![Performance graph](figures/bay_area_abm_performance.png)