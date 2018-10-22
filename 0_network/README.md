# network
* Processing the downloaded network from [OSMnx](https://geoffboeing.com/2016/11/osmnx-python-street-networks/);
* Converting the data into a sparse matrix (for [sp](https://github.com/cb-cities/sp)).

### Input
Network in `nodes.csv` and `edges.csv`.

### Output
* `network_sparse.mtx`
* `network_attributes.csv`

### Running
`python3 scripts/osmnx_to_mtx.py`