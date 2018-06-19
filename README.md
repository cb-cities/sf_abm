# sf_abm

Shortest path searching for large numbers of Origin-Destination (OD) pairs using python-igraph and multiprocessing on Cambridge HPC.

### Running
`sbatch slurm_submit_bz247` through sbatch or `python3 sf_abm_mp.py` on interactive use.  
Specifically, the slurm script executes `run.sh`, which executes `sf_abm_profile.py`. The latter uses `cProfile` to profile the `main()` function of the multiprocessing version of the shortest path finding code, `sf_abm_mp.py`.

### Settings for multiprocessing
1. In the main python script, `sf_abm_mp.py`, change `process_count` to the desired number of processes, usually equal to the number of cpus/cores on the machine. For example, set this variable to 32 on the Cambridge HPC Skylake.
2. In the submission script, `slurm_submit_bz247`, set `--ntasks=1` and `--exclusive`, change `--cpus-per-tast` and `OMP_NUM_THREADS` to the same as `process_count`, e.g., 32.

### Settings for numbers of origins
1. In the main python script, `sf_abm_mp.py`, change `unique_origin` to a number no more than the row count of the input OD matrix (50,897 at the moment). One or a chunck of unique origins will be passed to the shortest path finding function, `map_edge_pop`. Each unique origin may correspond to several different destinations. Python-igraph's `get_shortest_pathes` function accept input for one origin and a list of destinations.
