#!/bin/sh
module load python/3.5.1
module load gcc-7.2.0-gcc-4.8.5-pqn7o2k

python3 -m mpi4py mpi_test.py
