import os
import sys
import pstats
import cProfile

import ecoanalysis ### with our sp implementation

absolute_path = os.path.dirname(os.path.abspath(__file__))

case = os.environ['CASE']

def profile():
    cProfile.run('ecoanalysis.scenarios()', absolute_path+'/eco_profile_c{}.txt'.format(case))

def analysis():
    stats = pstats.Stats(absolute_path+'/eco_profile_c{}.txt'.format(case))
    print(stats.strip_dirs().sort_stats('tottime').print_stats(15)) #

if __name__ == '__main__':
    profile()
    analysis()
