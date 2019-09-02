import os
import pstats
import cProfile

import sf_residual_demand ### with our sp implementation

absolute_path = os.path.dirname(os.path.abspath(__file__))

def profile():
    #cProfile.run('sf_abm_mp_igraph.main()', 'sf_abm_mp_profile.txt')
    cProfile.run('sf_residual_demand.main()', absolute_path+'/sf_residual_demand_profile_p8ss15.txt')

def analysis():
    stats = pstats.Stats('sf_residual_demand_profile_p8ss15.txt')
    print(stats.strip_dirs().sort_stats('tottime').print_stats(15)) #

if __name__ == '__main__':
    #profile()
    analysis()
