import cProfile
import sf_abm_mp_igraph ### with python-igraph
# import sf_abm_mp_qdijkstra ### with our sp implementation

cProfile.run('sf_abm_mp_igraph.main()', 'sf_abm_mp_profile.txt')
# cProfile.run('sf_abm_mp_qdijkstra.main()', 'sf_abm_mp_profile.txt')
