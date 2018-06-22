import cProfile
import London_abm_mp_sssp_full

cProfile.run('London_abm_mp_sssp_full.main()', 'London_abm_mp_sssp_profile.txt')
#cProfile.run('mp_test.main()', 'mp_test_profile.txt')
