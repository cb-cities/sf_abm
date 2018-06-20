import cProfile
import sf_abm_mp_sssp

cProfile.run('sf_abm_mp_sssp.main()', 'sf_abm_mp_sssp_profile.txt')
#cProfile.run('mp_test.main()', 'mp_test_profile.txt')
