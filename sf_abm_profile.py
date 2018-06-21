import cProfile
import sf_abm_mp_sssp2

cProfile.run('sf_abm_mp_sssp2.main()', 'sf_abm_mp_sssp_profile.txt')
#cProfile.run('mp_test.main()', 'mp_test_profile.txt')
