import cProfile
import sf_abm_mp

cProfile.run('sf_abm_mp.main()', 'sf_abm_mp_profile.txt')
#cProfile.run('mp_test.main()', 'mp_test_profile.txt')
