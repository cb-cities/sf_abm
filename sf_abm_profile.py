import cProfile
import sf_abm_mp4
import mp_test

cProfile.run('sf_abm_mp4.main()', 'sf_abm_profile_0618.txt')
#cProfile.run('mp_test.main()', 'mp_test_profile.txt')
