import cProfile
import sf_abm_mp3
import mp_test

cProfile.run('sf_abm_mp3.main()', 'sf_abm_profile_0615.txt')
#cProfile.run('mp_test.main()', 'mp_test_profile.txt')
