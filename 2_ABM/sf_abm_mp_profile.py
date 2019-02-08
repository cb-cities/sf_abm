import cProfile
import sf_abm_mp_speed_info ### with our sp implementation

cProfile.run('sf_abm_mp_speed_info.main()', 'sf_abm_mp_profile.txt')
