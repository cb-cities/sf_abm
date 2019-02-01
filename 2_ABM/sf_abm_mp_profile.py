import cProfile
import sf_abm_mp_closure ### with our sp implementation

cProfile.run('sf_abm_mp_closure.main()', 'sf_abm_mp_profile.txt')
