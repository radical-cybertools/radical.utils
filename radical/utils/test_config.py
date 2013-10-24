

import radical.utils as ru


# ------------------------------------------------------------------------------
#
class TestConfig (ru.TestConfig): 

    #---------------------------------------------------------------------------
    # 
    def __init__ (self, cfg_file):

        # initialize configuration.  We only use the 'radical.utils.tests' 
        # category from the config file.
        ru.TestConfig.__init__ (self, cfg_file, 'radical.utils.tests')


# ------------------------------------------------------------------------------


