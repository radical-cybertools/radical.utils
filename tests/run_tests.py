
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import os
import sys

import radical.utils.test_config as rutc
import radical.utils             as ru


#-----------------------------------------------------------------------------
# entry point
if __name__ == "__main__":

    # set up the testing framework
    testing = ru.Testing ('radical.utils', __file__)

    for config in sys.argv[1:] :

        if  not os.path.exists (config) :
            print "ERROR: Directory/file '%s' doesn't exist." % config
            sys.exit (-1)

        # for each config, set up the test config singleton and run the tests
        tc = rutc.TestConfig (config)

        print tc

        testing.run ()


# ------------------------------------------------------------------------------


