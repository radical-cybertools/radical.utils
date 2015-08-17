#!/usr/bin/env python

__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import sys

import radical.utils.testing     as rut
import radical.utils             as ru


#-----------------------------------------------------------------------------
# entry point
if __name__ == "__main__":

    # set up the testing framework
    testing = rut.Testing ('radical.utils', __file__)
    ret     = True

    for config in sys.argv[1:] :

        if  not os.path.exists (config) :
            print "ERROR: Directory/file '%s' doesn't exist." % config
            sys.exit (-1)

        # for each config, set up the test config singleton and run the tests
        tc  = rut.TestConfig (config)

        # run tests
        if  not testing.run () :
            ret = False

    sys.exit (ret)


# ------------------------------------------------------------------------------


