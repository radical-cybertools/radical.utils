
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import pytest

import radical.utils.config as ruc


# ------------------------------------------------------------------------------
def test_config (tmpdir) :
    """ Test if configuration location is picked up from env """

    tmp = '%s/radical.util.cfg' % tmpdir
    os.environ ['RADICAL_UTILS_CONFIG'] = tmp

    with  open (tmp, 'w') as tmp_file :
        tmp_file.write ("[test]\n")
        tmp_file.write ("key=val\n")


    # set the configuration options for this object
    configurable = ruc.Configurable        ('radical.utils')
    config       = configurable.get_config ('test')

    assert ('val' == config['key'].get_value ())

    os.remove (tmp)


# ------------------------------------------------------------------------------

