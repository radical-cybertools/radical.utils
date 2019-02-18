
__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_config():

    path = '%s/data/resource_*' % os.path.abspath(os.path.dirname(__file__))

    cfg1 = ru.Config(module='radical.utils', path=path)
    assert('bar' == cfg1.query('yale.grace.agent_launch_method'))
    assert(None  is cfg1.query('yale.grace.no_launch_method'))

    os.environ['FOO'] = 'GSISSH'

    cfg2 = ru.Config(module='radical.utils', path=path)
    assert('GSISSH' == cfg2.query('yale.grace.agent_launch_method'))
    assert(None     is cfg2.query('yale.grace.no_launch_method'))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_config()


# ------------------------------------------------------------------------------

