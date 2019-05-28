#!/usr/bin/env python

__author__    = "Radical.Utils Development Team (Andre Merzky, Ole Weidner)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import os
import pytest
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_config():

    path = '%s/data/resource_*.json' % os.path.abspath(os.path.dirname(__file__))

    cfg1 = ru.Config(module='radical.utils', path=path)

    assert('bar' == cfg1.query('yale.grace.agent_launch_method'))
    assert('bar' == cfg1['yale']['grace']['agent_launch_method'])

    assert(None  is cfg1.query('yale.grace.no_launch_method'))
    with pytest.raises(KeyError):
        _ = cfg1['yale']['grace']['no_launch_method']

    os.environ['FOO'] = 'GSISSH'

    cfg2 = ru.Config(module='radical.utils', path=path)
    assert('GSISSH' == cfg2.query('yale.grace.agent_launch_method'))
    assert(None     is cfg2.query('yale.grace.no_launch_method'))

    cfg3 = ru.Config(module='radical.utils', path=path, expand=False)
    assert('${FOO:bar}' == cfg3.query('yale.grace.agent_launch_method'))

    env  = {'FOO' : 'baz'}
    cfg4 = ru.Config(module='radical.utils', path=path, env=env)
    assert('baz' == cfg4.query('yale.grace.agent_launch_method'))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_config()


# ------------------------------------------------------------------------------

