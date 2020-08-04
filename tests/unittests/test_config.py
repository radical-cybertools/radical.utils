#!/usr/bin/env python3

import os
import pytest
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_config():

    base = os.path.abspath(os.path.dirname(__file__))
    path = '%s/data/resource_*.json' % base

    with pytest.raises(ValueError):
        _ = ru.Config(path='foo', cfg='bar')

    cfg1 = ru.Config(name=path)

    assert('bar' == cfg1.yale.query('grace.agent_launch_method'))
    assert('bar' == cfg1['yale']['grace']['agent_launch_method'])

    assert(None  is cfg1.query('yale.grace.no_launch_method'))
    with pytest.raises(KeyError):
        _ = cfg1['yale']['grace']['no_launch_method']                # noqa F841

    os.environ['FOO'] = 'GSISSH'

    cfg2 = ru.Config(name=path)
    assert('GSISSH' == cfg2.query('yale.grace.agent_launch_method'))
    assert(None     is cfg2.query('yale.grace.no_launch_method'))

    cfg3 = ru.Config(name=path, expand=False)
    assert('${FOO:bar}' == cfg3.query('yale.grace.agent_launch_method'))

    env  = {'FOO' : 'baz'}
    cfg4 = ru.Config(name=path, env=env)
    assert('baz' == cfg4.query('yale.grace.agent_launch_method'))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_config()


# ------------------------------------------------------------------------------

