#!/usr/bin/env python

__author__    = 'Radical.Utils Development Team (Andre Merzky, Ole Weidner)'
__copyright__ = 'Copyright 2013, RADICAL@Rutgers'
__license__   = 'MIT'


import resource

import radical.utils as ru


# ------------------------------------------------------------------------------
def test_plugin_manager():
    '''
    Test plugin manager
    '''

    pmgr = ru.PluginManager('radical.utils')
    import pprint
    pprint.pprint(pmgr._registry)

    plugin_2 = pmgr.load('unittests_1', 'default_2', 'a', 1)
    ret = plugin_2.run()
    assert(plugin_2.plugin_type == 'unittests_1')
    assert(plugin_2.plugin_name == 'default_2')
    assert(ret == ('a', 1)), 'plugin_2 invocation: %s != %s' % (['a', 1], ret)

    try:
        pmgr.load('unittests_1', 'default_1')
        assert False
    except LookupError:
        pass
    except:
        assert False


    plugin_2 = pmgr.load('unittests_1', 'default_2', 'a', 1)
    ret = plugin_2.run()
    assert(ret == ('a', 1)), 'plugin_2 invocation: %s != %s' % (['a', 1], ret)

    plugin_3 = pmgr.load('unittests_2', 'default_2', 'a', 1)
    ret = plugin_3.run()
    assert(ret == ('a', 1)), 'plugin_3 invocation: %s != %s' % (['a', 1], ret)

    # load twice -- plugin_2 is marked as singleton plugin_2, and will raise
    # if it is created twice
    plugin_3 = pmgr.load('unittests_2', 'default_2', 'a', 1)
    ret = plugin_3.run()
    assert(ret == ('a', 1)), 'plugin_3 invocation: %s != %s' % (['a', 1], ret)


    mem_0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    pmgr  = ru.PluginManager('radical.utils')

    for i in range(1000000):
        plugin_4 = pmgr.load('unittests_2', 'default_2', 'a', 1)
        ret = plugin_4.run()
        assert(ret == ('a', 1)), 'plugin_4 invoc: %s != %s' % (['a', 1], ret)

        if not i % 100000:
            mem_1 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            assert(mem_1 <= 2 * mem_0)

    mem_1 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert(mem_1 <= 2 * mem_0)


# ------------------------------------------------------------------------------
# run tests if called directly
if __name__ == '__main__':

    test_plugin_manager()


# ------------------------------------------------------------------------------

